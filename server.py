import os
import re
import json
import time
import asyncio
import random
import logging
import uuid
import secrets
from typing import List, Optional, Tuple
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import asyncpg


from .httpx_client import HttpxClient
from .tls_client_impl import TlsClientImpl
from .idealista import Idealista
from .fotocasa import Fotocasa
from .habitaclia import Habitaclia
from .utils import parse_jwt_token
from .province_data import PROVINCES_BY_TIER, HABITACLIA_TREE, HABITACLIA_NO_TREE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Idealista Scraper API")

# --- Modo de servidor: "full" (todo), "api" (solo endpoints), "workers" (solo workers) ---
SERVER_MODE = os.environ.get("SERVER_MODE", "full")

# ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬ÂÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬ÂÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Cache del JWT token en memoria (Idealista) ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬ÂÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬ÂÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â
jwt_token_cache = {
    "token": None,
    "expires_at": 0
}

# --- Configuracion de Check Active (proceso continuo) ---
CHECK_ACTIVE_CONFIG = {
    "workers_per_tier": [5, 6, 1, 4],  # capitals=5, main=6, second_capitals=1, rest=4 (16 total)
    "batch_size": 50,
    "delay_min": 0.3,
    "delay_max": 0.5,
    "session_rotate_every": 30,
    "sleep_when_idle": 300,     # 5 min cuando el tier asignado al dia
    "restart_delay": 30,        # Espera antes de reiniciar worker crasheado
    "error_sleep": 60,          # Espera si batch tiene >50% errores
    "summary_interval": 21600,  # Resumen Slack cada 6h (en segundos)
}

CHECK_ACTIVE_TIERS = [
    {
        "name": "capitals",
        "cycle_hours": 2,
        "where_clause": (
            "province IN ('Madrid','Barcelona','València') "
            "AND municipality IN ('Madrid','Barcelona','València')"
        ),
    },
    {
        "name": "main_provinces",
        "cycle_hours": 14,
        "where_clause": (
            "province IN ('Madrid','Barcelona','València','Sevilla','Vizcaya',"
            "'Alicante','Málaga','Zaragoza','Balears (Illes)','Las Palmas') "
            "AND municipality NOT IN ('Madrid','Barcelona','València',"
            "'Sevilla','Bilbao','Málaga','Zaragoza')"
        ),
    },
    {
        "name": "second_capitals",
        "cycle_hours": 6,
        "where_clause": (
            "province IN ('Sevilla','Vizcaya','Málaga','Zaragoza') "
            "AND municipality IN ('Sevilla','Bilbao','Málaga','Zaragoza')"
        ),
    },
    {
        "name": "rest",
        "cycle_hours": 48,
        "where_clause": (
            "province NOT IN ('Madrid','Barcelona','València','Sevilla','Vizcaya',"
            "'Alicante','Málaga','Zaragoza','Balears (Illes)','Las Palmas')"
        ),
    },
]

check_active_state = {
    "running": False,
    "started_at": None,
    "workers": {},
    "totals": {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0, "by_tier": {}},
    "last_summary_at": 0,  # timestamp del ultimo resumen Slack
}
_id_ca_inflight = set()  # Propertycodes en vuelo — evita trabajo duplicado entre workers

# --- Configuracion de Enrich (proceso continuo, 1 worker) ---
ENRICH_CONFIG = {
    "batch_size": 500,
    "delay_min": 0.0,
    "delay_max": 0.0,
    "session_rotate_every": 30,
    "sleep_when_idle": 300,
    "restart_delay": 30,
    "error_sleep": 60,
}

enrich_state = {
    "running": False,
    "started_at": None,
    "totals": {"enriched": 0, "deactivated": 0, "errors": 0, "batches": 0},
    "status": "stopped",
}

# Propiedades que han fallado N veces en esta sesion — se resetea con cada deploy
_enrich_skip = {}  # {property_code: fail_count}
ENRICH_MAX_RETRIES = 3  # Tras 3 fallos, skip hasta reinicio

# --- Configuración de Enrich Fotocasa (worker de background) ---
FC_PROXY_ROTATE_EVERY = 30  # Moderado (20 era OK, 40 causó errores)
FC_PROACTIVE_REFRESH_REQUESTS = 350  # Pedir token fresco proactivamente cada N requests


def _randomize_fc_device(fotocasa_instance, token_cache=None):
    """Configura fingerprint: android_id del emulador Hetzner + resto random.

    Solo android_id debe coincidir con el token (se usa para Signature HMAC).
    session_id, device_token y user_agent se siguen randomizando para parecer
    dispositivos distintos desde IPs distintas (proxy rotation).

    token_cache: diccionario de cache de token a usar (default: _fc_token_slots[1]).
    """
    # android_id: del emulador Hetzner si disponible (HMAC Signature coherencia)
    cache = token_cache if token_cache is not None else _fc_token_slots.get(1, {})
    di = cache.get("device_info")
    if di and di.get("android_id"):
        fotocasa_instance.android_id = di["android_id"]
    else:
        fotocasa_instance.android_id = secrets.token_hex(8)
    # session_id: siempre nuevo por instancia (evita mismo device desde N IPs)
    fotocasa_instance.session_id = str(uuid.uuid4())
    # device_token: FCM-like, randomizar
    prefix = "f" + secrets.token_hex(10)[:11]
    suffix = secrets.token_urlsafe(100)[:100]
    fotocasa_instance.device_token = f"{prefix}:{suffix}"
    # user_agent: variar micro-version (realista, NO el del emulador cloud)
    micro = random.randint(0, 9)
    fotocasa_instance.user_agent = (
        f"AndroidApp/7.317.{micro} (14/34; sdk_gphone64_arm64; "
        f"sdk_gphone64_arm64; 6.1.23-android14-4-00257-g7e35917775b8-ab9964412; 12077443)"
    )

FC_ENRICH_CONFIG = {
    "num_workers": 3,            # 3 workers con slots Redroid dedicados (slots 1-3)
    "batch_size": 100,           # Subido de 50 → 100 (reduce overhead entre batches)
    "delay_min": 0.3,            # Reducido de 0.5 → 0.3 (~30% más rápido)
    "delay_max": 0.5,            # Reducido de 0.8 → 0.5
    "sleep_when_idle": 300,
    "restart_delay": 60,
    "error_sleep": 120,
    "token_refresh_interval": 500,
    # --- Flex worker: cuando no hay enrich, hacer check-active main_provinces ---
    # Cada worker tiene un threshold: si enrich_pending <= threshold → modo check-active
    "flex_thresholds": [0, 50, 200],  # w0: >0→enrich, w1: >50→enrich, w2: >200→enrich
    "flex_check_batch_size": 100,     # Batch size para check-active en modo flex
}

fc_enrich_state = {
    "running": False,
    "started_at": None,
    "totals": {"enriched": 0, "deactivated": 0, "errors": 0, "batches": 0},
    "status": "stopped",
    "workers": {},  # Per-worker status tracking
}

_fc_enrich_skip = {}  # {property_code: fail_count}
FC_ENRICH_MAX_RETRIES = 3

# --- Configuración de Fotocasa Check Active (proceso continuo, 2 workers) ---
# Similar a Idealista: requiere token Hetzner + proxy (Fotocasa tiene Cloudflare)
FC_CHECK_ACTIVE_CONFIG = {
    "workers_per_tier": [3, 5, 1, 2],  # capitals=3, main=5, second_capitals=1, rest=2 (11 total)
    "batch_size": 100,              # Subido de 50 → 100
    "delay_min": 0.3,               # Reducido de 0.5 → 0.3
    "delay_max": 0.5,               # Reducido de 0.8 → 0.5
    "session_rotate_every": 50,
    "sleep_when_idle": 300,
    "restart_delay": 30,
    "error_sleep": 60,
    "summary_interval": 21600,
    "token_refresh_interval": 400,
}

FC_CHECK_ACTIVE_TIERS = [
    {
        "name": "capitals",
        "cycle_hours": 4,
        "where_clause": (
            "province IN ('Madrid','Barcelona','Valencia') "
            "AND TRIM(municipality) IN ('Madrid Capital','Barcelona Capital','Valencia Capital')"
        ),
    },
    {
        "name": "main_provinces",
        "cycle_hours": 18,
        "where_clause": (
            "province IN ('Madrid','Barcelona','Valencia','Sevilla','Bizkaia',"
            "'Alicante','Málaga','Zaragoza','Illes Balears','Las Palmas') "
            "AND TRIM(municipality) NOT IN ('Madrid Capital','Barcelona Capital','Valencia Capital',"
            "'Sevilla Capital','Bilbao Capital','Málaga Capital','Zaragoza Capital')"
        ),
    },
    {
        "name": "second_capitals",
        "cycle_hours": 6,
        "where_clause": (
            "province IN ('Sevilla','Bizkaia','Málaga','Zaragoza') "
            "AND TRIM(municipality) IN ('Sevilla Capital','Bilbao Capital','Málaga Capital','Zaragoza Capital')"
        ),
    },
    {
        "name": "rest",
        "cycle_hours": 48,
        "where_clause": (
            "province NOT IN ('Madrid','Barcelona','Valencia','Sevilla','Bizkaia',"
            "'Alicante','Málaga','Zaragoza','Illes Balears','Las Palmas')"
        ),
    },
]

fc_check_active_state = {
    "running": False,
    "started_at": None,
    "workers": {},
    "totals": {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0, "by_tier": {}},
    "last_summary_at": 0,
}
_fc_ca_inflight = set()  # Propertycodes en vuelo — evita trabajo duplicado entre workers

# --- Semáforo adaptativo Fotocasa: throttle colectivo basado en 403s ---
# Ventana deslizante de 5 min. Todos los workers leen el mismo estado.
# 🟢 FAST: <2% error rate → delay 0.3-0.5s (velocidad normal)
# 🟡 CAUTIOUS: 2-8% error rate → delay 0.8-1.2s
# 🔴 SLOW: >8% error rate → delay 2.0-3.0s
FC_THROTTLE_WINDOW = 300  # 5 minutos de ventana deslizante
FC_THROTTLE_LEVELS = {
    "fast":     {"min": 0.3, "max": 0.5, "threshold": 0.02},   # <2% errors
    "cautious": {"min": 0.8, "max": 1.2, "threshold": 0.08},   # 2-8% errors
    "slow":     {"min": 2.0, "max": 3.0, "threshold": 1.0},    # >8% errors
}

_fc_throttle = {
    "events": [],       # lista de (timestamp, "ok"|"403") — ventana deslizante
    "current_level": "fast",
    "last_log_at": 0,   # evitar spam de logs en transiciones
}


def _fc_throttle_record(event_type: str):
    """Registra un evento (ok o 403) y recalcula el nivel del semáforo."""
    now = time.time()
    _fc_throttle["events"].append((now, event_type))

    # Purgar eventos fuera de la ventana
    cutoff = now - FC_THROTTLE_WINDOW
    _fc_throttle["events"] = [(t, e) for t, e in _fc_throttle["events"] if t > cutoff]

    events = _fc_throttle["events"]
    total = len(events)
    if total < 10:
        # Muy pocos datos, mantener nivel actual (evitar oscilaciones al arrancar)
        return

    error_count = sum(1 for _, e in events if e == "403")
    error_rate = error_count / total

    # Determinar nuevo nivel
    old_level = _fc_throttle["current_level"]
    if error_rate >= FC_THROTTLE_LEVELS["cautious"]["threshold"]:
        if error_rate >= FC_THROTTLE_LEVELS["slow"]["threshold"]:
            new_level = "slow"
        else:
            new_level = "cautious"
    else:
        new_level = "fast"

    _fc_throttle["current_level"] = new_level

    # Log solo en transiciones (máximo cada 30s para no spamear)
    if new_level != old_level and (now - _fc_throttle["last_log_at"]) > 30:
        emoji = {"fast": "🟢", "cautious": "🟡", "slow": "🔴"}[new_level]
        logger.info(
            f"[fc-throttle] {emoji} {old_level} → {new_level} "
            f"(error_rate={error_rate:.1%}, {error_count}/{total} in {FC_THROTTLE_WINDOW}s window)"
        )
        _fc_throttle["last_log_at"] = now


def _fc_throttle_get_delay() -> tuple:
    """Devuelve (delay_min, delay_max) según el nivel actual del semáforo."""
    level = _fc_throttle["current_level"]
    cfg = FC_THROTTLE_LEVELS[level]
    return cfg["min"], cfg["max"]

# --- Configuracion de Recheck (worker continuo) ---
RECHECK_CONFIG = {
    "batch_size": 1000,
    "delay_min": 0.3,
    "delay_max": 0.5,
    "session_rotate_every": 30,
    "sleep_when_idle": 300,     # 5 min si no hay pendientes
    "restart_delay": 30,        # Espera antes de reiniciar tras crash
    "error_sleep": 60,          # Espera si batch tiene >50% errores
}

recheck_state = {
    "running": False,
    "mode": "continuous",
    "totals": {"processed": 0, "reactivated": 0, "confirmed_inactive": 0, "errors": 0, "batches": 0},
    "last_batch_at": None,
}

# ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬ÂÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬ÂÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â ConfiguraciÃƒÆ’Ã‚Â³n de Supabase ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬ÂÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬ÂÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://xqbjvsnoqrtasxetoxil.supabase.co/rest/v1")
PROXY_URL_REGULAR = os.environ.get("PROXY_URL_REGULAR")
PROXY_URL_PREMIUM = os.environ.get("PROXY_URL")  # Alias para claridad
PROXY_REBOUNCE_SECONDS = 3600  # 1h en premium → intentar regular de nuevo


def _proxy_select(prefer_regular=True):
    """Selecciona proxy: regular ($0.1/GB) si disponible, premium ($5/GB) como fallback."""
    if prefer_regular and PROXY_URL_REGULAR:
        return PROXY_URL_REGULAR, "regular"
    return PROXY_URL_PREMIUM, "premium"
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhxYmp2c25vcXJ0YXN4ZXRveGlsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA5NDgzMDUsImV4cCI6MjA3NjUyNDMwNX0.uU0jIKX_oWsYq4C-DiEl4orzHUBhgDVBhq5vlNRVMRY")
DATABASE_URL = os.environ.get("DATABASE_URL")  # postgresql://postgres.[ref]:[pass]@db.[ref].supabase.co:5432/postgres
db_pool = None  # asyncpg pool, se crea en startup

# --- URLs del servidor Hetzner para tokens Fotocasa (16 slots Redroid) ---
# Servidor 1 (46.224.1.178): slots 1-4, 13-16
# Servidor 2 (91.107.212.236): slots 5-12
#
# Asignación:
#   Slots  1-3  → fc-enrich (3 workers)
#   Slots  4-6  → fc-check-active capitals (3 workers)
#   Slots  7-9, 13-15 → fc-check-active main_provinces (6 workers)
#   Slots 10-11 → fc-check-active rest (2 workers)
#   Slots 12, 16 → fc-recheck (2 workers)
HETZNER_TOKEN_URL    = os.environ.get("HETZNER_TOKEN_URL",    "http://46.224.1.178:8001/token")
HETZNER_TOKEN_URL_2  = os.environ.get("HETZNER_TOKEN_URL_2",  "http://46.224.1.178:8002/token")
HETZNER_TOKEN_URL_3  = os.environ.get("HETZNER_TOKEN_URL_3",  "http://46.224.1.178:8003/token")
HETZNER_TOKEN_URL_4  = os.environ.get("HETZNER_TOKEN_URL_4",  "http://46.224.1.178:8004/token")
HETZNER_TOKEN_URL_5  = os.environ.get("HETZNER_TOKEN_URL_5",  "http://91.107.212.236:8005/token")
HETZNER_TOKEN_URL_6  = os.environ.get("HETZNER_TOKEN_URL_6",  "http://91.107.212.236:8006/token")
HETZNER_TOKEN_URL_7  = os.environ.get("HETZNER_TOKEN_URL_7",  "http://91.107.212.236:8007/token")
HETZNER_TOKEN_URL_8  = os.environ.get("HETZNER_TOKEN_URL_8",  "http://91.107.212.236:8008/token")
HETZNER_TOKEN_URL_9  = os.environ.get("HETZNER_TOKEN_URL_9",  "http://91.107.212.236:8009/token")
HETZNER_TOKEN_URL_10 = os.environ.get("HETZNER_TOKEN_URL_10", "http://91.107.212.236:8010/token")
HETZNER_TOKEN_URL_11 = os.environ.get("HETZNER_TOKEN_URL_11", "http://91.107.212.236:8011/token")
HETZNER_TOKEN_URL_12 = os.environ.get("HETZNER_TOKEN_URL_12", "http://91.107.212.236:8012/token")
HETZNER_TOKEN_URL_13 = os.environ.get("HETZNER_TOKEN_URL_13", "http://46.224.1.178:8013/token")
HETZNER_TOKEN_URL_14 = os.environ.get("HETZNER_TOKEN_URL_14", "http://46.224.1.178:8014/token")
HETZNER_TOKEN_URL_15 = os.environ.get("HETZNER_TOKEN_URL_15", "http://46.224.1.178:8015/token")
HETZNER_TOKEN_URL_16 = os.environ.get("HETZNER_TOKEN_URL_16", "http://46.224.1.178:8016/token")

# â€”â€”â€” Whitelist de comarcas Habitaclia tratadas como capitales â€”â€”â€”
# Estas comarcas tienen alto volumen y se scrapean con capitalLimit (40) en vez de comarcaLimit (10)
HABITACLIA_COMARCAS_WHITELIST = {
    "alicante":  {"Vega Baja", "Marina Baixa", "Marina Alta"},
    "barcelona": {"BarcelonÃ¨s", "Maresme"},
    "granada":   {"Ãrea de Granada"},
    "malaga":    {"Costa Sol Occid-Ãrea Marbella", "Costa Sol Occid-Ãrea BenalmÃ¡dena",
                  "Valle del Guadalhorce", "Costa Sol Occid-Ãrea Estepona",
                  "Costa Sol Oriental-AxarquÃ­a"},
    "tarragona": {"Baix PenedÃ¨s", "TarragonÃ¨s"},
    "tenerife":  {"Adeje"},
}

# --- Slack Bot API para alertas con threading diario ---
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID")

# --- Anthropic API para clasificación de occupation_status ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# --- AURA iOS Backend — webhook de alertas de propiedades nuevas ---
AURA_BACKEND_URL = os.environ.get("AURA_BACKEND_URL", "https://aura-ios-backend-production.up.railway.app")
ALERTS_API_KEY = os.environ.get("ALERTS_API_KEY", "")
_notified_codes = set()
_notified_codes_last_reset = 0.0

# Estado de hilos diarios: cada categoria crea 1 hilo/dia natural (UTC)
_slack_threads = {
    "recheck": {"date": None, "ts": None},
    "deactivation": {"date": None, "ts": None},
    "mirror": {"date": None, "ts": None},
    "scrape_updates": {"date": None, "ts": None},
}

_THREAD_TITLES = {
    "recheck": "\U0001f4cb RECHECK PROCESSES",
    "deactivation": "\U0001f4cb DEACTIVATION WORKERS",
    "mirror": "\U0001f4cb MIRROR",
    "scrape_updates": "\U0001f4cb SCRAPE UPDATES",
}

# --- Configuración de Mirror Images (B2) ---
B2_KEY_ID = os.environ.get("B2_KEY_ID")
B2_APP_KEY = os.environ.get("B2_APP_KEY")
B2_BUCKET_NAME = os.environ.get("B2_BUCKET_NAME")
B2_ENDPOINT = os.environ.get("B2_ENDPOINT")
CDN_BASE_URL = os.environ.get("CDN_BASE_URL", "").rstrip("/")

MIRROR_CONFIG = {
    "batch_size": 30,
    "concurrent": 20,
    "delay_between_batches": 2.0,
}

mirror_state = {
    "running": False,
    "started_at": None,
    "totals": {"processed": 0, "images_uploaded": 0, "errors": 0},
    "status": "stopped",
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HABITACLIA CHECK ACTIVE — Configuración
#  Más agresivo que Idealista: sin anti-bot, sin JWT, sin proxy
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HAB_CHECK_ACTIVE_CONFIG = {
    "workers_per_tier": 3,       # 3 workers x 4 tiers = 12 workers totales
    "batch_size": 100,
    "concurrent_checks": 15,     # Requests concurrentes dentro de cada batch
    "delay_min": 0.02,           # 20ms (solo para no saturar event loop)
    "delay_max": 0.05,           # 50ms
    "sleep_when_idle": 900,      # 15 min cuando el tier asignado está al día (12 workers idle bombardean DB)
    "restart_delay": 30,         # Espera antes de reiniciar worker crasheado
    "error_sleep": 60,           # Espera si batch tiene >50% errores
    "summary_interval": 21600,   # Resumen Slack cada 6h (en segundos)
}

# --- Configuración de Habitaclia Enrich (worker de background) ---
HAB_ENRICH_CONFIG = {
    "batch_size": 100,
    "concurrent_checks": 15,     # Requests concurrentes para enrich
    "delay_min": 0.02,
    "delay_max": 0.05,
    "sleep_when_idle": 300,
    "restart_delay": 30,
    "error_sleep": 60,
}

hab_enrich_state = {
    "running": False,
    "started_at": None,
    "totals": {"enriched": 0, "deactivated": 0, "errors": 0, "batches": 0},
    "status": "stopped",
}

_hab_enrich_skip = {}  # {property_code: fail_count}
HAB_ENRICH_MAX_RETRIES = 3

HAB_CHECK_ACTIVE_TIERS = [
    {
        "name": "capitals",
        "cycle_hours": 2,
        "where_clause": (
            "province IN ('madrid','barcelona','valencia') "
            "AND municipality IN ('Madrid','Barcelona','Valencia')"
        ),
    },
    {
        "name": "main_provinces",
        "cycle_hours": 2,
        "where_clause": (
            "province IN ('madrid','barcelona','valencia','sevilla','vizcaya',"
            "'alicante','málaga','zaragoza','mallorca','gran canaria') "
            "AND municipality NOT IN ('Madrid','Barcelona','Valencia',"
            "'Sevilla','Bilbao','Málaga','Zaragoza')"
        ),
    },
    {
        "name": "second_capitals",
        "cycle_hours": 2,
        "where_clause": (
            "province IN ('sevilla','vizcaya','málaga','zaragoza') "
            "AND municipality IN ('Sevilla','Bilbao','Málaga','Zaragoza')"
        ),
    },
    {
        "name": "rest",
        "cycle_hours": 4,
        "where_clause": (
            "province NOT IN ('madrid','barcelona','valencia','sevilla','vizcaya',"
            "'alicante','málaga','zaragoza','mallorca','gran canaria')"
        ),
    },
]

hab_check_active_state = {
    "running": False,
    "started_at": None,
    "workers": {},
    "totals": {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0, "by_tier": {}},
    "last_summary_at": 0,
}
_hab_ca_inflight = set()  # Propertycodes en vuelo — evita trabajo duplicado entre workers

# ━━━ HABITACLIA SCRAPE UPDATES — Configuración ━━━
# Workers continuos que reemplazan el CRON de n8n

HAB_SCRAPE_UPDATES_CONFIG = {
    "capital_limit": 40,
    "comarca_limit": 10,
    "continue_threshold_capital": 35,    # >=35 de 40 nuevas → seguir paginando (capital/WL)
    "continue_threshold_comarca": 9,     # >=9 de 10 → promover a WL + seguir paginando
    "continue_threshold_whitelist": 35,  # >=35 de 40 → seguir paginando (WL)
    "stop_threshold": 5,                 # <5 inserts en una página → parar
    "max_extra_pages": 8,                # Safety cap: máx 8 páginas extra por zona
    "restart_delay": 30,
    "inter_zone_delay": 0.5,
    "inter_province_delay": 2,
    "overflow_threshold": 180,          # Per-cycle overflow (seguridad)
    "zone_overflow_threshold": 180,     # Per-zone: parar + flag si una zona acumula >180 nuevas
}

HAB_SCRAPE_UPDATES_SCHEDULE = {
    "capitals": {
        0: 1800, 1: 1800, 2: 1800, 3: 1800, 4: 1800, 5: 1800,
        6: 600, 7: 600, 8: 600,
        9: 300, 10: 300, 11: 300, 12: 300, 13: 300, 14: 300, 15: 300, 16: 300,
        17: 600, 18: 600, 19: 600, 20: 600, 21: 600, 22: 600, 23: 600,
    },
    "main_provinces": {
        0: 3600, 1: 3600, 2: 3600, 3: 3600, 4: 3600, 5: 3600,
        6: 900, 7: 900, 8: 900,
        9: 300, 10: 300, 11: 300, 12: 300, 13: 300, 14: 300, 15: 300, 16: 300,
        17: 600, 18: 600, 19: 600, 20: 600, 21: 600, 22: 600, 23: 600,
    },
    "rest": {
        0: 3600, 1: 3600, 2: 3600, 3: 3600, 4: 3600, 5: 3600,
        6: 1800, 7: 1800, 8: 1800,
        9: 900, 10: 900, 11: 900, 12: 900, 13: 900, 14: 900, 15: 900, 16: 900,
        17: 1800, 18: 1800, 19: 1800, 20: 1800, 21: 1800, 22: 1800, 23: 1800,
    },
}

hab_scrape_updates_state = {
    "running": False,
    "started_at": None,
    "workers": {},
    "totals": {
        "cycles": 0, "provinces_scraped": 0, "inserted": 0,
        "updated": 0, "enriched": 0, "max_pages_hits": 0, "errors": 0,
    },
    "last_summary_at": 0,        # timestamp del último resumen Slack
    "summary_interval": 21600,   # 6h en segundos
    # Acumulador entre resúmenes: {province_name: {inserted, updated, enriched, max_pages_zones: [zone_names]}}
    "accumulated": {},
}

# ━━━ IDEALISTA SCRAPE UPDATES — Configuración ━━━
# Workers continuos que reemplazan el CRON de n8n para Idealista.
# Usa TLS client + JWT token + proxy. Sin enrichment inline (el enrich worker existente lo hace).

ID_SCRAPE_UPDATES_CONFIG = {
    "max_items": 50,              # maxItems de la API Idealista
    "continue_threshold": 15,     # >=15 de 50 nuevas en la página → seguir paginando
    "stop_threshold": 5,          # <5 inserts en una página → parar (ya son conocidas)
    "max_pages": 10,              # Safety cap: máximo 10 páginas por provincia
    "overflow_threshold": 100,    # >100 inserted en un ciclo → alerta
    "restart_delay": 30,          # Espera antes de reiniciar worker crasheado
    "inter_province_delay": 2,    # Segundos entre provincias
    "session_rotate_every": 10,   # Rotar TLS session cada N provincias
}

# Sleep en segundos entre ciclos, por tier y hora UTC.
# Idealista tiene muchos menos gaps que Habitaclia → schedule más relajado
ID_SCRAPE_UPDATES_SCHEDULE = {
    "capitals": {
        0: 1800, 1: 1800, 2: 1800, 3: 1800, 4: 1800, 5: 1800,
        6: 600, 7: 600, 8: 600,
        9: 300, 10: 300, 11: 300, 12: 300, 13: 300, 14: 300, 15: 300, 16: 300,
        17: 600, 18: 600, 19: 600, 20: 600, 21: 600, 22: 600, 23: 600,
    },
    "main_provinces": {
        0: 3600, 1: 3600, 2: 3600, 3: 3600, 4: 3600, 5: 3600,
        6: 1200, 7: 1200, 8: 1200,
        9: 600, 10: 600, 11: 600, 12: 600, 13: 600, 14: 600, 15: 600, 16: 600,
        17: 1200, 18: 1200, 19: 1200, 20: 1200, 21: 1200, 22: 1200, 23: 1200,
    },
    "rest": {
        0: 3600, 1: 3600, 2: 3600, 3: 3600, 4: 3600, 5: 3600,
        6: 1800, 7: 1800, 8: 1800,
        9: 900, 10: 900, 11: 900, 12: 900, 13: 900, 14: 900, 15: 900, 16: 900,
        17: 1800, 18: 1800, 19: 1800, 20: 1800, 21: 1800, 22: 1800, 23: 1800,
    },
}

id_scrape_updates_state = {
    "running": False,
    "started_at": None,
    "workers": {},
    "totals": {
        "cycles": 0, "provinces_scraped": 0, "inserted": 0,
        "updated": 0, "max_pages_hits": 0, "errors": 0,
    },
    "last_summary_at": 0,
    "summary_interval": 21600,   # 6h
    "accumulated": {},           # {province_name: {inserted, updated, max_pages_hit: bool}}
}

# ━━━ FOTOCASA SCRAPE UPDATES — Configuración ━━━
# Workers continuos que reemplazan el CRON de n8n para Fotocasa.
# Usa httpx + proxy + CSRF session. SIN token Hetzner (search no lo requiere).
# SIN enrichment inline (el fc-enrich worker existente se encarga).
# Es la plataforma con más gaps (83% de todos los gaps históricos).

FC_SCRAPE_UPDATES_CONFIG = {
    "page_size": 36,              # pageSize de la API Fotocasa
    "pages_per_province": 2,      # 2 páginas × 36 = 72 propiedades (ronda inicial)
    "continue_threshold": 10,     # >=10 de 36 nuevas en la última pág → seguir (~28%)
    "stop_threshold": 5,          # <5 inserts en una página → parar
    "max_pages": 10,              # Safety cap: máximo 10 páginas por provincia
    "overflow_threshold": 200,    # >200 inserted en un ciclo → alerta
    "zone_overflow_threshold": 180, # Per-province: parar si acumula >180 nuevas
    "restart_delay": 30,
    "inter_province_delay": 3,    # Segundos entre provincias (más que Idealista por CSRF)
    "inter_page_delay": 2,        # Delay entre páginas de la misma provincia
    "proxy_rotate_every": 10,     # Rotar proxy cada N provincias
}

# Sleep entre ciclos (segundos). Fotocasa necesita refresh más frecuente
# por su alta tasa de gaps durante horario pico.
FC_SCRAPE_UPDATES_SCHEDULE = {
    "capitals": {
        0: 1800, 1: 1800, 2: 1800, 3: 1800, 4: 1800, 5: 1800,
        6: 600, 7: 600, 8: 600,
        9: 300, 10: 300, 11: 300, 12: 300, 13: 300, 14: 300, 15: 300, 16: 300,
        17: 600, 18: 600, 19: 600, 20: 600, 21: 600, 22: 600, 23: 600,
    },
    "main_provinces": {
        0: 3600, 1: 3600, 2: 3600, 3: 3600, 4: 3600, 5: 3600,
        6: 900, 7: 900, 8: 900,
        9: 300, 10: 300, 11: 300, 12: 300, 13: 300, 14: 300, 15: 300, 16: 300,
        17: 600, 18: 600, 19: 600, 20: 600, 21: 600, 22: 600, 23: 600,
    },
    "rest": {
        0: 3600, 1: 3600, 2: 3600, 3: 3600, 4: 3600, 5: 3600,
        6: 1800, 7: 1800, 8: 1800,
        9: 900, 10: 900, 11: 900, 12: 900, 13: 900, 14: 900, 15: 900, 16: 900,
        17: 1800, 18: 1800, 19: 1800, 20: 1800, 21: 1800, 22: 1800, 23: 1800,
    },
}

fc_scrape_updates_state = {
    "running": False,
    "started_at": None,
    "workers": {},
    "totals": {
        "cycles": 0, "provinces_scraped": 0, "inserted": 0,
        "updated": 0, "max_pages_hits": 0, "errors": 0,
    },
    "last_summary_at": 0,
    "summary_interval": 21600,   # 6h
    "accumulated": {},           # {province_name: {inserted, updated, max_pages_hit: bool}}
}
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  AUDIT — Error store + snapshot para /workers-audit
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Acumula errores desde la última auditoría. Se resetea al llamar /workers-audit.
# Cada error se deduplica por "firma" (process|error_type).
# Timestamps capped a 100 por firma para controlar memoria.

_audit_error_store = {}   # {signature: {count, first_at, last_at, timestamps: []}}
_audit_last_snapshot = None  # Snapshot de la auditoría anterior (para comparar velocidades)
_audit_last_at = None        # Timestamp de la última auditoría

AUDIT_MAX_TIMESTAMPS = None   


def _audit_err(process: str, error_type: str, detail: str = None):
    """Registra un error en el audit store. Llamada ligera (~0 overhead)."""
    sig = f"{process}|{error_type}"
    now_iso = datetime.now(timezone.utc).strftime("%H:%M:%S")
    
    if sig not in _audit_error_store:
        _audit_error_store[sig] = {
            "process": process,
            "error_type": error_type,
            "count": 0,
            "first_at": now_iso,
            "last_at": now_iso,
            "timestamps": [],
            "sample_detail": detail,
        }
    
    entry = _audit_error_store[sig]
    entry["count"] += 1
    entry["last_at"] = now_iso
    entry["timestamps"].append(now_iso)
    if detail and not entry["sample_detail"]:
        entry["sample_detail"] = detail




# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
#  MODELOS DE ENTRADA ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â IDEALISTA (sin cambios)
# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

class ScrapeRequest(BaseModel):
    coordinates: Optional[str] = None
    locationIds: Optional[str] = None
    operation: str = "sale"
    propertyType: str = "homes"
    maxItems: int = 50
    numPage: int = 1
    order: str = "weigh"
    minPrice: Optional[int] = None
    maxPrice: Optional[int] = None
    minSize: Optional[int] = None
    maxSize: Optional[int] = None
    bedrooms: List[str] = []
    bathrooms: List[str] = []
    preservations: str = ""
    flat: bool = False
    onlyFlats: bool = False
    duplex: bool = False
    penthouse: bool = False
    chalet: bool = False
    countryHouse: bool = False
    independentHouse: bool = False
    semidetachedHouse: bool = False
    terracedHouse: bool = False
    loftType: bool = False
    casaBajaType: bool = False
    villaType: bool = False
    apartamentoType: bool = False
    accessible: bool = False
    airConditioning: bool = False
    storeRoom: bool = False
    builtinWardrobes: bool = False
    exterior: bool = True
    garage: bool = False
    swimmingPool: bool = False
    elevator: bool = False
    luxury: bool = False
    terrace: bool = False
    garden: bool = False
    floorHeights: str = ""
    currentOccupationType: List[str] = []


class ScrapeUpdatesRequest(BaseModel):
    """Request para el endpoint /scrape-updates (usado por n8n CRON)"""
    locationIds: str
    operation: str = "sale"
    propertyType: str = "homes"
    maxItems: int = 50

# --- 1. Añadir este modelo junto a los otros modelos ---

class PropertyDetailRequest(BaseModel):
    """Request para obtener detalle de una propiedad individual de Idealista"""
    propertyId: str
    country: str = "es"


# --- 2. Añadir este endpoint junto a los otros endpoints de Idealista ---
# Ubicación sugerida: después del endpoint /scrape (línea ~2674)

@app.post("/idealista/detail")
async def idealista_detail(request: PropertyDetailRequest):
    """
    Obtiene el detalle completo de una propiedad de Idealista.
    Usado por el onepager-orchestrator para generar one-pagers.
    """
    logger.info(f"[idealista/detail] Getting detail for property: {request.propertyId}")

    try:
        idealista, _, _ = await _get_idealista_client()

        success, response = await idealista.get_property_detail(
            property_id=request.propertyId,
            country=request.country,
        )

        if not success:
            error_status = response.get("status_code", 502) if isinstance(response, dict) else 502
            error_msg = response.get("error", str(response)) if isinstance(response, dict) else str(response)
            logger.error(f"[idealista/detail] Failed for {request.propertyId}: {error_msg}")
            raise HTTPException(
                status_code=error_status,
                detail=f"Idealista detail failed: {error_msg}"
            )

        logger.info(f"[idealista/detail] Detail obtained for property {request.propertyId}")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[idealista/detail] Exception: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
#  MODELOS DE ENTRADA ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â FOTOCASA
# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

class FotocasaScrapeRequest(BaseModel):
    """BÃƒÆ’Ã‚Âºsqueda por coordenadas (boundingBox POST)"""
    propertyType: str = "HOME"
    transactionType: str = "SALE"
    latitude: float = 40.41456536556915
    longitude: float = -3.7040268816053867
    polygon: Optional[str] = None           # "lng,lat;lng,lat;..."
    mapBoundingBox: Optional[str] = None    # bounding box string
    page: int = 1
    pageSize: int = 36
    sort: int = 0
    zoom: int = 12
    # Filtros
    purchaseType: Optional[List[str]] = None
    rentalDuration: Optional[str] = None
    propertySubTypes: Optional[List[str]] = None
    roomsFrom: Optional[int] = None
    roomsTo: Optional[int] = None
    bathroomsFrom: Optional[int] = None
    bathroomsTo: Optional[int] = None
    priceFrom: Optional[int] = None
    priceTo: Optional[int] = None
    surfaceFrom: Optional[int] = None
    surfaceTo: Optional[int] = None
    conservationStates: Optional[List[str]] = None
    extras: Optional[List[str]] = None
    orientations: Optional[str] = None
    occupancyStatus: Optional[str] = None


class FotocasaLocationsRequest(BaseModel):
    """BÃƒÆ’Ã‚Âºsqueda por location codes (GET v3/placeholders/search)"""
    propertyType: str = "HOME"
    transactionType: str = "SALE"
    locations: str = "724,14,28,0,0,0,0,0,0"  # Madrid provincia por defecto
    page: int = 1
    pageSize: int = 36
    sort: int = 0
    conservationStates: str = "UNDEFINED"
    # Filtros opcionales
    purchaseType: Optional[List[str]] = None
    rentalDuration: Optional[str] = None
    propertySubTypes: Optional[List[str]] = None
    roomsFrom: Optional[int] = None
    roomsTo: Optional[int] = None
    bathroomsFrom: Optional[int] = None
    bathroomsTo: Optional[int] = None
    priceFrom: Optional[int] = None
    priceTo: Optional[int] = None
    surfaceFrom: Optional[int] = None
    surfaceTo: Optional[int] = None
    extras: Optional[List[str]] = None
    occupancyStatus: Optional[str] = None


class FotocasaDetailRequest(BaseModel):
    """Detalle de propiedad individual"""
    propertyId: str                         # ID numÃƒÆ’Ã‚Â©rico de la propiedad
    transactionType: str = "SALE"
    useHetznerToken: bool = True            # Obtener token fresco de Hetzner


class FotocasaScrapeUpdatesRequest(BaseModel):
    """Request para el endpoint /fotocasa/scrape-updates (usado por n8n CRON)"""
    locations: str = "724,14,28,0,0,0,0,0,0"
    transactionType: str = "SALE"
    propertyType: str = "HOME"
    pageSize: int = 36
    pages: int = 2  # Numero de paginas a scrapear


# Ã¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”Â
#  MODELOS DE ENTRADA Ã¢â‚¬â€ HABITACLIA
# Ã¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”Â

class HabitacliaScrapeRequest(BaseModel):
    """BÃƒÂºsqueda por coordenadas (bounding box) Ã¢â‚¬â€ GetClusters"""
    minLat: float
    minLon: float
    maxLat: float
    maxLon: float
    operation: str = "V"                          # V=Venta, A=Alquiler
    propertyTypes: Optional[List[str]] = None     # pisos, aticos, duplex, casas
    surfaceMin: int = 0
    roomsMin: int = 99                            # 99 = sin filtro
    priceMax: int = 99999999
    onlyWithPhotos: bool = False
    advancedFilters: Optional[List[str]] = None   # obranueva, parking, piscina, etc.
    publicationPeriod: int = 0                    # 0=todos, 1=semana, 2=mes, 3=3dÃƒÂ­as
    zoomLevel: int = 25
    clusterDistance: int = 35
    codProv: int = 1
    codCom: int = 1
    fetchDetails: bool = False                    # Si true, obtener detalle de cada propiedad


class HabitacliaDiscoverTreeRequest(BaseModel):
    """Descubrimiento del ÃƒÂ¡rbol de ubicaciones de una provincia"""
    locationCode: str                             # ej: "madrid", "albacete", "barcelona"
    operation: str = "comprar"                    # "comprar" o "alquilar"
    propertyType: str = "vivienda"                # "vivienda", "piso", etc.


class HabitacliaListingsRequest(BaseModel):
    """Scrape de listings paginados por slug"""
    listingSlug: str                              # ej: "viviendas-albacete"
    sort: str = "puntuacion_habitaclia"           # Criterio de ordenaciÃƒÂ³n
    maxPages: Optional[int] = None                # LÃƒÂ­mite de pÃƒÂ¡ginas (None = todas)
    fetchDetails: bool = False                    # Si true, obtener detalle de cada propiedad


class HabitacliaDetailRequest(BaseModel):
    """Detalle individual de propiedad"""
    codEmp: int                                   # CÃƒÂ³digo empresa
    codInm: int                                   # CÃƒÂ³digo inmueble

class IdealistaBackfillRequest(BaseModel):
    """One-shot search + upsert para backfill de gaps."""
    locationIds: str = "[0-EU-ES-28]"   # ej: Madrid
    numPages: int = 3                    # Páginas a scrapear
    maxItems: int = 50                   # Props por página

class HabitacliaScrapeUpdatesRequest(BaseModel):
    """
    Request para /habitaclia/scrape-updates (CRON via n8n).
    Consulta habitaclia_tree en Supabase y scrapea las propiedades
    mÃƒÂ¡s recientes de cada zona de la provincia.
    """
    locationCode: str                       # ej: "albacete", "madrid", "barcelona"
    capitalLimit: int = 40                  # Props a obtener de capitales (is_aggregate=false)
    comarcaLimit: int = 10                  # Props a obtener de comarcas (is_aggregate=true)
    maxPagesPerZone: int = 1            # Páginas por zona (1 = normal, >1 = backfill)


# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
#  HEALTH CHECK
# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

@app.on_event("startup")
async def startup_event():
    """Crea pool de DB y arranca workers."""
    global db_pool
    if DATABASE_URL:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=2, max_size=15,
            statement_cache_size=0,
        )
        logger.info(f"[startup] asyncpg pool created (min=2, max=15)")
    else:
        logger.error("[startup] DATABASE_URL not set! Workers will not start.")
        return

    # Cargar whitelist dinámica de comarcas desde DB
    try:
        rows = await db_pool.fetch(
            "SELECT location_code, comarca_name FROM habitaclia_dynamic_whitelist"
        )
        for row in rows:
            loc = row["location_code"]
            name = row["comarca_name"]
            if loc not in HABITACLIA_COMARCAS_WHITELIST:
                HABITACLIA_COMARCAS_WHITELIST[loc] = set()
            HABITACLIA_COMARCAS_WHITELIST[loc].add(name)
        if rows:
            logger.info(f"[startup] Loaded {len(rows)} dynamic whitelist entries")
    except Exception as e:
        logger.warning(f"[startup] Could not load dynamic whitelist (table may not exist): {e}")

    if SERVER_MODE in ("workers", "full"):
        await asyncio.sleep(5)  # Esperar cold start de Render
        asyncio.create_task(_start_check_active())
        asyncio.create_task(_start_enrich())
        asyncio.create_task(_periodic_summary_loop())
        asyncio.create_task(_start_id_scrape_updates())
        asyncio.create_task(_start_id_recheck())
        logger.info("[startup] Idealista background workers launched (including recheck)")
        asyncio.create_task(_start_hab_check_active())
        asyncio.create_task(_start_hab_enrich())
        asyncio.create_task(_start_hab_scrape_updates())
        asyncio.create_task(_start_hab_recheck())
        logger.info("[startup] Habitaclia recheck continuous worker launched")
        asyncio.create_task(_start_fc_recheck())
        logger.info("[startup] Fotocasa recheck continuous worker launched (slot 12)")
        asyncio.create_task(_start_fc_check_active())
        logger.info("[startup] Fotocasa check-active workers launched (2 workers, slots 3-4)")
        asyncio.create_task(_start_fc_scrape_updates())
        logger.info("[startup] Fotocasa scrape-updates workers launched")
        asyncio.create_task(_start_fc_enrich())
        logger.info("[startup] Fotocasa enrich flex workers launched (slots 1-3)")
        asyncio.create_task(_start_occupation_ai())
        logger.info("[startup] Occupation AI classifier worker launched (delayed 2min)")
    else:
        logger.info(f"[startup] SERVER_MODE={SERVER_MODE} — no background workers launched")

@app.on_event("shutdown")
async def shutdown_event():
    """Cierra pool de DB."""
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("[shutdown] asyncpg pool closed")


@app.get("/health")
async def health():
    return {"status": "ok"}


# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
#  IDEALISTA ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Helpers (sin cambios)
# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  OCCUPATION STATUS — Helpers para extracción estructural
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_ID_LABEL_TO_OCCUPATION = {
    "occupation.bareOwnership": "bare_ownership",
    "occupation.tenanted": "rented",
    "occupation.illegallyOccupied": "occupied",
}

def _extract_occupation_from_id_labels(labels):
    """Extrae occupation_status de Idealista labels (scrape + detail)."""
    if not labels or not isinstance(labels, list):
        return None
    for label in labels:
        name = label.get("name", "") if isinstance(label, dict) else ""
        if name in _ID_LABEL_TO_OCCUPATION:
            return _ID_LABEL_TO_OCCUPATION[name]
    return None

def _extract_occupation_from_id_translated_texts(detail_data):
    """Extrae occupation_status de translatedTexts.currentOccupationType (Idealista detail)."""
    translated = detail_data.get("translatedTexts", {})
    for section in translated.get("characteristicsDescriptions", []):
        if section.get("key") == "currentOccupationType":
            features = section.get("detailFeatures", [])
            if features:
                phrase = features[0].get("phrase", "").lower()
                if "nuda propiedad" in phrase:
                    return "bare_ownership"
                if "ocupada ilegalmente" in phrase:
                    return "occupied"
                if "alquilad" in phrase:
                    return "rented"
                # Fallback: si hay currentOccupationType con contenido pero no matchea
                return "occupied"
    return None

def _extract_occupation_from_fc_dynamic(dynamic_features):
    """Extrae occupation_status de Fotocasa dynamicFeatures."""
    if not dynamic_features or not isinstance(dynamic_features, list):
        return None
    names = [f.get("feature", "") for f in dynamic_features if isinstance(f, dict)]
    if "IS_OCCUPIED" in names:
        return "occupied"
    if "IS_RENTED" in names:
        return "rented"
    return None

def transform_property(prop):
    """Transforma una propiedad del formato Idealista al formato de Supabase."""
    result = {
        "propertycode": prop.get("propertyCode"),
        "thumbnail": prop.get("thumbnail"),
        "externalreference": prop.get("externalReference"),
        "numphotos": prop.get("numPhotos"),
        "floor": prop.get("floor"),
        "price": prop.get("price"),
        "propertytype": prop.get("propertyType"),
        "operation": prop.get("operation"),
        "size": prop.get("size"),
        "exterior": prop.get("exterior"),
        "rooms": prop.get("rooms"),
        "bathrooms": prop.get("bathrooms"),
        "address": prop.get("address"),
        "province": prop.get("province"),
        "municipality": prop.get("municipality"),
        "district": prop.get("district"),
        "country": prop.get("country"),
        "neighborhood": prop.get("neighborhood"),
        "locationid": prop.get("locationId"),
        "latitude": prop.get("latitude"),
        "longitude": prop.get("longitude"),
        "showaddress": prop.get("showAddress"),
        "url": prop.get("url"),
        "distance": prop.get("distance"),
        "description": prop.get("description"),
        "hasvideo": prop.get("hasVideo"),
        "status": prop.get("status"),
        "newdevelopment": prop.get("newDevelopment"),
        "favourite": prop.get("favourite"),
        "newproperty": prop.get("newProperty"),
        "haslift": prop.get("hasLift"),
        "pricebyarea": prop.get("priceByArea"),
        "hasplan": prop.get("hasPlan"),
        "has3dtour": prop.get("has3DTour"),
        "has360": prop.get("has360"),
        "hasstaging": prop.get("hasStaging"),
        "preferencehighlight": prop.get("preferenceHighlight"),
        "tophighlight": prop.get("topHighlight"),
        "topnewdevelopment": prop.get("topNewDevelopment"),
        "newdevelopmenthighlight": prop.get("newDevelopmentHighlight"),
        "topplus": prop.get("topPlus"),
        "urgentvisualhighlight": prop.get("urgentVisualHighlight"),
        "visualhighlight": prop.get("visualHighlight"),
        "priceinfo": prop.get("priceInfo"),
        "contactinfo": prop.get("contactInfo"),
        "features": prop.get("features"),
        "detailedtype": prop.get("detailedType"),
        "suggestedtexts": prop.get("suggestedTexts"),
        "multimedia": prop.get("multimedia"),
        "highlight": prop.get("highlight"),
        "parkingspace": prop.get("parkingSpace"),
        "savedad": prop.get("savedAd"),
        "labels": prop.get("labels"),
        "ribbons": prop.get("ribbons"),
        "notes": prop.get("notes"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "sell_type": "new_build" if prop.get("newDevelopment") is True else ("second_hand" if prop.get("newDevelopment") is False else None),
        "source": "idealista",
        "adisactive": True,

    }
    # occupation_status desde labels (solo si hay valor, no sobreescribir con null)
    _occ = _extract_occupation_from_id_labels(prop.get("labels"))
    if _occ:
        result["occupation_status"] = _occ
    return result



async def notify_new_properties(property_codes: list[str]):
    """
    Fire-and-forget: notifica al backend iOS para evaluar alertas.
    Envía en batches de 5000 códigos máximo.
    Dedup en memoria: si otro worker ya notificó el mismo code
    en la última hora, lo descarta.
    Si falla, solo logea — no afecta al scraper.
    """
    global _notified_codes, _notified_codes_last_reset

    if not property_codes or not ALERTS_API_KEY:
        return

    # Limpiar set cada hora para no crecer indefinidamente
    now = time.time()
    if now - _notified_codes_last_reset > 3600:
        _notified_codes = set()
        _notified_codes_last_reset = now

    # Filtrar codes ya notificados por otro worker
    fresh = [c for c in property_codes if c not in _notified_codes]
    if not fresh:
        return
    _notified_codes.update(fresh)

    try:
        async with httpx.AsyncClient(timeout=30.0) as wh_client:
            for i in range(0, len(fresh), 5000):
                batch = fresh[i:i + 5000]
                await wh_client.post(
                    f"{AURA_BACKEND_URL}/alerts/match",
                    json={"property_codes": batch},
                    headers={"X-API-Key": ALERTS_API_KEY},
                )
        logger.info(f"[alerts-webhook] Sent {len(fresh)} codes ({len(property_codes) - len(fresh)} deduped)")
    except Exception as e:
        logger.warning(f"[alerts-webhook] Failed (non-blocking): {e}")


async def upsert_to_supabase(client: httpx.AsyncClient, properties: list, _new_codes_out: list = None):
    """
    Hace upsert en Supabase usando una sola llamada batch.
    Devuelve (success, inserted, updated, error_message)
    Si _new_codes_out es una lista, acumula ahí los propertycodes nuevos.
    """
    if not properties:
        return True, 0, 0, None

    transformed = [transform_property(p) for p in properties]
    property_codes = [p["propertycode"] for p in transformed]

    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }

    # 1. Consultar cuÃƒÆ’Ã‚Â¡les ya existen
    try:
        query_url = f"{SUPABASE_URL}/async_properties?propertycode=in.({','.join(property_codes)})&select=propertycode"
        response = await client.get(query_url, headers=headers, timeout=30)

        if response.status_code == 200:
            existing = {item["propertycode"] for item in response.json()}
        else:
            logger.warning(f"Error checking existing properties: {response.status_code}")
            existing = set()
    except Exception as e:
        logger.warning(f"Exception checking existing properties: {e}")
        existing = set()

    # 2. Separar en nuevos vs existentes
    new_props = [p for p in transformed if p["propertycode"] not in existing]
    update_props = [p for p in transformed if p["propertycode"] in existing]

    inserted = 0
    updated = 0
    errors = []

    # 3. Insertar nuevos (batch)
    if new_props:
        try:
            response = await client.post(
                f"{SUPABASE_URL}/async_properties?on_conflict=propertycode",
                headers=headers,
                json=new_props,
                timeout=30
            )
            if response.status_code in [200, 201]:
                inserted = len(new_props)
                logger.info(f"Inserted {inserted} new properties")
                # Acumular codes nuevos para webhook al final del ciclo
                if _new_codes_out is not None:
                    _new_codes_out.extend(p["propertycode"] for p in new_props)
            else:
                errors.append(f"Insert error: {response.status_code} - {response.text[:100]}")
        except Exception as e:
            errors.append(f"Insert exception: {str(e)}")

    # 4. Actualizar existentes (batch con upsert)
    if update_props:
        # Eliminar campos null para no sobreescribir datos enriched
        update_props_clean = [
            {k: v for k, v in p.items() if v is not None}
            for p in update_props
        ]
        try:
            headers_upsert = headers.copy()
            headers_upsert["Prefer"] = "resolution=merge-duplicates"

            response = await client.post(
                f"{SUPABASE_URL}/async_properties?on_conflict=propertycode",
                headers=headers_upsert,
                json=update_props_clean,
                timeout=30
            )
            if response.status_code in [200, 201]:
                updated = len(update_props)
                logger.info(f"Updated {updated} existing properties")
            else:
                errors.append(f"Update error: {response.status_code} - {response.text[:100]}")
        except Exception as e:
            errors.append(f"Update exception: {str(e)}")

    error_message = "; ".join(errors) if errors else None
    return len(errors) == 0, inserted, updated, error_message

def _parse_fc_price(val):
    """Parse Fotocasa price: puede ser numérico o string tipo '25,000 €'."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        cleaned = str(val).replace('€', '').replace(' ', '').replace(',', '').strip()
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None

def transform_fotocasa_property(prop):
    """Transforma una propiedad del formato Fotocasa al formato de Supabase.
    Normaliza los campos para ser compatibles con el esquema de Idealista."""
    segment = prop.get("segmentData", {})
    floor_info = prop.get("propertyFloor", {})
    agency = prop.get("agency", {})

    # Ã¢â€â‚¬Ã¢â€â‚¬ Parse extraList codes Ã¢â€â‚¬Ã¢â€â‚¬
    extra_list_str = prop.get("extraList", "")
    extra_codes = set(extra_list_str.split()) if extra_list_str else set()

    # Ã¢â€â‚¬Ã¢â€â‚¬ Parse dynamicFeatures Ã¢â€â‚¬Ã¢â€â‚¬
    dynamic_features = prop.get("dynamicFeatures", [])
    dynamic_feature_names = [f.get("feature", "") for f in dynamic_features if isinstance(f, dict)]

    # Ã¢â€â‚¬Ã¢â€â‚¬ exterior from dynamicFeatures Ã¢â€â‚¬Ã¢â€â‚¬
    is_exterior = "IS_EXTERIOR" in dynamic_feature_names

    # Ã¢â€â‚¬Ã¢â€â‚¬ haslift from extraList code 13 Ã¢â€â‚¬Ã¢â€â‚¬
    has_lift = "13" in extra_codes

    # Ã¢â€â‚¬Ã¢â€â‚¬ features dict (Idealista-compatible) Ã¢â€â‚¬Ã¢â€â‚¬
    features = {}
    if "1" in extra_codes:
        features["hasAirConditioning"] = True
    if "17" in extra_codes:
        features["hasSwimmingPool"] = True
    if "10" in extra_codes:
        features["hasTerrace"] = True
    if "11" in extra_codes:
        features["hasBoxRoom"] = True
    if "7" in extra_codes:
        features["hasGarden"] = True
    if "2" in extra_codes or "3" in extra_codes:
        features["hasHeating"] = True

    # Ã¢â€â‚¬Ã¢â€â‚¬ parkingspace from extraList code 5 Ã¢â€â‚¬Ã¢â€â‚¬
    has_parking = "5" in extra_codes
    parking_space = {"hasParkingSpace": True, "isParkingSpaceIncludedInPrice": False} if has_parking else None

    # Ã¢â€â‚¬Ã¢â€â‚¬ contactinfo with userType Ã¢â€â‚¬Ã¢â€â‚¬
    contact_info = {}
    if prop.get("phone"):
        contact_info["phone1"] = {"phoneNumber": prop["phone"]}
    if agency:
        contact_info["agencyName"] = agency.get("alias") or agency.get("name")
        contact_info["agencyLogo"] = agency.get("logo")
    is_professional = prop.get("isProfessional")
    if is_professional is not None:
        contact_info["userType"] = "professional" if is_professional else "private"

    # Ã¢â€â‚¬Ã¢â€â‚¬ multimedia Ã¢â€â‚¬Ã¢â€â‚¬
    media_list = prop.get("mediaList", [])
    multimedia = None
    if media_list:
        images = []
        for m in media_list:
            url_dtos = m.get("mediaUrlDtos", [])
            large_url = None
            for dto in url_dtos:
                if dto.get("mediaSize") == "LARGE":
                    large_url = dto.get("url")
                    break
            images.append({"url": large_url or m.get("url", "")})
        multimedia = {"images": images}

    # Ã¢â€â‚¬Ã¢â€â‚¬ isDevelopment Ã¢â€â‚¬Ã¢â€â‚¬
    is_development = prop.get("isDevelopment")
    if isinstance(is_development, str):
        is_development = is_development.lower() == "true"

    # Ã¢â€â‚¬Ã¢â€â‚¬ operation Ã¢â€â‚¬Ã¢â€â‚¬
    transaction = prop.get("transactionType", "SALE")
    operation = transaction.lower() if transaction else "sale"

    # Ã¢â€â‚¬Ã¢â€â‚¬ propertytype: usar propertySubtype normalizado (Idealista usa "flat", no "homes") Ã¢â€â‚¬Ã¢â€â‚¬
    psubtype = prop.get("propertySubtype", "")
    if psubtype:
        mapped_ptype = psubtype.lower()  # "FLAT" Ã¢â€ â€™ "flat", "PENTHOUSE" Ã¢â€ â€™ "penthouse"
    else:
        ptype = prop.get("propertyType", "HOME")
        property_type_map = {"HOME": "homes", "OFFICE": "offices", "GARAGE": "garages", "LAND": "lands"}
        mapped_ptype = property_type_map.get(ptype, ptype.lower() if ptype else "homes")

    # Ã¢â€â‚¬Ã¢â€â‚¬ showaddress: STREET o STREET_NUMBER = true Ã¢â€â‚¬Ã¢â€â‚¬
    show_poi = prop.get("showPoi", "")
    show_address = show_poi in ("STREET", "STREET_NUMBER")

    # Ã¢â€â‚¬Ã¢â€â‚¬ suggestedtexts (Idealista-compatible: {title, subtitle}) Ã¢â€â‚¬Ã¢â€â‚¬
    fc_title = prop.get("title", "")
    fc_location = prop.get("locationDescription", "")
    suggested_texts = None
    if fc_title or fc_location:
        suggested_texts = {}
        if fc_title:
            suggested_texts["title"] = fc_title
        if fc_location:
            suggested_texts["subtitle"] = fc_location

    # Ã¢â€â‚¬Ã¢â€â‚¬ detailedtype normalizado a minÃƒÂºscula Ã¢â€â‚¬Ã¢â€â‚¬
    detailed_type = {"typology": psubtype.lower()} if psubtype else None

    # Ã¢â€â‚¬Ã¢â€â‚¬ fotocasa_specific: campos sin equivalente Idealista Ã¢â€â‚¬Ã¢â€â‚¬
    fotocasa_specific = {}
    if prop.get("listDate"):
        fotocasa_specific["listDate"] = prop["listDate"]
    if prop.get("diffPrice"):
        fotocasa_specific["diffPrice"] = prop["diffPrice"]
    if prop.get("orientation"):
        fotocasa_specific["orientation"] = prop["orientation"]
    if fc_title:
        fotocasa_specific["title"] = fc_title
    if is_professional is not None:
        fotocasa_specific["isProfessional"] = is_professional
    if prop.get("isProSellHouse") is not None:
        fotocasa_specific["isProSellHouse"] = prop["isProSellHouse"]
    if dynamic_feature_names:
        fotocasa_specific["dynamicFeatures"] = dynamic_feature_names
    if extra_list_str:
        fotocasa_specific["extraList"] = extra_list_str
    if prop.get("contracts"):
        fotocasa_specific["contracts"] = prop["contracts"]
    if prop.get("productList"):
        fotocasa_specific["productList"] = prop["productList"]
    if prop.get("promotionId") and prop["promotionId"] != "0":
        fotocasa_specific["promotionId"] = prop["promotionId"]
    if prop.get("hasSubsidies"):
        fotocasa_specific["hasSubsidies"] = prop["hasSubsidies"]
    if show_poi:
        fotocasa_specific["showPoi"] = show_poi
    if prop.get("dateDescription"):
        fotocasa_specific["dateDescription"] = prop["dateDescription"]
    if prop.get("priceDescription"):
        fotocasa_specific["priceDescription"] = prop["priceDescription"]

    result = {
        "propertycode": f"fc-{prop.get('propertyId', '')}",
        "thumbnail": prop.get("photo"),
        "externalreference": prop.get("id"),                         # "1_188618552" (ID interno FC)
        "numphotos": len(media_list) if media_list else None,
        "floor": floor_info.get("description") if floor_info else None,
        "price": prop.get("price"),
        "propertytype": mapped_ptype,                                # "flat" (no "homes")
        "operation": operation,
        "size": prop.get("surface"),
        "exterior": is_exterior if is_exterior else None,            # True si IS_EXTERIOR, null si no
        "rooms": prop.get("rooms"),
        "bathrooms": prop.get("bathrooms"),
        "address": prop.get("addressLine") or prop.get("locationDescription"),  # Calle primero, barrio fallback
        "province": prop.get("province") or segment.get("province"),
        "municipality": prop.get("city") or segment.get("city"),
        "district": segment.get("district"),
        "country": "es",                                             # Normalizado (no "EspaÃƒÂ±a")
        "neighborhood": segment.get("neighbourhood"),
        "locationid": None,
        "latitude": prop.get("latitude"),
        "longitude": prop.get("longitude"),
        "showaddress": show_address,                                 # STREET y STREET_NUMBER = true
        "url": prop.get("urlMarketplace"),
        "distance": None,
        "description": prop.get("comments"),
        "hasvideo": prop.get("hasVideo"),
        "status": None,
        "newdevelopment": is_development,                            # isDevelopment (stringÃ¢â€ â€™bool)
        "favourite": prop.get("favorite"),
        "newproperty": prop.get("propertyNew"),
        "haslift": has_lift if has_lift else None,                   # extraList "13"
        "pricebyarea": round(prop["price"] / prop["surface"], 2) if prop.get("price") and prop.get("surface") and prop["surface"] > 0 else None,
        "hasplan": prop.get("hasDescriptionPlan", False),            # hasDescriptionPlan
        "has3dtour": prop.get("hasTourVirtual"),
        "has360": None,
        "hasstaging": None,
        "preferencehighlight": None,
        "tophighlight": None,
        "topnewdevelopment": None,
        "newdevelopmenthighlight": None,
        "topplus": None,
        "urgentvisualhighlight": None,
        "visualhighlight": None,
        "priceinfo": {"price": {"amount": prop.get("price"), "currencySuffix": "Ã¢â€šÂ¬"}} if prop.get("price") else None,
        "contactinfo": contact_info if contact_info else None,
        "features": features if features else None,                  # Construido de extraList
        "detailedtype": detailed_type,                               # typology en minÃƒÂºscula
        "suggestedtexts": suggested_texts,                           # title + subtitle
        "multimedia": multimedia,
        "highlight": None,
        "parkingspace": parking_space,                               # extraList "5"
        "savedad": None,
        "labels": None,
        "ribbons": None,
        "notes": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": "fotocasa",
        "adisactive": True,
        "allowsremotevisit": prop.get("hasOnlineGuidedTour", False), # hasOnlineGuidedTour Ã¢â€ â€™ allowsremotevisit
        "fotocasa_specific": fotocasa_specific if fotocasa_specific else None,
        "sell_type": "new_build" if is_development is True else ("second_hand" if is_development is False else None),
        "pricedown": {"value": abs(_parse_fc_price(prop["diffPrice"]))} if _parse_fc_price(prop.get("diffPrice")) is not None else None,
        "originalprice": round(prop["price"] + abs(_parse_fc_price(prop["diffPrice"])), 2) if _parse_fc_price(prop.get("diffPrice")) is not None and prop.get("price") else None,
        "fc_hab_furnished": "furnished" if "19" in extra_codes else ("unfurnished" if "130" in extra_codes else None),
    }
    # occupation_status desde dynamicFeatures (solo si hay valor)
    _occ = _extract_occupation_from_fc_dynamic(prop.get("dynamicFeatures"))
    if _occ:
        result["occupation_status"] = _occ
    return result
    

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOTOCASA — Enrichment Transform (detail API → Supabase PATCH)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Mapeo de antigüedad Fotocasa → año de construcción estimado
# Los valores representan AÑOS DE ANTIGÜEDAD, no décadas.
# Fórmula: año_actual - punto_medio_del_rango
_FC_ANTIQUITY_MAP = {
    "FROM_0_TO_5": 2,
    "FROM_5_TO_10": 7,
    "FROM_10_TO_20": 15,
    "FROM_20_TO_30": 25,
    "FROM_30_TO_50": 40,
    "FROM_50_TO_70": 60,
    "FROM_70_TO_100": 85,
    "MORE_THAN_100": 120,
}

_CURRENT_YEAR = 2026  # Año base para cálculo de antigüedad


def transform_fotocasa_detail_to_enrichment(detail_data: dict, existing_fotocasa_specific: dict = None) -> dict:
    """
    Transforma la respuesta del detail API de Fotocasa
    a campos de enrichment para hacer PATCH en async_properties.
    Extrae: energycertification, status, year_built, allowscounteroffers,
    contactinfo enriquecido, pricedown, originalprice, y todo lo demás a fotocasa_specific.
    """
    enrichment = {}

    # --- Energy Certification (normalizado al formato Idealista) ---
    energy = detail_data.get("energyCertificate", {})
    if energy and energy.get("efficiencyRatingType"):
        # VALUE_E → "e", VALUE_A → "a"
        consumption_type = energy.get("efficiencyRatingType", "")
        emissions_type = energy.get("environmentImpactRatingType", "")

        def _parse_fc_energy_type(val):
            """VALUE_E → 'e', VALUE_A → 'a', IN_PROCESS → 'inProcess'"""
            if not val:
                return ""
            if val.startswith("VALUE_"):
                return val.replace("VALUE_", "").lower()
            if val == "IN_PROCESS":
                return "inProcess"
            return val.lower()

        enrichment["energycertification"] = {
            "title": "Certificado energético",
            "energyConsumption": {"type": _parse_fc_energy_type(consumption_type)} if consumption_type else {},
            "emissions": {"type": _parse_fc_energy_type(emissions_type)} if emissions_type else {},
        }

    # --- Status (buildingStatus → status normalizado a formato Idealista .lower()) ---
    building_status = detail_data.get("buildingStatus")
    if building_status:
        enrichment["status"] = building_status.lower()

    # --- Year built (features.antiquity → id_hab_year_built) ---
    fc_features = detail_data.get("features", {})
    antiquity = fc_features.get("antiquity") if isinstance(fc_features, dict) else None
    if antiquity and antiquity in _FC_ANTIQUITY_MAP:
        enrichment["id_hab_year_built"] = _CURRENT_YEAR - _FC_ANTIQUITY_MAP[antiquity]

    # --- allowscounteroffers (showCounterOffer → boolean) ---
    show_counter = detail_data.get("showCounterOffer")
    if show_counter is not None:
        if isinstance(show_counter, str):
            enrichment["allowscounteroffers"] = show_counter.lower() == "true"
        else:
            enrichment["allowscounteroffers"] = bool(show_counter)

    # --- originalprice (directo del detail) ---
    original_price = detail_data.get("originalPrice")
    current_price = detail_data.get("price")
    if original_price and current_price and original_price != current_price:
        enrichment["originalprice"] = original_price
        drop_value = abs(original_price - current_price)
        enrichment["pricedown"] = {"value": drop_value}

    # --- contactinfo enriquecido (merge con teléfono del detail) ---
    contact_phone = detail_data.get("contact")
    agency_data = detail_data.get("agency", {})
    if contact_phone or agency_data:
        contact_enriched = {}
        if contact_phone:
            contact_enriched["phone1"] = {"phoneNumber": contact_phone}
        if agency_data:
            contact_enriched["agencyName"] = agency_data.get("alias") or agency_data.get("name")
            contact_enriched["agencyLogo"] = agency_data.get("logo")
            if agency_data.get("type"):
                contact_enriched["userType"] = agency_data["type"]
        enrichment["contactinfo"] = contact_enriched

    # --- description (detail tiene descripción completa, placeholders solo comments) ---
    description = detail_data.get("description")
    if description:
        enrichment["description"] = description

    # --- fotocasa_specific: MERGE con existente — todo lo exclusivo del detail ---
    fc_specific = dict(existing_fotocasa_specific) if existing_fotocasa_specific else {}

    # Campos exclusivos del detail que no tienen columna propia
    detail_exclusive_fields = {
        "creationDate": (detail_data.get("segmentData") or {}).get("creationDate"),
        "antiquity": antiquity,  # Valor crudo como backup
        "heating": fc_features.get("heating") if isinstance(fc_features, dict) else None,
        "hotWater": fc_features.get("hotWater") if isinstance(fc_features, dict) else None,
        "buildingStatus": building_status,
        "originalPrice": original_price,
        "agencyReference": detail_data.get("agencyReference"),
        "extraListDescription": detail_data.get("extraListDescription"),
        "advertiserData": detail_data.get("advertiserData"),
        "showCounterOffer": detail_data.get("showCounterOffer"),
        "priceFeatures": detail_data.get("priceFeatures"),
        "rentReferenceIndex": detail_data.get("rentReferenceIndex"),
        "ratings": detail_data.get("ratings"),
        "videoList": detail_data.get("videoList"),
        "videoOnLineList": detail_data.get("videoOnLineList"),
        "multimediaList": detail_data.get("multimediaList"),
        "urlMarketplace": detail_data.get("urlMarketplace"),
        "locations": detail_data.get("locations"),
        "locationLevel1": detail_data.get("locationLevel1"),
        "locationLevel2": detail_data.get("locationLevel2"),
        "locationLevel3": detail_data.get("locationLevel3"),
        "locationLevel4": detail_data.get("locationLevel4"),
        "touristOfficeCode": detail_data.get("touristOfficeCode"),
        "geoAdvisorUrl": detail_data.get("geoAdvisorUrl"),
    }

    # Parsear postal_code de dataLayer
    data_layer = detail_data.get("dataLayer", "")
    if data_layer:
        for pair in data_layer.split("&"):
            if pair.startswith("postal_code="):
                pc = pair.split("=", 1)[1]
                if pc and pc != "null":
                    detail_exclusive_fields["postal_code"] = pc
                break

    # Merge: solo añadir campos con valor
    for k, v in detail_exclusive_fields.items():
        if v is not None and v != "" and v != []:
            fc_specific[k] = v

    enrichment["fotocasa_specific"] = fc_specific if fc_specific else None

    # --- Marcas de control ---
    enrichment["detail_enriched_at"] = datetime.now(timezone.utc).isoformat()
    enrichment["adisactive"] = True
    enrichment["updated_at"] = datetime.now(timezone.utc).isoformat()

    return enrichment


def _parse_habitaclia_pricedown(price_down_str):
    """Parsea PriceDown de Habitaclia (ej: '-15.000 €') a JSONB {value: 15000}."""
    if not price_down_str:
        return None
    try:
        cleaned = price_down_str.replace(".", "").replace("€", "").replace(" ", "").strip()
        value = abs(float(cleaned))
        return {"value": value} if value > 0 else None
    except (ValueError, TypeError):
        return None


def _calc_habitaclia_originalprice(price, price_down_str):
    """Calcula originalprice para Habitaclia: price + abs(PriceDown parseado)."""
    if not price or not price_down_str:
        return None
    parsed = _parse_habitaclia_pricedown(price_down_str)
    if parsed and parsed.get("value"):
        return round(price + parsed["value"], 2)
    return None


def transform_habitaclia_property(ad: dict) -> dict:
    """
    Transforma un Ad del formato Habitaclia (listings API) al esquema
    unificado de async_properties en Supabase.
    
    Campos disponibles en listing: Title, Location, Price, Features,
    SegmentData, Agency, CodProd, MainImage, flags.
    
    Campos NO disponibles en listing (requieren enrichment via detail API):
    address, province, municipality, district, neighborhood, latitude,
    longitude, description, features detalladas, energycertification.
    """

    # Ã¢â€â‚¬Ã¢â€â‚¬ Features: parsear array [{Type, Value}] Ã¢â€â‚¬Ã¢â€â‚¬
    features_list = ad.get("Features", [])
    size = None
    rooms = None
    bathrooms = None
    for f in features_list:
        ftype = f.get("Type", "")
        fval = f.get("Value")
        if not fval:
            continue
        if ftype == "surface":
            try:
                size = float(fval)
            except (ValueError, TypeError):
                pass
        elif ftype == "rooms":
            try:
                rooms = int(fval)
            except (ValueError, TypeError):
                pass
        elif ftype == "bathrooms":
            try:
                bathrooms = int(fval)
            except (ValueError, TypeError):
                pass

    # Ã¢â€â‚¬Ã¢â€â‚¬ SegmentData Ã¢â€â‚¬Ã¢â€â‚¬
    segment = ad.get("SegmentData", {})

    # propertytype: PropertySubType Ã¢â€ â€™ minÃƒÂºscula (FLATÃ¢â€ â€™flat, APARTMENTÃ¢â€ â€™apartment, HOUSEÃ¢â€ â€™house)
    psubtype = segment.get("PropertySubType", "")
    propertytype = psubtype.lower() if psubtype else "homes"

    # operation: Transaction Ã¢â€ â€™ minÃƒÂºscula (SALEÃ¢â€ â€™sale, RENTÃ¢â€ â€™rent)
    transaction = segment.get("Transaction", "SALE")
    operation = transaction.lower() if transaction else "sale"

    # sell_type: SellType (NEW_BUILDÃ¢â€ â€™new_build, SECOND_HANDÃ¢â€ â€™second_hand)
    sell_type_raw = segment.get("SellType", "")
    sell_type = None
    if sell_type_raw == "NEW_BUILD":
        sell_type = "new_build"
    elif sell_type_raw == "SECOND_HAND":
        sell_type = "second_hand"

    # Fallback: si ObraNueva=true pero SellType vacío, forzar new_build
    if sell_type is None and ad.get("ObraNueva") is True:
        sell_type = "new_build"

    # Ã¢â€â‚¬Ã¢â€â‚¬ Precio Ã¢â€â‚¬Ã¢â€â‚¬
    price = ad.get("Price")
    pricebyarea = round(price / size, 2) if price and size and size > 0 else None

    # Ã¢â€â‚¬Ã¢â€â‚¬ contactinfo: Agency + IsParticular Ã¢â€â‚¬Ã¢â€â‚¬
    agency = ad.get("Agency", {})
    contact_info = {}
    if agency.get("Phone"):
        contact_info["phone1"] = {"phoneNumber": agency["Phone"]}
    if agency.get("Name"):
        contact_info["agencyName"] = agency["Name"]
    if agency.get("LogoUrl"):
        contact_info["agencyLogo"] = agency["LogoUrl"]
    is_particular = segment.get("IsParticular", "")
    if is_particular:
        contact_info["userType"] = "professional" if is_particular == "PROFESSIONAL" else "private"

    # Ã¢â€â‚¬Ã¢â€â‚¬ multimedia: solo MainImage en listing Ã¢â€â‚¬Ã¢â€â‚¬
    main_image = ad.get("MainImage")
    multimedia = {"images": [{"url": main_image}]} if main_image else None

    # Ã¢â€â‚¬Ã¢â€â‚¬ suggestedtexts: {title, subtitle} Ã¢â€â‚¬Ã¢â€â‚¬
    title = ad.get("Title", "")
    location = ad.get("Location", "")
    suggested_texts = None
    if title or location:
        suggested_texts = {}
        if title:
            suggested_texts["title"] = title
        if location:
            suggested_texts["subtitle"] = location

    # Ã¢â€â‚¬Ã¢â€â‚¬ detailedtype Ã¢â€â‚¬Ã¢â€â‚¬
    detailed_type = {"typology": propertytype} if propertytype else None

    # Ã¢â€â‚¬Ã¢â€â‚¬ Flags booleanos Ã¢â€â‚¬Ã¢â€â‚¬
    has_video_3d = ad.get("HasVideo3D", False)
    has_video_360 = ad.get("HasVideo360", False)
    allows_remote = has_video_3d or has_video_360

    # Ã¢â€â‚¬Ã¢â€â‚¬ habitaclia_specific: campos sin equivalente en Idealista Ã¢â€â‚¬Ã¢â€â‚¬
    hab_specific = {}
    if ad.get("PriceDown"):
        hab_specific["PriceDown"] = ad["PriceDown"]
    if ad.get("Premium"):
        hab_specific["Premium"] = ad["Premium"]
    if ad.get("Prioritario"):
        hab_specific["Prioritario"] = ad["Prioritario"]
    if ad.get("UpdatedAd"):
        hab_specific["UpdatedAd"] = ad["UpdatedAd"]
    if sell_type:
        hab_specific["sellType"] = sell_type
    if ad.get("Oportunidad"):
        hab_specific["Oportunidad"] = ad["Oportunidad"]
    if ad.get("CodEmp") is not None:
        hab_specific["CodEmp"] = ad["CodEmp"]
    if ad.get("CodOfi") is not None:
        hab_specific["CodOfi"] = ad["CodOfi"]
    if ad.get("CodInm") is not None:
        hab_specific["CodInm"] = ad["CodInm"]

    # Fallback: si CodEmp/CodInm no vienen en el ad, extraerlos de la URL.
    # URL format: https://m.habitaclia.com/i{CodEmp}{zeros}{CodInm}
    # Formula inversa: total_digits = len(CodEmp) + 9
    if ("CodEmp" not in hab_specific or "CodInm" not in hab_specific) and ad.get("Url"):
        try:
            url_path = ad["Url"].rstrip("/").split("/")[-1]  # "i53116000000040"
            if url_path.startswith("i") and url_path[1:].isdigit():
                code = url_path[1:]  # "53116000000040"
                len_emp = len(code) - 9
                if len_emp > 0:
                    hab_specific["CodEmp"] = int(code[:len_emp])
                    hab_specific["CodInm"] = int(code[len_emp:])
                    logger.debug(
                        f"[hab-transform] Extracted CodEmp={hab_specific['CodEmp']}, "
                        f"CodInm={hab_specific['CodInm']} from URL for {ad.get('CodProd', '?')}"
                    )
        except (ValueError, IndexError):
            pass  # URL no parseable, la prop se insertará sin codes

    return {
        "propertycode": f"hab-{ad.get('CodProd', '')}",
        "thumbnail": main_image,
        "externalreference": None,
        "numphotos": ad.get("ImageCount"),
        "floor": None,
        "price": price,
        "propertytype": propertytype,
        "operation": operation,
        "size": size,
        "exterior": None,
        "rooms": rooms,
        "bathrooms": bathrooms,
        "address": None,
        "province": None,
        "municipality": None,
        "district": None,
        "country": "es",
        "neighborhood": None,
        "locationid": None,
        "latitude": None,
        "longitude": None,
        "showaddress": False,
        "url": ad.get("Url"),
        "distance": None,
        "description": None,
        "hasvideo": ad.get("HasVideo", False),
        "status": None,
        "newdevelopment": ad.get("ObraNueva", False),
        "favourite": ad.get("Favorite"),
        "newproperty": None,
        "haslift": None,
        "pricebyarea": pricebyarea,
        "hasplan": None,
        "has3dtour": has_video_3d,
        "has360": has_video_360,
        "hasstaging": None,
        "preferencehighlight": None,
        "tophighlight": None,
        "topnewdevelopment": None,
        "newdevelopmenthighlight": None,
        "topplus": None,
        "urgentvisualhighlight": None,
        "visualhighlight": None,
        "priceinfo": {"price": {"amount": price, "currencySuffix": "Ã¢â€šÂ¬"}} if price else None,
        "contactinfo": contact_info if contact_info else None,
        "features": None,
        "detailedtype": detailed_type,
        "suggestedtexts": suggested_texts,
        "multimedia": multimedia,
        "highlight": None,
        "parkingspace": None,
        "savedad": None,
        "labels": None,
        "ribbons": None,
        "notes": None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": "habitaclia",
        "adisactive": True,
        "allowsremotevisit": allows_remote,
        "sell_type": sell_type,
        "pricedown": _parse_habitaclia_pricedown(ad.get("PriceDown")),
        "originalprice": _calc_habitaclia_originalprice(price, ad.get("PriceDown")),
        "habitaclia_specific": hab_specific if hab_specific else None,
    }


def transform_habitaclia_detail_to_enrichment(detail: dict) -> dict:
    """
    Transforma la respuesta del detail API de Habitaclia
    a campos de enrichment para hacer PATCH en async_properties.
    Extrae: lat/lng, descripcion, ubicacion, features, energy cert, multimedia, contacto.
    """
    enrichment = {}

    # --- Coordenadas ---
    map_data = detail.get("Map", {})
    if map_data.get("VGPSLat"):
        enrichment["latitude"] = map_data["VGPSLat"]
    if map_data.get("VGPSLon"):
        enrichment["longitude"] = map_data["VGPSLon"]

    # --- Descripcion ---
    description = detail.get("Description", "")
    if description:
        description = re.sub(r'<br\s*/?>', '\n', description)
        description = re.sub(r'<[^>]+>', '', description)
        enrichment["description"] = description.strip()

    # --- Ubicacion desde DetailSegmentData (JSON string) ---
    segment_str = detail.get("DetailSegmentData", "")
    segment = {}
    if segment_str:
        try:
            segment = json.loads(segment_str) if isinstance(segment_str, str) else segment_str
        except (json.JSONDecodeError, TypeError):
            pass

    if segment.get("street"):
        enrichment["address"] = segment["street"]
        enrichment["showaddress"] = True
    if segment.get("region_level2"):
        enrichment["province"] = segment["region_level2"].lower()
    if segment.get("city"):
        enrichment["municipality"] = segment["city"]
    if segment.get("district"):
        enrichment["district"] = segment["district"]
    if segment.get("neighbourhood"):
        enrichment["neighborhood"] = segment["neighbourhood"]
    # --- PropertyType desde DetailSegmentData ---
    prop_sub = segment.get("property_sub", "")
    if prop_sub:
        enrichment["propertytype"] = prop_sub.lower()

    # --- Features desde DescriptionFeatures ---
    desc_features = detail.get("DescriptionFeatures", [])
    if desc_features:
        has_lift = None
        has_ac = None
        is_furnished = None
        year_built = None
        has_parking = None
        has_heating = None
        is_exterior = None

        for feat in desc_features:
            feat_lower = feat.lower() if isinstance(feat, str) else ""
            if "ascensor" in feat_lower:
                has_lift = True
            if "sin ascensor" in feat_lower:
                has_lift = False
            if "aire acondicionado" in feat_lower and "sin" not in feat_lower:
                has_ac = True
            if "sin aire acondicionado" in feat_lower:
                has_ac = False
            if "amueblado" in feat_lower and "sin" not in feat_lower:
                is_furnished = True
            if "sin amueblar" in feat_lower:
                is_furnished = False
            if "calefacción" in feat_lower or "calefaccion" in feat_lower:
                has_heating = True
            if "plaza parking" in feat_lower and "sin" not in feat_lower:
                has_parking = True
            if "sin plaza parking" in feat_lower:
                has_parking = False
            if "exterior" in feat_lower and "sin" not in feat_lower:
                is_exterior = True
            if "interior" in feat_lower:
                is_exterior = False
            if "año construcción" in feat_lower or "ano construccion" in feat_lower:
                year_match = re.search(r'(\d{4})', feat)
                if year_match:
                    year_built = int(year_match.group(1))

        if has_lift is not None:
            enrichment["haslift"] = has_lift
        if is_exterior is not None:
            enrichment["exterior"] = is_exterior
        if year_built is not None:
            enrichment["id_hab_year_built"] = year_built
        if has_parking is True:
            enrichment["parkingspace"] = {"hasParkingSpace": True}
        elif has_parking is False:
            enrichment["parkingspace"] = {"hasParkingSpace": False}

        features = {"description_features": desc_features}
        if has_ac is not None:
            features["hasAirConditioning"] = has_ac
        if is_furnished is not None:
            features["isFurnished"] = is_furnished
        if has_heating is not None:
            features["hasHeating"] = has_heating
        if has_parking is not None:
            features["hasParking"] = has_parking
        if year_built is not None:
            features["yearBuilt"] = year_built
        enrichment["features"] = features

        if is_furnished is True:
            enrichment["fc_hab_furnished"] = "furnished"
        elif is_furnished is False:
            enrichment["fc_hab_furnished"] = "unfurnished"

    # --- Energy Certification (normalizado al formato Idealista) ---
    energy = detail.get("EnergyEfficiencyCertificate", {})
    if energy and energy.get("Status") != "NOT_AVAILABLE":
        values = energy.get("Values") or {}
        consumption_label = (values.get("Consumption") or {}).get("Label", "")
        emissions_label = (values.get("Emissions") or {}).get("Label", "")
        if consumption_label or emissions_label:
            enrichment["energycertification"] = {
                "title": "Certificado energético",
                "energyConsumption": {"type": consumption_label.lower()} if consumption_label else {},
                "emissions": {"type": emissions_label.lower()} if emissions_label else {},
            }

    # --- Multimedia (imagenes completas) ---
    images = detail.get("Images", [])
    if images:
        multimedia_images = []
        for img in images:
            url_xl = img.get("URLXL", "")
            url_g = img.get("URLG", "")
            url_base = url_xl or url_g or img.get("URL", "")
            if url_base.startswith("//"):
                url_base = f"https:{url_base}"
            multimedia_images.append({
                "url": url_base,
                "tag": img.get("Titulo") or "image",
            })
        enrichment["multimedia"] = {"images": multimedia_images}
        enrichment["numphotos"] = len(multimedia_images)

        if multimedia_images:
            enrichment["thumbnail"] = multimedia_images[0]["url"]

    # --- Contact Info (Agency) ---
    agency = detail.get("Agency", {})
    if agency:
        enrichment["contactinfo"] = {
            "agencyName": agency.get("Name"),
            "phone1": agency.get("Phone"),
            "logoUrl": agency.get("LogoUrl"),
            "minisiteUrl": agency.get("MinisiteUrl"),
        }

    # --- Videos ---
    videos = detail.get("Videos", [])
    if videos:
        has_video = any(v.get("Type") != "360" for v in videos)
        has_360 = any(v.get("Type") == "360" for v in videos)
        enrichment["hasvideo"] = has_video
        enrichment["has360"] = has_360

    # --- Size/rooms/bathrooms (rellenar si listing no los tenia) ---
    for f in detail.get("Features", []):
        ftype = f.get("Type", "")
        fval = f.get("Value")
        if fval:
            if ftype == "surface":
                try:
                    enrichment["size"] = float(fval)
                except (ValueError, TypeError):
                    pass
            elif ftype == "rooms":
                try:
                    enrichment["rooms"] = int(fval)
                except (ValueError, TypeError):
                    pass
            elif ftype == "bathrooms":
                try:
                    enrichment["bathrooms"] = int(fval)
                except (ValueError, TypeError):
                    pass

    # --- Modification date (parseo relativo de UpdatedAd) ---
    updated_ad = detail.get("UpdatedAd", "")
    if updated_ad:
        now = datetime.now(timezone.utc)
        m = re.search(
            r'hace\s+(\d+)\s+(día|dias|días|hora|horas|minuto|minutos|mes|meses)',
            updated_ad.lower()
        )
        if m:
            amount = int(m.group(1))
            unit = m.group(2)
            if "día" in unit or "dia" in unit:
                enrichment["modification_date"] = (now - timedelta(days=amount)).isoformat()
            elif "hora" in unit:
                enrichment["modification_date"] = (now - timedelta(hours=amount)).isoformat()
            elif "minuto" in unit:
                enrichment["modification_date"] = (now - timedelta(minutes=amount)).isoformat()
            elif "mes" in unit:
                enrichment["modification_date"] = (now - timedelta(days=amount * 30)).isoformat()

    # --- Timestamps ---
    enrichment["detail_enriched_at"] = datetime.now(timezone.utc).isoformat()
    enrichment["updated_at"] = datetime.now(timezone.utc).isoformat()

    return enrichment


async def upsert_fotocasa_to_supabase(client: httpx.AsyncClient, properties: list, _new_codes_out: list = None):
    """
    Hace upsert de propiedades de Fotocasa en Supabase.
    Recibe propiedades en formato Fotocasa, las transforma y hace upsert.
    Devuelve (success, inserted, updated, error_message)
    Si _new_codes_out es una lista, acumula ahí los propertycodes nuevos.
    """
    if not properties:
        return True, 0, 0, None

    transformed = [transform_fotocasa_property(p) for p in properties]
    # Filtrar propiedades sin propertycode valido
    transformed = [p for p in transformed if p["propertycode"] and p["propertycode"] != "fc-"]
    # Deduplicar por propertycode (quedarse con la ÃƒÂºltima apariciÃƒÂ³n)
    seen = {}
    for p in transformed:
        seen[p["propertycode"]] = p
    transformed = list(seen.values())
    property_codes = [p["propertycode"] for p in transformed]

    if not property_codes:
        return True, 0, 0, None

    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }

    # 1. Consultar cuales ya existen
    try:
        query_url = f"{SUPABASE_URL}/async_properties?propertycode=in.({','.join(property_codes)})&select=propertycode"
        response = await client.get(query_url, headers=headers, timeout=30)

        if response.status_code == 200:
            existing = {item["propertycode"] for item in response.json()}
        else:
            logger.warning(f"Error checking existing Fotocasa properties: {response.status_code}")
            existing = set()
    except Exception as e:
        logger.warning(f"Exception checking existing Fotocasa properties: {e}")
        existing = set()

    # 2. Separar en nuevos vs existentes
    new_props = [p for p in transformed if p["propertycode"] not in existing]
    update_props = [p for p in transformed if p["propertycode"] in existing]

    inserted = 0
    updated = 0
    errors = []

    # 3. Insertar nuevos (batch)
    if new_props:
        try:
            response = await client.post(
                f"{SUPABASE_URL}/async_properties?on_conflict=propertycode",
                headers=headers,
                json=new_props,
                timeout=30
            )
            if response.status_code in [200, 201]:
                inserted = len(new_props)
                logger.info(f"[fotocasa] Inserted {inserted} new properties")
                # Acumular codes nuevos para webhook al final del ciclo
                if _new_codes_out is not None:
                    _new_codes_out.extend(p["propertycode"] for p in new_props)
            else:
                errors.append(f"Insert error: {response.status_code} - {response.text[:100]}")
        except Exception as e:
            errors.append(f"Insert exception: {str(e)}")

    # 4. Actualizar existentes (batch con upsert)
    if update_props:
        # Eliminar campos null para no sobreescribir datos enriched
        update_props_clean = [
            {k: v for k, v in p.items() if v is not None}
            for p in update_props
        ]
        try:
            headers_upsert = headers.copy()
            headers_upsert["Prefer"] = "resolution=merge-duplicates"

            response = await client.post(
                f"{SUPABASE_URL}/async_properties?on_conflict=propertycode",
                headers=headers_upsert,
                json=update_props_clean,
                timeout=30
            )
            if response.status_code in [200, 201]:
                updated = len(update_props)
                logger.info(f"[fotocasa] Updated {updated} existing properties")
            else:
                errors.append(f"Update error: {response.status_code} - {response.text[:100]}")
        except Exception as e:
            errors.append(f"Update exception: {str(e)}")

    error_message = "; ".join(errors) if errors else None
    return len(errors) == 0, inserted, updated, error_message

async def upsert_habitaclia_to_supabase(client: httpx.AsyncClient, properties: list, _new_codes_out: list = None):
    """
    Hace upsert de propiedades de Habitaclia en Supabase.
    Recibe Ads[] en formato Habitaclia, los transforma y hace upsert.
    Devuelve (success, inserted, updated, error_message, new_property_codes)
    new_property_codes: lista de propertycodes recien insertados (para enrichment post-insert)
    Si _new_codes_out es una lista, acumula ahí los propertycodes nuevos.
    """
    if not properties:
        return True, 0, 0, None, []

    transformed = [transform_habitaclia_property(ad) for ad in properties]
    # Filtrar propiedades sin propertycode vÃƒÂ¡lido
    transformed = [p for p in transformed if p["propertycode"] and p["propertycode"] != "hab-"]
    # Deduplicar por propertycode (quedarse con la ÃƒÂºltima apariciÃƒÂ³n)
    seen = {}
    for p in transformed:
        seen[p["propertycode"]] = p
    transformed = list(seen.values())
    property_codes = [p["propertycode"] for p in transformed]

    if not property_codes:
        return True, 0, 0, None, []

    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }

    # 1. Consultar cuÃƒÂ¡les ya existen (batches de 100 para evitar lÃƒÂ­mite de URL)
    existing = set()
    try:
        for i in range(0, len(property_codes), 100):
            batch_codes = property_codes[i:i + 100]
            query_url = (
                f"{SUPABASE_URL}/async_properties"
                f"?propertycode=in.({','.join(batch_codes)})"
                f"&select=propertycode"
            )
            response = await client.get(query_url, headers=headers, timeout=30)
            if response.status_code == 200:
                for item in response.json():
                    existing.add(item["propertycode"])
            else:
                logger.warning(f"[habitaclia] Error checking existing: {response.status_code}")
    except Exception as e:
        logger.warning(f"[habitaclia] Exception checking existing: {e}")

    # 2. Separar en nuevos vs existentes
    new_props = [p for p in transformed if p["propertycode"] not in existing]
    update_props = [p for p in transformed if p["propertycode"] in existing]

    inserted = 0
    updated = 0
    errors = []

    # 3. Insertar nuevos (batch)
    if new_props:
        try:
            response = await client.post(
                f"{SUPABASE_URL}/async_properties?on_conflict=propertycode",
                headers=headers,
                json=new_props,
                timeout=30,
            )
            if response.status_code in [200, 201]:
                inserted = len(new_props)
                logger.info(f"[habitaclia] Inserted {inserted} new properties")
                # Acumular codes nuevos para webhook al final del ciclo
                if _new_codes_out is not None:
                    _new_codes_out.extend(p["propertycode"] for p in new_props)
            else:
                errors.append(f"Insert error: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            errors.append(f"Insert exception: {str(e)}")

    # 4. Actualizar existentes — solo campos de "frescura" del listing
    #    El listing API de Habitaclia es muy pobre vs el detail API.
    #    Solo actualizamos precio, estado, y metadatos. NO tocamos
    #    campos que el enrichment llena mejor (multimedia, coords, etc.)
    HABITACLIA_UPDATE_FIELDS = {
        "propertycode",         # PK (requerido para upsert)
        "price",                # Precio fresco
        "pricebyarea",          # Derivado del precio
        "priceinfo",            # Info de precio
        "sell_type",            # Tipo de venta
        "operation",            # sale/rent
        "propertytype",         # Ahora viene bien del SegmentData
        "detailedtype",         # Derivado de propertytype
        "newdevelopment",       # Flag obra nueva
        "size",                 # Features del listing (rooms/size suelen coincidir)
        "rooms",
        "bathrooms",
        "url",                  # URL actualizada
        "suggestedtexts",       # Título y ubicación textual
        "habitaclia_specific",  # Metadatos de plataforma
        "pricedown",             # Bajada de precio (JSONB unificado)
        "originalprice",        # Precio original calculado
        "updated_at",           # Timestamp
        "source",               # Siempre "habitaclia"
        "adisactive",           # Implícitamente true (apareció en listing)
        "hasvideo",             # Flags del listing
        "has3dtour",
        "has360",
        "allowsremotevisit",
    }

    if update_props:
        update_props_clean = [
            {k: v for k, v in p.items()
             if k in HABITACLIA_UPDATE_FIELDS and v is not None
             and not (k == "propertytype" and v == "homes")}
            for p in update_props
        ]
        # Normalizar keys: PostgREST exige que todos los objetos tengan las mismas keys
        if update_props_clean:
            all_keys = set()
            for p in update_props_clean:
                all_keys.update(p.keys())
            update_props_clean = [
                {k: p.get(k) for k in all_keys}
                for p in update_props_clean
            ]
        try:
            headers_upsert = headers.copy()
            headers_upsert["Prefer"] = "resolution=merge-duplicates"
            response = await client.post(
                f"{SUPABASE_URL}/async_properties?on_conflict=propertycode",
                headers=headers_upsert,
                json=update_props_clean,
                timeout=30,
            )
            if response.status_code in [200, 201]:
                updated = len(update_props)
                logger.info(f"[habitaclia] Updated {updated} existing properties")
            else:
                errors.append(f"Update error: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            errors.append(f"Update exception: {str(e)}")

    error_message = "; ".join(errors) if errors else None
    new_property_codes = [p["propertycode"] for p in new_props] if inserted > 0 else []
    return len(errors) == 0, inserted, updated, error_message, new_property_codes

def generate_tree_csv(leaf_nodes: list) -> str:
    """Genera CSV (se mantiene como fallback / debug)."""
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "path", "name", "code", "level",
        "num_ads", "listing_slug", "url_navigation_api", "is_aggregate",
    ])
    for node in leaf_nodes:
        writer.writerow([
            node.get("path", ""),
            node.get("name", ""),
            node.get("code", ""),
            node.get("level", ""),
            node.get("num_ads", 0),
            node.get("listing_slug", ""),
            node.get("url_navigation_api", ""),
            node.get("is_aggregate", False),
        ])
    return output.getvalue()

async def upsert_tree_to_supabase(
    client: httpx.AsyncClient,
    leaf_nodes: list,
    location_code: str,
    operation: str,
    property_type: str,
) -> Tuple[bool, int, int, str]:
    """
    Upsert de leaf_nodes a la tabla habitaclia_tree en Supabase.
    Devuelve (success, inserted, updated, error_message).
    """
    if not leaf_nodes:
        return True, 0, 0, None

    now_iso = datetime.now(timezone.utc).isoformat()

    # Transformar leaf_nodes al esquema de la tabla
    rows = []
    for node in leaf_nodes:
        slug = node.get("listing_slug", "")
        if not slug:
            continue
        rows.append({
            "location_code": location_code,
            "operation": operation,
            "property_type": property_type,
            "path": node.get("path", ""),
            "name": node.get("name", ""),
            "code": node.get("code"),
            "level": node.get("level"),
            "num_ads": node.get("num_ads", 0),
            "listing_slug": slug,
            "url_navigation_api": node.get("url_navigation_api"),
            "is_aggregate": node.get("is_aggregate", False),
            "updated_at": now_iso,
        })

    if not rows:
        return True, 0, 0, None

    # Deduplicar por listing_slug (quedarse con la ÃƒÂºltima apariciÃƒÂ³n)
    seen = {}
    for r in rows:
        seen[r["listing_slug"]] = r
    rows = list(seen.values())

    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }

    # Consultar cuÃƒÂ¡les ya existen
    slugs = [r["listing_slug"] for r in rows]
    existing = set()
    try:
        # Supabase tiene lÃƒÂ­mite de URL, hacer en batches de 100
        for i in range(0, len(slugs), 100):
            batch_slugs = slugs[i:i+100]
            query_url = (
                f"{SUPABASE_URL}/habitaclia_tree"
                f"?location_code=eq.{location_code}"
                f"&operation=eq.{operation}"
                f"&property_type=eq.{property_type}"
                f"&listing_slug=in.({','.join(batch_slugs)})"
                f"&select=listing_slug"
            )
            resp = await client.get(query_url, headers=headers, timeout=30)
            if resp.status_code == 200:
                for item in resp.json():
                    existing.add(item["listing_slug"])
    except Exception as e:
        logger.warning(f"[habitaclia-tree] Error checking existing: {e}")

    new_rows = [r for r in rows if r["listing_slug"] not in existing]
    update_rows = [r for r in rows if r["listing_slug"] in existing]

    inserted = 0
    updated = 0
    errors = []

    # Insertar nuevos
    if new_rows:
        try:
            resp = await client.post(
                f"{SUPABASE_URL}/habitaclia_tree",
                headers=headers,
                json=new_rows,
                timeout=30,
            )
            if resp.status_code in [200, 201]:
                inserted = len(new_rows)
                logger.info(f"[habitaclia-tree] Inserted {inserted} new rows")
            else:
                errors.append(f"Insert error: {resp.status_code} - {resp.text[:200]}")
        except Exception as e:
            errors.append(f"Insert exception: {str(e)}")

    # Actualizar existentes (upsert merge-duplicates)
    if update_rows:
        try:
            headers_upsert = headers.copy()
            headers_upsert["Prefer"] = "resolution=merge-duplicates"
            resp = await client.post(
                f"{SUPABASE_URL}/habitaclia_tree",
                headers=headers_upsert,
                json=update_rows,
                timeout=30,
            )
            if resp.status_code in [200, 201]:
                updated = len(update_rows)
                logger.info(f"[habitaclia-tree] Updated {updated} existing rows")
            else:
                errors.append(f"Update error: {resp.status_code} - {resp.text[:200]}")
        except Exception as e:
            errors.append(f"Update exception: {str(e)}")

    error_msg = "; ".join(errors) if errors else None
    return len(errors) == 0, inserted, updated, error_msg


# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â
#  IDEALISTA Ã¢â‚¬â€ Enrichment (detail API)
# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â

def transform_detail_to_enrichment(detail_data: dict) -> dict:
    """Transforma la respuesta del detail API a los campos de enrichment para Supabase."""
    enrichment = {}

    # Campos de texto
    enrichment["hometype"] = detail_data.get("homeType")
    ubication = detail_data.get("ubication", {})
    if ubication:
        enrichment["locationname"] = ubication.get("locationName")
        # --- Geographic fields from ubication ---
        if ubication.get("administrativeAreaLevel1"):
            enrichment["province"] = ubication["administrativeAreaLevel1"]
        if ubication.get("administrativeAreaLevel2"):
            enrichment["municipality"] = ubication["administrativeAreaLevel2"]
        if ubication.get("administrativeAreaLevel3"):
            enrichment["district"] = ubication["administrativeAreaLevel3"]
        if ubication.get("administrativeAreaLevel4"):
            enrichment["neighborhood"] = ubication["administrativeAreaLevel4"]
        if ubication.get("latitude"):
            enrichment["latitude"] = ubication["latitude"]
        if ubication.get("longitude"):
            enrichment["longitude"] = ubication["longitude"]
        if ubication.get("locationId"):
            enrichment["locationid"] = ubication["locationId"]
        if ubication.get("title") and not ubication.get("hasHiddenAddress"):
            enrichment["address"] = ubication["title"]
            enrichment["showaddress"] = True

    # --- Operation & propertyType from top-level ---
    if detail_data.get("operation"):
        enrichment["operation"] = detail_data["operation"]
    # propertytype: Idealista detail devuelve propertyType="homes" (genérico)
    # en la mayoría de pisos — nunca usable. extendedPropertyType y homeType
    # sí dan el valor real ("flat", "chalet", "penthouse"...) y coinciden
    # cuando no son null. Guardas: no escribir si null, no escribir "homes"
    # (defensivo por si algún día lo devuelven ahí también).
    _prop_type = (
        detail_data.get("extendedPropertyType")
        or detail_data.get("homeType")
    )
    if _prop_type and _prop_type != "homes":
        enrichment["propertytype"] = _prop_type

    # Campos JSONB
    enrichment["morecharacteristics"] = detail_data.get("moreCharacteristics")
    enrichment["energycertification"] = detail_data.get("energyCertification")

    # Campos booleanos
    enrichment["has360vhs"] = detail_data.get("has360VHS", False)
    enrichment["allowscounteroffers"] = detail_data.get("allowsCounterOffers", False)
    enrichment["allowsremotevisit"] = detail_data.get("allowsRemoteVisit", False)
    enrichment["allowsprofilequalification"] = detail_data.get("allowsProfileQualification", False)

    # Fecha de modificacion (epoch ms -> ISO timestamp)
    mod_date = detail_data.get("modificationDate", {})
    if isinstance(mod_date, dict) and mod_date.get("value"):
        ts = mod_date["value"] / 1000
        enrichment["modification_date"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    # Actualizar contactinfo (version enriquecida del detail con address, proAgent, etc.)
    if detail_data.get("contactInfo"):
        enrichment["contactinfo"] = detail_data["contactInfo"]

    # Marcas de control
    enrichment["detail_enriched_at"] = datetime.now(timezone.utc).isoformat()
    enrichment["adisactive"] = True
    enrichment["updated_at"] = datetime.now(timezone.utc).isoformat()

    # Year built Ã¢â‚¬â€ buscar en moreCharacteristics o parsear de translatedTexts
    year_built = None
    more_chars = detail_data.get("moreCharacteristics", {})
    if isinstance(more_chars, dict) and more_chars.get("constructionYear"):
        year_built = int(more_chars["constructionYear"])
    else:
        translated = detail_data.get("translatedTexts", {})
        for section in translated.get("characteristicsDescriptions", []):
            for feature in section.get("detailFeatures", []):
                phrase = feature.get("phrase", "")
                if "Construido en" in phrase:
                    match = re.search(r"Construido en (\d{4})", phrase)
                    if match:
                        year_built = int(match.group(1))
                    break
    if year_built:
        enrichment["id_hab_year_built"] = year_built

    # ─── Campos que cambian durante la vida del anuncio ────────────────────
    # Hasta ahora solo se escribían en el scrape inicial (/scrape, /scrape-updates).
    # Esto dejaba precios y contadores stale en propiedades "viejas" (las que
    # no reaparecen en la primera página de publicationDate desc). Ahora los
    # 3 workers que usan esta función (check-active, recheck, enrich) los
    # refrescan en cada visita.
    #
    # Validado empíricamente con curls al detail API (17-abr-2026):
    # has3DTour, has360, newDevelopment NO existen en el detail top-level
    # (solo en el search). Por eso NO se escriben aquí — blanquearían la DB.

    # Price — mantiene la DB sincronizada con el precio público de Idealista
    current_price = detail_data.get("price")
    if current_price is not None:
        enrichment["price"] = current_price
        enrichment["priceinfo"] = {
            "price": {"amount": current_price, "currencySuffix": "€"}
        }
        # pricebyarea: computado desde constructedArea de moreCharacteristics
        _more_chars = detail_data.get("moreCharacteristics") or {}
        _area = _more_chars.get("constructedArea")
        if _area and _area > 0:
            enrichment["pricebyarea"] = round(current_price / _area, 2)

    # Multimedia counters — solo si multimedia tiene contenido real
    # (evita blanquear datos buenos en detalles transitorios null)
    _multimedia = detail_data.get("multimedia")
    if _multimedia:
        enrichment["numphotos"] = len(_multimedia.get("images") or [])
        enrichment["hasvideo"] = len(_multimedia.get("videos") or []) > 0
    # ─── END Campos que cambian ───────────────────────────────────────────

    # Price drop — priceDropInfo del detail API
    price_drop_info = detail_data.get("priceDropInfo")
    if price_drop_info and price_drop_info.get("priceDropValue"):
        drop_value = price_drop_info["priceDropValue"]
        pricedown_obj = {"value": abs(drop_value)}
        if price_drop_info.get("priceDropPercentage"):
            pricedown_obj["percentage"] = price_drop_info["priceDropPercentage"]
        if price_drop_info.get("dropDate"):
            drop_ts = price_drop_info["dropDate"] / 1000
            pricedown_obj["dropDate"] = datetime.fromtimestamp(drop_ts, tz=timezone.utc).isoformat()
        enrichment["pricedown"] = pricedown_obj

        # originalprice = precio actual + bajada
        current_price = detail_data.get("price")
        if current_price:
            enrichment["originalprice"] = round(current_price + abs(drop_value), 2)

    # occupation_status — intenta labels primero, luego translatedTexts
    _occ = _extract_occupation_from_id_labels(detail_data.get("labels"))
    if not _occ:
        _occ = _extract_occupation_from_id_translated_texts(detail_data)
    if _occ:
        enrichment["occupation_status"] = _occ

    return enrichment



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IDEALISTA — Slack alerts para workers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _send_slack_worker_alert(
    emoji: str, title: str, fields: dict, detail: str = None,
    category: str = "deactivation", is_alert: bool = False,
):
    """Envia alerta Slack como respuesta en hilo diario por categoria.

    Args:
        category: 'recheck' | 'deactivation' | 'mirror' — determina el hilo
        is_alert: True para errores/crashes — anade mencion a @pablo
    """
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        logger.warning("[slack] SLACK_BOT_TOKEN o SLACK_CHANNEL_ID no configurados")
        return

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    thread_info = _slack_threads[category]

    # --- Crear hilo padre si es el primero del dia ---
    if thread_info["date"] != today or thread_info["ts"] is None:
        parent_title = f"{_THREAD_TITLES[category]} — {today}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "https://slack.com/api/chat.postMessage",
                    headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
                    json={
                        "channel": SLACK_CHANNEL_ID,
                        "text": parent_title,
                        "blocks": [{
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": parent_title,
                                "emoji": True,
                            }
                        }],
                    },
                    timeout=10,
                )
                data = resp.json()
                if data.get("ok"):
                    thread_info["date"] = today
                    thread_info["ts"] = data["ts"]
                    logger.info(f"[slack] Hilo diario creado para {category}: {data['ts']}")
                else:
                    logger.error(f"[slack] Error creando hilo: {data.get('error')}")
                    return
        except Exception as e:
            logger.error(f"[slack] Excepcion creando hilo: {e}")
            return

    # --- Construir bloques del mensaje hijo ---
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{emoji} {title}", "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{k}:* {v}"}
                for k, v in fields.items()
            ]
        },
    ]

    if detail:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": detail}
        })

    # Mencion solo en CRASH (no en alertas normales de error rate)
    if is_alert and "CRASH" in title:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "<@U09AKUV02UE>"}
        })

    # --- Enviar como respuesta en el hilo ---
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
                json={
                    "channel": SLACK_CHANNEL_ID,
                    "thread_ts": thread_info["ts"],
                    "text": f"{emoji} {title}",
                    "blocks": blocks,
                },
                timeout=10,
            )
            data = resp.json()
            if not data.get("ok"):
                logger.error(f"[slack] Respuesta en hilo fallida: {data.get('error')}")
    except Exception as e:
        logger.error(f"[slack] Excepcion enviando a hilo: {e}")


async def _periodic_summary_loop():
    """Legacy — ya no envía resúmenes 6h. Datos disponibles via /workers-audit."""
    # Los resúmenes periódicos han sido reemplazados por /workers-audit.
    # Mantenemos la función para no romper la llamada en startup.
    while True:
        await asyncio.sleep(86400)  # Dormida eterna (1 día)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IDEALISTA — Funciones compartidas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _get_idealista_client(proxy_url=None):
    """Crea cliente Idealista con proxy y JWT valido.
    Si proxy_url no se pasa, usa PROXY_URL_PREMIUM (para JWT refresh, siempre fiable)."""
    if proxy_url is None:
        proxy_url = PROXY_URL_PREMIUM
    tls_impl = TlsClientImpl(proxy_url=proxy_url)
    idealista = Idealista(tls_impl)

    now_ts = time.time()
    if jwt_token_cache["token"] and jwt_token_cache["expires_at"] > now_ts + 60:
        idealista.jwt_token = jwt_token_cache["token"]
    else:
        success, result = await idealista.update_token()
        if not success:
            raise Exception(f"JWT token failed: {result}")
        jwt_token_cache["token"] = result["token"]
        parsed = parse_jwt_token(result["token"])
        if parsed:
            jwt_token_cache["expires_at"] = parsed.get("exp", 0)
        idealista.jwt_token = result["token"]

    return idealista, tls_impl, proxy_url


async def _refresh_jwt_if_needed(idealista):
    """Renueva JWT si esta por expirar (< 2 min)."""
    if jwt_token_cache["expires_at"] < time.time() + 120:
        success, result = await idealista.update_token()
        if success:
            jwt_token_cache["token"] = result["token"]
            parsed = parse_jwt_token(result["token"])
            if parsed:
                jwt_token_cache["expires_at"] = parsed.get("exp", 0)
            idealista.jwt_token = result["token"]


async def _verify_is_active(idealista, property_code, delay_between_retries=2.0):
    """
    Verifica si una propiedad sigue activa en Idealista.
    Retry 3x en 404/410 antes de confirmar inactiva.
    Returns: ("active", detail_data) | ("inactive", None) | ("error", {"status_code": N, "reason": str})
    """
    for attempt in range(3):
        try:
            success, response = await idealista.get_property_detail(property_code)
        except Exception as e:
            # Error de red/proxy (connection refused, 503, EOF, timeout, etc.)
            logger.warning(f"[verify-active] Network error for {property_code}: {e}")
            return "error", {"status_code": 0, "reason": "network"}

        if success and response:
            return "active", response

        error_status = 0
        if isinstance(response, dict):
            error_status = response.get("status_code", 0)

        if error_status in (404, 410):
            if attempt < 2:
                await asyncio.sleep(delay_between_retries)
                continue
            return "inactive", None
        else:
            # Log del código HTTP real para diagnosticar errores persistentes
            logger.warning(f"[verify-active] HTTP {error_status} for {property_code}")
            return "error", {"status_code": error_status, "reason": f"http_{error_status}"}

    return "inactive", None


def _supabase_headers(prefer="return=minimal"):
    """Headers para Supabase REST API (usado para UPDATEs individuales)."""
    h = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h

async def _get_check_active_batch(batch_size=50):
    """
    Obtiene el siguiente batch de propiedades activas a verificar.
    Usa asyncpg directo (sin PostgREST) para evitar timeouts.
    Prioriza por tier: capitales > provincias principales > resto.
    Returns: (properties_list, tier_name) o ([], None) si todo al dia.
    """
    now_utc = datetime.now(timezone.utc)

    for tier in CHECK_ACTIVE_TIERS:
        cutoff = now_utc - timedelta(hours=tier["cycle_hours"])

        # Query 1: propiedades nunca verificadas (NULL)
        query_null = f"""
            SELECT propertycode FROM async_properties
            WHERE source = 'idealista' AND adisactive = true
            AND {tier['where_clause']}
            AND last_checked_at IS NULL
            LIMIT {batch_size}
        """
        try:
            rows = await db_pool.fetch(query_null)
            if rows:
                return [{"propertycode": r["propertycode"]} for r in rows], tier["name"]
        except Exception as e:
            logger.warning(f"[check-active] SQL error (null) tier {tier['name']}: {e}")
            continue

        # Query 2: verificadas hace mas del ciclo
        query_old = f"""
            SELECT propertycode FROM async_properties
            WHERE source = 'idealista' AND adisactive = true
            AND {tier['where_clause']}
            AND last_checked_at < $1
            ORDER BY last_checked_at ASC
            LIMIT {batch_size}
        """
        try:
            rows = await db_pool.fetch(query_old, cutoff)
            if rows:
                return [{"propertycode": r["propertycode"]} for r in rows], tier["name"]
        except Exception as e:
            logger.warning(f"[check-active] SQL error (old) tier {tier['name']}: {e}")
            continue

    return [], None

async def _get_check_active_batch_for_tier(tier_index: int, batch_size=50):
    """
    Obtiene batch de propiedades Idealista a verificar para UN tier específico.
    Filtra propiedades ya en vuelo por otro worker para evitar trabajo duplicado.
    tier_index: 0=capitals, 1=main_provinces, 2=second_capitals, 3=rest
    Returns: (properties_list, tier_name) o ([], None) si el tier está al día.
    """
    if tier_index >= len(CHECK_ACTIVE_TIERS):
        return [], None

    tier = CHECK_ACTIVE_TIERS[tier_index]
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=tier["cycle_hours"])
    inflight = _id_ca_inflight
    # Over-fetch to compensate for in-flight filtering
    fetch_size = batch_size * 20

    # Query 1: propiedades nunca verificadas (NULL) — prioridad
    query_null = f"""
        SELECT propertycode FROM async_properties
        WHERE source = 'idealista' AND adisactive = true
        AND {tier['where_clause']}
        AND last_checked_at IS NULL
        LIMIT {fetch_size}
    """
    try:
        rows = await db_pool.fetch(query_null)
        if rows:
            filtered = [{"propertycode": r["propertycode"]} for r in rows
                        if r["propertycode"] not in inflight]
            if filtered:
                result = filtered[:batch_size]
                skipped = len(rows) - len(filtered)
                if skipped > 0:
                    logger.debug(f"[check-active] {tier['name']} null: {skipped} in-flight skipped")
                return result, tier["name"]
    except Exception as e:
        logger.warning(f"[check-active] SQL error (null) tier {tier['name']}: {e}")

    # Query 2: verificadas hace más del ciclo
    query_old = f"""
        SELECT propertycode FROM async_properties
        WHERE source = 'idealista' AND adisactive = true
        AND {tier['where_clause']}
        AND last_checked_at < $1
        ORDER BY last_checked_at ASC
        LIMIT {fetch_size}
    """
    try:
        rows = await db_pool.fetch(query_old, cutoff)
        if rows:
            filtered = [{"propertycode": r["propertycode"]} for r in rows
                        if r["propertycode"] not in inflight]
            if filtered:
                result = filtered[:batch_size]
                skipped = len(rows) - len(filtered)
                if skipped > 0:
                    logger.debug(f"[check-active] {tier['name']} old: {skipped} in-flight skipped")
                return result, tier["name"]
    except Exception as e:
        logger.warning(f"[check-active] SQL error (old) tier {tier['name']}: {e}")

    return [], None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CHECK ACTIVE — Proceso continuo (2 workers)
#  SELECTs via asyncpg, UPDATEs via httpx/REST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _check_active_worker(worker_id: int, tier_index: int):
    """
    Worker que verifica propiedades continuamente contra Idealista.
    Auto-reinicio si crashea. Para cuando check_active_state["running"] = False.
    """
    cfg = CHECK_ACTIVE_CONFIG
    logger.info(f"[check-active-w{worker_id}] Worker starting")

    # --- Proxy: regular con fallback a premium ---
    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
    _consecutive_proxy_errors = 0
    _fallback_at = 0
    logger.info(f"[check-active-w{worker_id}] Using {_proxy_label} proxy")

    while check_active_state["running"]:
        try:
            # --- Rebounce: si llevamos 1h en premium, probar regular ---
            if _proxy_label == "premium" and _fallback_at and PROXY_URL_REGULAR:
                if time.time() - _fallback_at > PROXY_REBOUNCE_SECONDS:
                    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
                    _consecutive_proxy_errors = 0
                    _fallback_at = 0
                    logger.info(f"[check-active-w{worker_id}] PROXY REBOUNCE: premium → regular")

            # JWT siempre via premium (fiable), luego recrear con proxy activo
            temp_idealista, _, _ = await _get_idealista_client()
            jwt = temp_idealista.jwt_token
            proxy_url = _active_proxy
            tls_impl = TlsClientImpl(proxy_url=proxy_url)
            idealista = Idealista(tls_impl)
            idealista.jwt_token = jwt
            headers = _supabase_headers()
            request_count = 0

            async with httpx.AsyncClient() as client:
                while check_active_state["running"]:
                    await _refresh_jwt_if_needed(idealista)

                    # Obtener batch via asyncpg (SQL directo, sin timeout)
                    batch, tier_name = await _get_check_active_batch_for_tier(tier_index, cfg["batch_size"])

                    if not batch:
                        # Work-stealing: intentar otros tiers antes de dormir
                        for other_idx in range(len(CHECK_ACTIVE_TIERS)):
                            if other_idx == tier_index:
                                continue
                            batch, tier_name = await _get_check_active_batch_for_tier(other_idx, cfg["batch_size"])
                            if batch:
                                logger.info(f"[check-active-w{worker_id}] Work-stealing: helping tier {tier_name}")
                                break
                        if not batch:
                            check_active_state["workers"][worker_id]["status"] = "idle"
                            check_active_state["workers"][worker_id]["_last_idle_at"] = time.time()
                            logger.info(f"[check-active-w{worker_id}] All tiers up to date, sleeping {cfg['sleep_when_idle']}s")
                            await asyncio.sleep(cfg["sleep_when_idle"])
                            continue

                    check_active_state["workers"][worker_id]["status"] = f"checking:{tier_name}"
                    check_active_state["workers"][worker_id]["current_tier"] = tier_name
                    # Accumulate idle time
                    _idle_at = check_active_state["workers"][worker_id].get("_last_idle_at")
                    if _idle_at:
                        check_active_state["workers"][worker_id]["idle_total_s"] = (
                            check_active_state["workers"][worker_id].get("idle_total_s", 0) + (time.time() - _idle_at)
                        )
                        check_active_state["workers"][worker_id]["_last_idle_at"] = None
                    batch_start = time.time()

                    # Track in-flight to prevent duplicate work
                    batch_codes = {p["propertycode"] for p in batch}
                    _id_ca_inflight.update(batch_codes)

                    to_deactivate = []
                    to_update_active = []
                    batch_errors = 0
                    batch_proxy_errors = 0

                    for prop in batch:
                        if not check_active_state["running"]:
                            break

                        property_code = prop["propertycode"]
                        await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))

                        # Rotar sesion TLS
                        request_count += 1
                        if request_count % cfg["session_rotate_every"] == 0:
                            tls_impl = TlsClientImpl(proxy_url=proxy_url)
                            idealista = Idealista(tls_impl)
                            idealista.jwt_token = jwt_token_cache["token"]

                        status, detail_data = await _verify_is_active(idealista, property_code)

                        if status == "active":
                            enrichment = transform_detail_to_enrichment(detail_data)
                            enrichment["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                            to_update_active.append((property_code, enrichment))
                        elif status == "inactive":
                            to_deactivate.append(property_code)
                        else:
                            batch_errors += 1
                            # Si 407 (proxy auth fail), rotar sesion TLS inmediatamente
                            err_code = detail_data.get("status_code", 0) if isinstance(detail_data, dict) else 0
                            _audit_err("id-check-active", f"HTTP {err_code}")
                            # Solo contar HTTP 0/407 como errores de proxy (no 401 JWT, etc)
                            if err_code in (0, 407):
                                batch_proxy_errors += 1
                            if err_code == 407:
                                tls_impl = TlsClientImpl(proxy_url=proxy_url)
                                idealista = Idealista(tls_impl)
                                idealista.jwt_token = jwt_token_cache["token"]
                                request_count = 0
                            elif err_code == 401:
                                # JWT expirado/revocado — forzar expiración y refrescar
                                logger.info(f"[check-active-w{worker_id}] 401 → refreshing JWT")
                                jwt_token_cache["expires_at"] = 0  # Forzar refresh
                                await _refresh_jwt_if_needed(idealista)
                                idealista.jwt_token = jwt_token_cache["token"]

                    # --- UPDATEs via REST (funciona bien para single-row) ---

                    for code, enrichment in to_update_active:
                        try:
                            url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{code}"
                            await client.patch(url, headers=headers, json=enrichment, timeout=30)
                        except Exception as e:
                            logger.warning(f"[check-active-w{worker_id}] DB error {code}: {e}")
                            batch_errors += 1
                            _audit_err("id-check-active", "DB error", str(e)[:80])

                    if to_deactivate:
                        deactivate_data = {
                            "adisactive": False,
                            "recheck_count": 0,
                            "last_checked_at": datetime.now(timezone.utc).isoformat(),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }
                        codes_csv = ",".join(to_deactivate)
                        url = f"{SUPABASE_URL}/async_properties?propertycode=in.({codes_csv})"
                        try:
                            await client.patch(url, headers=headers, json=deactivate_data, timeout=30)
                        except Exception as e:
                            logger.error(f"[check-active-w{worker_id}] Batch deactivate error: {e}")

                    # --- Avanzar last_checked_at de propiedades con error ---
                    # Evita que las mismas propiedades bloqueen la cola en bucle
                    error_codes = [
                        p["propertycode"] for p in batch
                        if p["propertycode"] not in [c for c, _ in to_update_active]
                        and p["propertycode"] not in to_deactivate
                    ]
                    if error_codes:
                        try:
                            error_update = {
                                "last_checked_at": datetime.now(timezone.utc).isoformat(),
                            }
                            codes_csv = ",".join(error_codes)
                            url = f"{SUPABASE_URL}/async_properties?propertycode=in.({codes_csv})"
                            await client.patch(url, headers=headers, json=error_update, timeout=30)
                        except Exception as e:
                            logger.warning(f"[check-active-w{worker_id}] Error updating error timestamps: {e}")

                    # Contadores
                    num_active = len(to_update_active)
                    num_deactivated = len(to_deactivate)
                    batch_checked = num_active + num_deactivated + batch_errors
                    check_active_state["totals"]["checked"] += batch_checked
                    check_active_state["totals"]["still_active"] += num_active
                    check_active_state["totals"]["deactivated"] += num_deactivated
                    check_active_state["totals"]["errors"] += batch_errors
                    # Per-tier
                    bt = check_active_state["totals"]["by_tier"]
                    if tier_name not in bt:
                        bt[tier_name] = {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0}
                    bt[tier_name]["checked"] += batch_checked
                    bt[tier_name]["still_active"] += num_active
                    bt[tier_name]["deactivated"] += num_deactivated
                    bt[tier_name]["errors"] += batch_errors
                    # Per-worker
                    w_state = check_active_state["workers"][worker_id]
                    w_state["checked"] = w_state.get("checked", 0) + batch_checked
                    w_state["errors"] = w_state.get("errors", 0) + batch_errors
                    w_state["batches_done"] = w_state.get("batches_done", 0) + 1
                    w_state["last_batch_at"] = datetime.now(timezone.utc).isoformat()

                    # Release in-flight codes
                    _id_ca_inflight.difference_update(batch_codes)

                    batch_time = time.time() - batch_start
                    logger.info(
                        f"[check-active-w{worker_id}] Batch ({tier_name}): "
                        f"{num_active}A {num_deactivated}D {batch_errors}E "
                        f"({batch_time:.0f}s) | Total: {check_active_state['totals']['checked']}"
                    )

                    # Alerta Slack si error rate alto
                    if batch_errors > len(batch) * 0.5 and len(batch) >= 30:
                        logger.warning(f"[check-active-w{worker_id}] High error rate, sleeping {cfg['error_sleep']}s")
                        await _send_slack_worker_alert(
                            emoji="\u26a0\ufe0f",
                            title=f"[IDEALISTA] Check Active w{worker_id} — Error rate alto",
                            fields={
                                "Worker": f"w{worker_id}",
                                "Tier": tier_name,
                                "Batch": f"{num_active}A {num_deactivated}D {batch_errors}E / {len(batch)} total",
                                "Error rate": f"{batch_errors/len(batch)*100:.0f}%",
                            },
                            category="deactivation",
                            is_alert=True,
                        )
                        await asyncio.sleep(cfg["error_sleep"])

                    # --- Proxy fallback: si regular tiene muchos errores de PROXY, cambiar a premium ---
                    if _proxy_label == "regular" and len(batch) >= 10:
                        if batch_proxy_errors > len(batch) * 0.3:
                            _consecutive_proxy_errors += 1
                            if _consecutive_proxy_errors >= 2:
                                _active_proxy, _proxy_label = _proxy_select(prefer_regular=False)
                                _consecutive_proxy_errors = 0
                                _fallback_at = time.time()
                                check_active_state["workers"][worker_id]["proxy"] = _proxy_label
                                logger.warning(f"[check-active-w{worker_id}] PROXY FALLBACK: regular → premium")
                                await _send_slack_worker_alert(
                                    "\U0001f504", f"[IDEALISTA] Check Active w{worker_id} — Proxy fallback a PREMIUM",
                                    {"Motivo": "2 batches >30% errors", "Proxy": "premium"},
                                    category="deactivation", is_alert=True,
                                )
                                break  # Recrear client con premium
                        else:
                            _consecutive_proxy_errors = 0

        except Exception as e:
            # Release any in-flight codes on crash
            try:
                _id_ca_inflight.difference_update(batch_codes)
            except Exception:
                pass
            logger.error(f"[check-active-w{worker_id}] Worker crashed: {e}", exc_info=True)
            check_active_state["workers"][worker_id]["status"] = "crashed"
            _audit_err("id-check-active", "CRASH", str(e)[:120])
            await _send_slack_worker_alert(
                emoji="\U0001f6a8",
                title=f"[IDEALISTA] Check Active w{worker_id} — CRASH",
                fields={
                    "Worker": f"w{worker_id}",
                    "Error": str(e)[:200],
                    "Accion": f"Reiniciando en {cfg['restart_delay']}s",
                    "Total checked": str(check_active_state["totals"]["checked"]),
                },
                category="deactivation",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info(f"[check-active-w{worker_id}] Restarting worker...")

    logger.info(f"[check-active-w{worker_id}] Worker stopped")


async def _start_check_active():
    """Lanza los workers de Check Active — dedicados por tier."""
    check_active_state["running"] = True
    check_active_state["started_at"] = datetime.now(timezone.utc).isoformat()
    check_active_state["totals"] = {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0, "by_tier": {}}
    check_active_state["last_summary_at"] = time.time()

    workers_per_tier = CHECK_ACTIVE_CONFIG["workers_per_tier"]
    worker_id = 0
    for tier_index, tier in enumerate(CHECK_ACTIVE_TIERS):
        num_workers = workers_per_tier[tier_index] if tier_index < len(workers_per_tier) else 1
        for w in range(num_workers):
            check_active_state["workers"][worker_id] = {
                "status": "starting", "current_tier": tier["name"],
                "batches_done": 0, "last_batch_at": None,
                "checked": 0, "errors": 0,
                "idle_total_s": 0, "_last_idle_at": None,
            }
            asyncio.create_task(_check_active_worker(worker_id, tier_index))
            worker_id += 1

    total_workers = sum(workers_per_tier)
    logger.info(f"[check-active] Started {total_workers} workers (per tier: {workers_per_tier})")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HABITACLIA CHECK ACTIVE — Funciones core
#  Sin JWT/TLS/proxy — usa httpx directo con User-Agent Android
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _hab_verify_is_active(hab_client, cod_emp: int, cod_inm: int, delay_between_retries=1.0):
    """
    Verifica si una propiedad sigue activa en Habitaclia.
    Retry 1x en 404/410 antes de confirmar inactiva.
    HAB false-inactive rate is ~0.16% — 2 attempts are sufficient.
    Returns: ("active", detail_data) | ("inactive", None) | ("error", error_info)
    """
    for attempt in range(2):
        try:
            success, response = await hab_client.get_property_detail(cod_emp, cod_inm)

            if success and response:
                return "active", response

            error_status = 0
            error_detail = ""
            if isinstance(response, dict):
                error_status = response.get("status_code", 0)
                error_detail = str(response.get("error", ""))[:120]

            if error_status in (404, 410):
                if attempt < 1:
                    await asyncio.sleep(delay_between_retries)
                    continue
                return "inactive", None
            else:
                return "error", f"HTTP {error_status}: {error_detail}" if error_status else f"unknown: {error_detail}"

        except Exception as e:
            logger.warning(f"[hab-check-active] verify error cod_emp={cod_emp} cod_inm={cod_inm}: {e}")
            if attempt < 1:
                await asyncio.sleep(delay_between_retries)
                continue
            return "error", f"exception: {str(e)[:120]}"

    return "inactive", None


async def _get_hab_check_active_batch(batch_size=100):
    """
    Obtiene el siguiente batch de propiedades Habitaclia activas a verificar.
    Usa asyncpg directo. Extrae CodEmp/CodInm del JSONB habitaclia_specific.
    Prioriza por tier: capitales > provincias principales > resto.
    Returns: (properties_list, tier_name) o ([], None) si todo al día.
    """
    now_utc = datetime.now(timezone.utc)

    for tier in HAB_CHECK_ACTIVE_TIERS:
        cutoff = now_utc - timedelta(hours=tier["cycle_hours"])

        # Query 1: propiedades nunca verificadas (NULL)
        query_null = f"""
            SELECT propertycode,
                   (habitaclia_specific->>'CodEmp')::int AS cod_emp,
                   (habitaclia_specific->>'CodInm')::int AS cod_inm
            FROM async_properties
            WHERE source = 'habitaclia' AND adisactive = true
            AND habitaclia_specific->>'CodEmp' IS NOT NULL
            AND habitaclia_specific->>'CodInm' IS NOT NULL
            AND {tier['where_clause']}
            AND last_checked_at IS NULL
            LIMIT {batch_size}
        """
        try:
            rows = await db_pool.fetch(query_null)
            if rows:
                return [
                    {"propertycode": r["propertycode"], "cod_emp": r["cod_emp"], "cod_inm": r["cod_inm"]}
                    for r in rows
                ], tier["name"]
        except Exception as e:
            logger.warning(f"[hab-check-active] SQL error (null) tier {tier['name']}: {e}")
            continue

        # Query 2: verificadas hace más del ciclo
        query_old = f"""
            SELECT propertycode,
                   (habitaclia_specific->>'CodEmp')::int AS cod_emp,
                   (habitaclia_specific->>'CodInm')::int AS cod_inm
            FROM async_properties
            WHERE source = 'habitaclia' AND adisactive = true
            AND habitaclia_specific->>'CodEmp' IS NOT NULL
            AND habitaclia_specific->>'CodInm' IS NOT NULL
            AND {tier['where_clause']}
            AND last_checked_at < $1
            ORDER BY last_checked_at ASC
            LIMIT {batch_size}
        """
        try:
            rows = await db_pool.fetch(query_old, cutoff)
            if rows:
                return [
                    {"propertycode": r["propertycode"], "cod_emp": r["cod_emp"], "cod_inm": r["cod_inm"]}
                    for r in rows
                ], tier["name"]
        except Exception as e:
            logger.warning(f"[hab-check-active] SQL error (old) tier {tier['name']}: {e}")
            continue

    return [], None


async def _get_hab_check_active_batch_for_tier(tier_index: int, batch_size=100):
    """
    Obtiene el siguiente batch de propiedades Habitaclia activas a verificar
    para un tier ESPECÍFICO. Filtra propiedades ya en vuelo.
    """
    if tier_index >= len(HAB_CHECK_ACTIVE_TIERS):
        return [], None

    tier = HAB_CHECK_ACTIVE_TIERS[tier_index]
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=tier["cycle_hours"])
    inflight = _hab_ca_inflight
    fetch_size = batch_size * 20

    # Query 1: propiedades nunca verificadas (NULL)
    query_null = f"""
        SELECT propertycode,
               (habitaclia_specific->>'CodEmp')::int AS cod_emp,
               (habitaclia_specific->>'CodInm')::int AS cod_inm
        FROM async_properties
        WHERE source = 'habitaclia' AND adisactive = true
        AND habitaclia_specific->>'CodEmp' IS NOT NULL
        AND habitaclia_specific->>'CodInm' IS NOT NULL
        AND {tier['where_clause']}
        AND last_checked_at IS NULL
        LIMIT {fetch_size}
    """
    try:
        rows = await db_pool.fetch(query_null)
        if rows:
            filtered = [
                {"propertycode": r["propertycode"], "cod_emp": r["cod_emp"], "cod_inm": r["cod_inm"]}
                for r in rows if r["propertycode"] not in inflight
            ]
            if filtered:
                return filtered[:batch_size], tier["name"]
    except Exception as e:
        logger.warning(f"[hab-check-active] SQL error (null) tier {tier['name']}: {e}")

    # Query 2: verificadas hace más del ciclo
    query_old = f"""
        SELECT propertycode,
               (habitaclia_specific->>'CodEmp')::int AS cod_emp,
               (habitaclia_specific->>'CodInm')::int AS cod_inm
        FROM async_properties
        WHERE source = 'habitaclia' AND adisactive = true
        AND habitaclia_specific->>'CodEmp' IS NOT NULL
        AND habitaclia_specific->>'CodInm' IS NOT NULL
        AND {tier['where_clause']}
        AND last_checked_at < $1
        ORDER BY last_checked_at ASC
        LIMIT {fetch_size}
    """
    try:
        rows = await db_pool.fetch(query_old, cutoff)
        if rows:
            filtered = [
                {"propertycode": r["propertycode"], "cod_emp": r["cod_emp"], "cod_inm": r["cod_inm"]}
                for r in rows if r["propertycode"] not in inflight
            ]
            if filtered:
                return filtered[:batch_size], tier["name"]
    except Exception as e:
        logger.warning(f"[hab-check-active] SQL error (old) tier {tier['name']}: {e}")

    return [], None

    return [], None


async def _hab_check_active_worker(worker_id: int, tier_index: int):
    """
    Worker que verifica propiedades Habitaclia continuamente para UN tier específico.
    Sin JWT/TLS/proxy — usa HttpxClient directo.
    Auto-reinicio si crashea. Para cuando hab_check_active_state["running"] = False.
    """
    cfg = HAB_CHECK_ACTIVE_CONFIG
    tier_name = HAB_CHECK_ACTIVE_TIERS[tier_index]["name"]
    logger.info(f"[hab-check-active-w{worker_id}] Worker starting (tier: {tier_name})")

    while hab_check_active_state["running"]:
        try:
            headers_rest = _supabase_headers()

            async with httpx.AsyncClient(timeout=30) as client:
                httpx_client = HttpxClient(client)
                hab = Habitaclia(httpx_client)

                while hab_check_active_state["running"]:
                    # Obtener batch via asyncpg — primero tier asignado
                    batch, current_tier = await _get_hab_check_active_batch_for_tier(tier_index, cfg["batch_size"])

                    if not batch:
                        # Work-stealing: intentar otros tiers antes de dormir
                        for other_idx in range(len(HAB_CHECK_ACTIVE_TIERS)):
                            if other_idx == tier_index:
                                continue
                            batch, current_tier = await _get_hab_check_active_batch_for_tier(other_idx, cfg["batch_size"])
                            if batch:
                                logger.info(f"[hab-check-active-w{worker_id}] Work-stealing: helping tier {current_tier}")
                                break
                        if not batch:
                            hab_check_active_state["workers"][worker_id]["status"] = "idle"
                            hab_check_active_state["workers"][worker_id]["_last_idle_at"] = time.time()
                            logger.info(f"[hab-check-active-w{worker_id}] All tiers up to date, sleeping {cfg['sleep_when_idle']}s")
                            await asyncio.sleep(cfg["sleep_when_idle"])
                            continue

                    # Usar current_tier (puede ser el propio o uno robado)
                    working_tier = current_tier or tier_name
                    hab_check_active_state["workers"][worker_id]["status"] = f"checking:{working_tier}"
                    hab_check_active_state["workers"][worker_id]["current_tier"] = working_tier
                    # Accumulate idle time
                    _idle_at = hab_check_active_state["workers"][worker_id].get("_last_idle_at")
                    if _idle_at:
                        hab_check_active_state["workers"][worker_id]["idle_total_s"] = (
                            hab_check_active_state["workers"][worker_id].get("idle_total_s", 0) + (time.time() - _idle_at)
                        )
                        hab_check_active_state["workers"][worker_id]["_last_idle_at"] = None
                    batch_start = time.time()

                    # Track in-flight to prevent duplicate work
                    batch_codes = {p["propertycode"] for p in batch}
                    _hab_ca_inflight.update(batch_codes)

                    to_deactivate = []
                    to_update_active = []
                    batch_errors = 0

                    # Concurrent checks con semaphore
                    sem = asyncio.Semaphore(cfg.get("concurrent_checks", 15))

                    async def _check_one(prop):
                        """Verifica una propiedad con concurrency limitada."""
                        async with sem:
                            if not hab_check_active_state["running"]:
                                return None
                            property_code = prop["propertycode"]
                            cod_emp = prop["cod_emp"]
                            cod_inm = prop["cod_inm"]
                            await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))
                            status, detail_data = await _hab_verify_is_active(hab, cod_emp, cod_inm)
                            return (property_code, status, detail_data)

                    results = await asyncio.gather(
                        *[_check_one(p) for p in batch],
                        return_exceptions=True,
                    )

                    for r in results:
                        if r is None:
                            continue
                        if isinstance(r, Exception):
                            logger.warning(f"[hab-check-active-w{worker_id}] Concurrent check error: {r}")
                            batch_errors += 1
                            _audit_err("hab-check-active", "exception", str(r)[:80])
                            continue
                        property_code, status, detail_data = r
                        if status == "active":
                            enrichment = transform_habitaclia_detail_to_enrichment(detail_data)
                            enrichment["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                            enrichment["adisactive"] = True
                            to_update_active.append((property_code, enrichment))
                        elif status == "inactive":
                            to_deactivate.append(property_code)
                        else:
                            batch_errors += 1
                            _audit_err("hab-check-active", f"HTTP error", str(detail_data)[:120] if detail_data else None)

                    # --- UPDATEs via REST (single-row PATCHes) ---

                    for code, enrichment in to_update_active:
                        try:
                            url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{code}"
                            await client.patch(url, headers=headers_rest, json=enrichment, timeout=30)
                        except Exception as e:
                            logger.warning(f"[hab-check-active-w{worker_id}] DB error {code}: {e}")
                            batch_errors += 1
                            _audit_err("hab-check-active", "DB error", str(e)[:80])

                    if to_deactivate:
                        deactivate_data = {
                            "adisactive": False,
                            "recheck_count": 0,
                            "last_checked_at": datetime.now(timezone.utc).isoformat(),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }
                        codes_csv = ",".join(to_deactivate)
                        url = f"{SUPABASE_URL}/async_properties?propertycode=in.({codes_csv})"
                        try:
                            await client.patch(url, headers=headers_rest, json=deactivate_data, timeout=30)
                        except Exception as e:
                            logger.error(f"[hab-check-active-w{worker_id}] Batch deactivate error: {e}")

                    # --- Avanzar last_checked_at de propiedades con error ---
                    # Evita que las mismas propiedades bloqueen la cola en bucle
                    error_codes = [
                        p["propertycode"] for p in batch
                        if p["propertycode"] not in [c for c, _ in to_update_active]
                        and p["propertycode"] not in to_deactivate
                    ]
                    if error_codes:
                        try:
                            error_update = {
                                "last_checked_at": datetime.now(timezone.utc).isoformat(),
                            }
                            codes_csv = ",".join(error_codes)
                            url = f"{SUPABASE_URL}/async_properties?propertycode=in.({codes_csv})"
                            await client.patch(url, headers=headers_rest, json=error_update, timeout=30)
                        except Exception as e:
                            logger.warning(f"[hab-check-active-w{worker_id}] Error updating error timestamps: {e}")

                    # Contadores
                    num_active = len(to_update_active)
                    num_deactivated = len(to_deactivate)
                    batch_checked = num_active + num_deactivated + batch_errors
                    hab_check_active_state["totals"]["checked"] += batch_checked
                    hab_check_active_state["totals"]["still_active"] += num_active
                    hab_check_active_state["totals"]["deactivated"] += num_deactivated
                    hab_check_active_state["totals"]["errors"] += batch_errors
                    # Per-tier
                    bt = hab_check_active_state["totals"]["by_tier"]
                    if working_tier not in bt:
                        bt[working_tier] = {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0}
                    bt[working_tier]["checked"] += batch_checked
                    bt[working_tier]["still_active"] += num_active
                    bt[working_tier]["deactivated"] += num_deactivated
                    bt[working_tier]["errors"] += batch_errors
                    # Per-worker
                    w_state = hab_check_active_state["workers"][worker_id]
                    w_state["checked"] = w_state.get("checked", 0) + batch_checked
                    w_state["errors"] = w_state.get("errors", 0) + batch_errors
                    w_state["batches_done"] = w_state.get("batches_done", 0) + 1
                    w_state["last_batch_at"] = datetime.now(timezone.utc).isoformat()

                    # Release in-flight codes
                    _hab_ca_inflight.difference_update(batch_codes)

                    batch_time = time.time() - batch_start
                    logger.info(
                        f"[hab-check-active-w{worker_id}] Batch ({working_tier}): "
                        f"{num_active}A {num_deactivated}D {batch_errors}E "
                        f"({batch_time:.0f}s) | Total: {hab_check_active_state['totals']['checked']}"
                    )

                    # Alerta Slack si error rate alto
                    if batch_errors > len(batch) * 0.5 and len(batch) >= 30:
                        logger.warning(f"[hab-check-active-w{worker_id}] High error rate, sleeping {cfg['error_sleep']}s")
                        await _send_slack_worker_alert(
                            emoji="⚠️",
                            title=f"[HABITACLIA] Check Active w{worker_id} — Error rate alto",
                            fields={
                                "Worker": f"hab-w{worker_id}",
                                "Tier": working_tier,
                                "Batch": f"{num_active}A {num_deactivated}D {batch_errors}E / {len(batch)} total",
                                "Error rate": f"{batch_errors/len(batch)*100:.0f}%",
                            },
                            category="deactivation",
                            is_alert=True,
                        )
                        await asyncio.sleep(cfg["error_sleep"])

        except Exception as e:
            # Release any in-flight codes on crash
            try:
                _hab_ca_inflight.difference_update(batch_codes)
            except Exception:
                pass
            logger.error(f"[hab-check-active-w{worker_id}] Worker crashed: {e}", exc_info=True)
            hab_check_active_state["workers"][worker_id]["status"] = "crashed"
            _audit_err("hab-check-active", "CRASH", str(e)[:120])
            await _send_slack_worker_alert(
                emoji="🚨",
                title=f"[HABITACLIA] Check Active w{worker_id} — CRASH",
                fields={
                    "Worker": f"hab-w{worker_id}",
                    "Error": str(e)[:200],
                    "Accion": f"Reiniciando en {cfg['restart_delay']}s",
                    "Total checked": str(hab_check_active_state["totals"]["checked"]),
                },
                category="deactivation",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info(f"[hab-check-active-w{worker_id}] Restarting worker...")

    logger.info(f"[hab-check-active-w{worker_id}] Worker stopped")


async def _start_hab_check_active():
    """Lanza workers de Habitaclia Check Active — dedicados por tier (4 por tier)."""
    hab_check_active_state["running"] = True
    hab_check_active_state["started_at"] = datetime.now(timezone.utc).isoformat()
    hab_check_active_state["totals"] = {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0, "by_tier": {}}
    hab_check_active_state["last_summary_at"] = time.time()

    workers_per_tier = HAB_CHECK_ACTIVE_CONFIG["workers_per_tier"]
    worker_id = 0
    for tier_index, tier in enumerate(HAB_CHECK_ACTIVE_TIERS):
        for _ in range(workers_per_tier):
            hab_check_active_state["workers"][worker_id] = {
                "status": "starting", "current_tier": tier["name"],
                "batches_done": 0, "last_batch_at": None,
                "checked": 0, "errors": 0,
                "idle_total_s": 0, "_last_idle_at": None,
            }
            asyncio.create_task(_hab_check_active_worker(worker_id, tier_index))
            worker_id += 1

    total_workers = workers_per_tier * len(HAB_CHECK_ACTIVE_TIERS)
    logger.info(f"[hab-check-active] Started {total_workers} workers ({workers_per_tier} per tier)")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HABITACLIA ENRICH — Proceso continuo (1 worker)
#  Sin proxy/JWT — usa HttpxClient directo como check-active
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _hab_enrich_worker():
    """
    Enriquece propiedades Habitaclia pendientes (detail_enriched_at IS NULL).
    Usa concurrency con semaphore como los check-active workers.
    Loop continuo, sleep cuando no hay pendientes.
    """
    cfg = HAB_ENRICH_CONFIG
    logger.info("[hab-enrich] Worker starting")

    while hab_enrich_state["running"]:
        try:
            headers_rest = _supabase_headers()

            async with httpx.AsyncClient(timeout=30) as client:
                httpx_client = HttpxClient(client)
                hab = Habitaclia(httpx_client)

                while hab_enrich_state["running"]:
                    # SELECT via asyncpg: propiedades sin enriquecer
                    try:
                        rows = await db_pool.fetch("""
                            SELECT propertycode,
                                   (habitaclia_specific->>'CodEmp')::int AS cod_emp,
                                   (habitaclia_specific->>'CodInm')::int AS cod_inm
                            FROM async_properties
                            WHERE source = 'habitaclia'
                            AND adisactive = true
                            AND detail_enriched_at IS NULL
                            AND habitaclia_specific->>'CodEmp' IS NOT NULL
                            AND habitaclia_specific->>'CodInm' IS NOT NULL
                            LIMIT $1
                        """, cfg["batch_size"])
                        batch = [
                            {"propertycode": r["propertycode"], "cod_emp": r["cod_emp"], "cod_inm": r["cod_inm"]}
                            for r in rows
                        ]
                    except Exception as e:
                        logger.warning(f"[hab-enrich] SQL error: {e}")
                        await asyncio.sleep(cfg["restart_delay"])
                        continue

                    if not batch:
                        hab_enrich_state["status"] = "idle"
                        logger.info(f"[hab-enrich] No pending properties, sleeping {cfg['sleep_when_idle']}s")
                        await asyncio.sleep(cfg["sleep_when_idle"])
                        continue

                    hab_enrich_state["status"] = f"enriching ({len(batch)} props)"
                    batch_start = time.time()
                    batch_enriched = 0
                    batch_deactivated = 0
                    batch_errors = 0

                    # Concurrent enrichment con semaphore
                    sem = asyncio.Semaphore(cfg.get("concurrent_checks", 15))

                    async def _enrich_one(prop):
                        """Enriquece una propiedad con concurrency limitada."""
                        async with sem:
                            if not hab_enrich_state["running"]:
                                return None
                            pcode = prop["propertycode"]

                            # Skip propiedades que ya fallaron N veces
                            if _hab_enrich_skip.get(pcode, 0) >= HAB_ENRICH_MAX_RETRIES:
                                return None

                            await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))
                            status, detail_data = await _hab_verify_is_active(
                                hab, prop["cod_emp"], prop["cod_inm"]
                            )
                            return (pcode, status, detail_data)

                    results = await asyncio.gather(
                        *[_enrich_one(p) for p in batch],
                        return_exceptions=True,
                    )

                    for r in results:
                        if r is None:
                            continue
                        if isinstance(r, Exception):
                            logger.warning(f"[hab-enrich] Concurrent enrich error: {r}")
                            batch_errors += 1
                            _audit_err("hab-enrich", "exception", str(r)[:80])
                            continue

                        pcode, status, detail_data = r

                        try:
                            if status == "active":
                                enrichment = transform_habitaclia_detail_to_enrichment(detail_data)
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{pcode}"
                                await client.patch(url, headers=headers_rest, json=enrichment, timeout=30)
                                batch_enriched += 1

                            elif status == "inactive":
                                deactivate_data = {
                                    "adisactive": False,
                                    "recheck_count": 0,
                                    "detail_enriched_at": datetime.now(timezone.utc).isoformat(),
                                    "updated_at": datetime.now(timezone.utc).isoformat(),
                                }
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{pcode}"
                                await client.patch(url, headers=headers_rest, json=deactivate_data, timeout=30)
                                batch_deactivated += 1

                            else:
                                batch_errors += 1
                                _audit_err("hab-enrich", "HTTP error", str(detail_data)[:120] if detail_data else None)
                                _hab_enrich_skip[pcode] = _hab_enrich_skip.get(pcode, 0) + 1
                                if _hab_enrich_skip[pcode] >= HAB_ENRICH_MAX_RETRIES:
                                    logger.info(f"[hab-enrich] Skipping {pcode} after {HAB_ENRICH_MAX_RETRIES} failures")

                        except Exception as e:
                            logger.warning(f"[hab-enrich] DB error {pcode}: {e}")
                            batch_errors += 1
                            _audit_err("hab-enrich", "DB error", str(e)[:80])

                    # Contadores
                    hab_enrich_state["totals"]["enriched"] += batch_enriched
                    hab_enrich_state["totals"]["deactivated"] += batch_deactivated
                    hab_enrich_state["totals"]["errors"] += batch_errors
                    hab_enrich_state["totals"]["batches"] += 1

                    batch_time = time.time() - batch_start
                    logger.info(
                        f"[hab-enrich] Batch: {batch_enriched}E {batch_deactivated}D {batch_errors}err "
                        f"({batch_time:.0f}s) | Total: {hab_enrich_state['totals']['enriched']} enriched"
                    )

                    if batch_errors > len(batch) * 0.5 and len(batch) >= 30:
                        logger.warning(f"[hab-enrich] High error rate, sleeping {cfg['error_sleep']}s")
                        await _send_slack_worker_alert(
                            emoji="⚠️",
                            title="[HABITACLIA] Enrich — Error rate alto",
                            fields={
                                "Batch": f"{batch_enriched}E {batch_deactivated}D {batch_errors}err / {len(batch)} total",
                                "Error rate": f"{batch_errors/len(batch)*100:.0f}%",
                                "Total enriched": str(hab_enrich_state["totals"]["enriched"]),
                            },
                            category="deactivation",
                            is_alert=True,
                        )
                        await asyncio.sleep(cfg["error_sleep"])

        except Exception as e:
            logger.error(f"[hab-enrich] Worker crashed: {e}", exc_info=True)
            hab_enrich_state["status"] = "crashed"
            _audit_err("hab-enrich", "CRASH", str(e)[:120])
            await _send_slack_worker_alert(
                emoji="🚨",
                title="[HABITACLIA] Enrich — CRASH",
                fields={
                    "Error": str(e)[:200],
                    "Accion": f"Reiniciando en {cfg['restart_delay']}s",
                    "Total enriched": str(hab_enrich_state["totals"]["enriched"]),
                },
                category="deactivation",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info("[hab-enrich] Restarting worker...")

    hab_enrich_state["status"] = "stopped"
    logger.info("[hab-enrich] Worker stopped")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HABITACLIA SCRAPE UPDATES — Workers
#  Scrapea propiedades recientes por provincia con retry de gaps
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _hab_scrape_updates_sleep(tier_name: str) -> int:
    """Devuelve los segundos de sleep entre ciclos según hora UTC y tier."""
    hour = datetime.now(timezone.utc).hour
    schedule = HAB_SCRAPE_UPDATES_SCHEDULE.get(tier_name, {})
    return schedule.get(hour, 900)


async def _hab_scrape_fetch_page(client: httpx.AsyncClient, slug: str, page: int) -> list:
    """Fetcha una página individual de listings de Habitaclia.
    Devuelve lista de Ads (puede estar vacía)."""
    url = (
        f"https://nativeapps.gw.habitaclia.com/listados/"
        f"{slug}-{page}.api?ordenar=mas_recientes"
    )
    headers = {
        "Accept": "application/json",
        "Accept-Charset": "UTF-8",
        "Accept-Encoding": "gzip",
        "Connection": "Keep-Alive",
        "User-Agent": "Android/Ktor (habitacliaMobileApp[7.290.0;729000])",
        "Host": "nativeapps.gw.habitaclia.com",
    }
    try:
        resp = await client.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return []
        data = resp.json()
        return data.get("Ads", [])
    except Exception as e:
        logger.warning(f"[hab-scrape] Error fetching page {page} of {slug}: {e}")
        return []


async def _hab_scrape_enrich_new(
    client: httpx.AsyncClient, hab, ads: list, new_codes: list, label: str
) -> int:
    """Enriquece propiedades recién insertadas inline (detail API)."""
    if not new_codes:
        return 0

    code_map = {}
    for ad in ads:
        cod_prod = ad.get("CodProd", "")
        cod_emp = ad.get("CodEmp")
        cod_inm = ad.get("CodInm")
        if cod_prod and cod_emp and cod_inm:
            code_map[f"hab-{cod_prod}"] = (cod_emp, cod_inm)

    supabase_headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    enriched = 0
    for pcode in new_codes:
        if pcode not in code_map:
            continue
        cod_emp, cod_inm = code_map[pcode]
        try:
            detail_ok, detail_data = await hab.get_property_detail(
                cod_emp=cod_emp, cod_inm=cod_inm
            )
            if detail_ok and isinstance(detail_data, dict):
                enrichment = transform_habitaclia_detail_to_enrichment(detail_data)
                update_url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{pcode}"
                resp = await client.patch(
                    update_url, headers=supabase_headers, json=enrichment, timeout=30
                )
                if resp.status_code in [200, 204]:
                    enriched += 1
                else:
                    logger.warning(f"[{label}] Enrich DB fail {pcode}: {resp.status_code}")
        except Exception as e:
            logger.warning(f"[{label}] Enrich error {pcode}: {e}")
        await asyncio.sleep(0.3)

    return enriched


async def _promote_comarca_to_whitelist(location_code: str, comarca_name: str):
    """Promueve una comarca a whitelist (memoria + DB persistente).
    Se llama cuando una comarca regular satura su límite de 10."""
    global db_pool
    if location_code not in HABITACLIA_COMARCAS_WHITELIST:
        HABITACLIA_COMARCAS_WHITELIST[location_code] = set()
    if comarca_name in HABITACLIA_COMARCAS_WHITELIST[location_code]:
        return  # Ya está en whitelist
    HABITACLIA_COMARCAS_WHITELIST[location_code].add(comarca_name)
    try:
        if db_pool:
            await db_pool.execute(
                "INSERT INTO habitaclia_dynamic_whitelist (location_code, comarca_name) "
                "VALUES ($1, $2) ON CONFLICT DO NOTHING",
                location_code, comarca_name,
            )
        logger.info(f"[whitelist] PROMOTED '{comarca_name}' ({location_code}) to whitelist")
    except Exception as e:
        logger.warning(f"[whitelist] DB persist failed for '{comarca_name}': {e}")


async def _hab_scrape_updates_worker(worker_id: int, tier_name: str):
    """
    Worker continuo: scrapea propiedades recientes de Habitaclia
    para todas las provincias de su tier.
    Retry automático de páginas extra si detecta gap.
    """
    cfg = HAB_SCRAPE_UPDATES_CONFIG
    label = f"hab-scrape-{tier_name}-w{worker_id}"
    state_ref = hab_scrape_updates_state["workers"][worker_id]
    totals = hab_scrape_updates_state["totals"]

    logger.info(f"[{label}] Starting")
    state_ref["status"] = "running"

    while hab_scrape_updates_state["running"]:
        try:
            cycle_start = time.time()
            provinces = PROVINCES_BY_TIER[tier_name]

            cycle_inserted = 0
            cycle_updated = 0
            cycle_enriched = 0
            cycle_errors = 0
            cycle_gaps = 0
            cycle_new_codes = []
            province_results = []

            async with httpx.AsyncClient(timeout=30) as client:
                httpx_client = HttpxClient(client)
                hab = Habitaclia(httpx_client)

                for province in provinces:
                    if not hab_scrape_updates_state["running"]:
                        break

                    prov_name = province["name"]
                    hab_codes = province["habitaclia_codes"]
                    state_ref["current_province"] = prov_name

                    prov_inserted = 0
                    prov_updated = 0
                    prov_enriched = 0
                    prov_errors = 0
                    prov_max_pages_zones = []

                    for hab_code in hab_codes:
                        if hab_code in HABITACLIA_NO_TREE:
                            continue

                        tree_children = HABITACLIA_TREE.get(hab_code)
                        if not tree_children:
                            continue

                        # Separar zonas
                        zone_capitals = [z for z in tree_children if not z["is_aggregate"]]
                        all_comarcas = [z for z in tree_children if z["is_aggregate"]]

                        whitelist_names = HABITACLIA_COMARCAS_WHITELIST.get(hab_code, set())
                        comarcas_wl = [z for z in all_comarcas if z["name"] in whitelist_names]
                        comarcas_reg = [z for z in all_comarcas if z["name"] not in whitelist_names]

                        # --- Helper: scrape zona con retry de gaps ---
                        # Comarca regular (limit 10): si >=9 new → PROMUEVE a whitelist
                        #   + re-fetch con limit 40, luego retry páginas como capital.
                        # Capital/WL (limit 40): si >=35 new → retry páginas acumulando.
                        # Para y flaggea si una zona acumula >180 nuevas.
                        async def _scrape_zone(zone, zone_type, ads_limit, continue_threshold):
                            nonlocal prov_inserted, prov_updated, prov_enriched, prov_errors

                            slug = zone["listing_slug"]
                            zone_name = zone["name"]
                            zone_total_inserted = 0
                            zone_total_enriched = 0
                            pages_scraped = 0
                            current_limit = ads_limit
                            current_threshold = continue_threshold

                            try:
                                # — Página 0 —
                                success, result = await hab.scrape_listings(
                                    listing_slug=slug,
                                    sort="mas_recientes",
                                    max_pages=1,
                                )
                                if not success:
                                    logger.warning(f"[{label}] {zone_type} '{zone_name}' failed: {result}")
                                    prov_errors += 1
                                    return

                                ads = result.get("ads", [])[:current_limit]
                                pages_scraped = 1

                                if not ads:
                                    return

                                upsert_ok, inserted, updated, error_msg, new_codes = \
                                    await upsert_habitaclia_to_supabase(client, ads, _new_codes_out=cycle_new_codes)

                                zone_total_inserted += inserted
                                prov_updated += updated

                                if error_msg:
                                    logger.warning(f"[{label}] upsert err '{zone_name}': {error_msg}")

                                # Enriquecer nuevas inline
                                enr = await _hab_scrape_enrich_new(
                                    client, hab, ads, new_codes, label
                                )
                                zone_total_enriched += enr

                                # — COMARCA PROMOTION —
                                # Si comarca regular satura (>=9 de 10) → promover a whitelist
                                # y re-fetch con límite 40 para capturar las que faltan.
                                if zone_type == "comarca" and inserted >= cfg["continue_threshold_comarca"]:
                                    logger.info(
                                        f"[{label}] PROMOTING '{zone_name}' ({hab_code}): "
                                        f"{inserted}>={cfg['continue_threshold_comarca']}. "
                                        f"Re-fetching with limit {cfg['capital_limit']}..."
                                    )
                                    await _promote_comarca_to_whitelist(hab_code, zone_name)

                                    # Escalar límites a nivel capital
                                    current_limit = cfg["capital_limit"]
                                    current_threshold = cfg["continue_threshold_capital"]

                                    # Re-fetch página 0 con límite mayor
                                    success2, result2 = await hab.scrape_listings(
                                        listing_slug=slug,
                                        sort="mas_recientes",
                                        max_pages=1,
                                    )
                                    if success2:
                                        ads2 = result2.get("ads", [])[:current_limit]
                                        if ads2:
                                            u2_ok, ins2, upd2, err2, new2 = \
                                                await upsert_habitaclia_to_supabase(client, ads2, _new_codes_out=cycle_new_codes)
                                            zone_total_inserted += ins2
                                            prov_updated += upd2
                                            enr2 = await _hab_scrape_enrich_new(
                                                client, hab, ads2, new2, label
                                            )
                                            zone_total_enriched += enr2
                                            # Usar este inserted para la detección de continuación
                                            inserted = ins2
                                            logger.info(
                                                f"[{label}] Re-fetch '{zone_name}' @40: "
                                                f"+{ins2} new, +{upd2} upd"
                                            )
                                        else:
                                            inserted = 0
                                    else:
                                        inserted = 0

                                # — ADAPTIVE PAGING —
                                # Para capitals, whitelist, y comarcas recién promovidas.
                                # Seguir paginando hasta que <5 nuevas en una página o max_extra_pages.
                                zone_max_pages_hit = False
                                if inserted >= current_threshold:
                                    logger.info(
                                        f"[{label}] '{zone_name}': "
                                        f"{inserted}>={current_threshold}. Continuing pagination..."
                                    )

                                    for extra_page in range(1, cfg["max_extra_pages"] + 1):
                                        # Check per-zone overflow ANTES de pedir más
                                        if zone_total_inserted >= cfg["zone_overflow_threshold"]:
                                            logger.warning(
                                                f"[{label}] ZONE OVERFLOW '{zone_name}': "
                                                f"{zone_total_inserted}>={cfg['zone_overflow_threshold']}. "
                                                f"Stopping."
                                            )
                                            await _send_slack_worker_alert(
                                                "\U0001f6a8",
                                                f"HAB SCRAPE ZONE OVERFLOW",
                                                {
                                                    "Zone": zone_name,
                                                    "Province": prov_name,
                                                    "Total inserted": str(zone_total_inserted),
                                                    "Pages scraped": str(pages_scraped),
                                                },
                                                category="scrape_updates",
                                                is_alert=True,
                                            )
                                            break

                                        extra_ads = await _hab_scrape_fetch_page(
                                            client, slug, extra_page
                                        )
                                        if not extra_ads:
                                            break

                                        pages_scraped += 1
                                        extra_ads = extra_ads[:current_limit]

                                        rtr_ok, rtr_ins, rtr_upd, rtr_err, rtr_new = \
                                            await upsert_habitaclia_to_supabase(client, extra_ads, _new_codes_out=cycle_new_codes)

                                        zone_total_inserted += rtr_ins
                                        prov_updated += rtr_upd

                                        rtr_enr = await _hab_scrape_enrich_new(
                                            client, hab, extra_ads, rtr_new, label
                                        )
                                        zone_total_enriched += rtr_enr

                                        logger.info(
                                            f"[{label}] p{extra_page} '{zone_name}': "
                                            f"+{rtr_ins} new, +{rtr_upd} upd "
                                            f"(zone total: {zone_total_inserted})"
                                        )

                                        # Condición de parada: <5 nuevas → ya son conocidas
                                        if rtr_ins < cfg["stop_threshold"]:
                                            break

                                        await asyncio.sleep(1)

                                    # Safety cap: alertar si llegamos al máximo de páginas extra
                                    if pages_scraped >= cfg["max_extra_pages"]:
                                        zone_max_pages_hit = True
                                        prov_max_pages_zones.append(zone_name)
                                        logger.warning(
                                            f"[{label}] MAX_PAGES_REACHED '{zone_name}': "
                                            f"{pages_scraped}p, {zone_total_inserted} inserted"
                                        )
                                        await _send_slack_worker_alert(
                                            "\u26a0\ufe0f", "HAB SCRAPE — MAX PAGES REACHED",
                                            {
                                                "Zone": zone_name,
                                                "Province": prov_name,
                                                "Pages scraped": str(pages_scraped),
                                                "Total inserted": str(zone_total_inserted),
                                            },
                                            category="scrape_updates",
                                            is_alert=True,
                                        )
                                        _audit_err("hab-scrape-updates", "max_pages_reached", f"{zone_name}: {zone_total_inserted} ins in {pages_scraped}p")

                            except Exception as e:
                                logger.error(f"[{label}] Exception '{zone_name}': {e}")
                                prov_errors += 1
                                return

                            prov_inserted += zone_total_inserted
                            prov_enriched += zone_total_enriched

                            if zone_total_inserted > 0:
                                logger.info(
                                    f"[{label}] {zone_type} '{zone_name}': "
                                    f"{zone_total_inserted} new, {zone_total_enriched} enr "
                                    f"({pages_scraped}p)"
                                )

                        # --- Scrapear todas las zonas ---
                        for cap in zone_capitals:
                            await _scrape_zone(
                                cap, "capital",
                                cfg["capital_limit"], cfg["continue_threshold_capital"]
                            )
                            await asyncio.sleep(cfg["inter_zone_delay"])

                        for wl in comarcas_wl:
                            await _scrape_zone(
                                wl, "comarca_wl",
                                cfg["capital_limit"], cfg["continue_threshold_whitelist"]
                            )
                            await asyncio.sleep(cfg["inter_zone_delay"])

                        for com in comarcas_reg:
                            await _scrape_zone(
                                com, "comarca",
                                cfg["comarca_limit"], cfg["continue_threshold_comarca"]
                            )
                            await asyncio.sleep(cfg["inter_zone_delay"])

                    # --- Resultado provincia ---
                    province_results.append({
                        "name": prov_name,
                        "inserted": prov_inserted,
                        "updated": prov_updated,
                        "enriched": prov_enriched,
                        "errors": prov_errors,
                        "max_pages_zones": prov_max_pages_zones,
                    })

                    cycle_inserted += prov_inserted
                    cycle_updated += prov_updated
                    cycle_enriched += prov_enriched
                    cycle_errors += prov_errors
                    cycle_gaps += len(prov_max_pages_zones)

                    # Flush webhook por provincia (no esperar al final del ciclo)
                    if cycle_new_codes:
                        await notify_new_properties(cycle_new_codes)
                        cycle_new_codes = []

                    await asyncio.sleep(cfg["inter_province_delay"])

            # --- Fin del ciclo ---
            cycle_time = time.time() - cycle_start
            totals["cycles"] += 1
            totals["provinces_scraped"] += len(provinces)
            totals["inserted"] += cycle_inserted
            totals["updated"] += cycle_updated
            totals["enriched"] += cycle_enriched
            totals["max_pages_hits"] += cycle_gaps
            totals["errors"] += cycle_errors

            state_ref["last_cycle_at"] = datetime.now(timezone.utc).isoformat()
            state_ref["last_cycle_time"] = round(cycle_time, 1)
            state_ref["last_cycle_inserted"] = cycle_inserted
            state_ref["last_cycle_gaps"] = cycle_gaps
            state_ref["cycles_done"] = state_ref.get("cycles_done", 0) + 1

            logger.info(
                f"[{label}] Cycle #{state_ref['cycles_done']} done in {cycle_time:.0f}s: "
                f"{cycle_inserted} new, {cycle_updated} upd, {cycle_enriched} enr, "
                f"{cycle_gaps} gaps, {cycle_errors} err"
            )

            # Acumular resultados por provincia para resumen periódico
            acc = hab_scrape_updates_state["accumulated"]
            for pr in province_results:
                pn = pr["name"]
                if pn not in acc:
                    acc[pn] = {"inserted": 0, "updated": 0, "enriched": 0, "errors": 0, "max_pages_zones": []}
                acc[pn]["inserted"] += pr["inserted"]
                acc[pn]["updated"] += pr["updated"]
                acc[pn]["enriched"] += pr["enriched"]
                acc[pn]["errors"] += pr["errors"]
                acc[pn]["max_pages_zones"].extend(pr["max_pages_zones"])

            # Resumen Slack cada 6h (solo el worker 0 lo envía para evitar duplicados)
            now_ts = time.time()
            interval = hab_scrape_updates_state["summary_interval"]
            if worker_id == 0 and (now_ts - hab_scrape_updates_state["last_summary_at"]) >= interval:
                await _hab_scrape_updates_periodic_summary()
                hab_scrape_updates_state["last_summary_at"] = now_ts

            # --- Circuit breaker: detect sustained failures ---
            if cycle_inserted == 0 and cycle_errors > 0:
                state_ref["_consecutive_failures"] = state_ref.get("_consecutive_failures", 0) + 1
            else:
                state_ref["_consecutive_failures"] = 0

            if state_ref.get("_consecutive_failures", 0) >= 3:
                cb_count = state_ref["_consecutive_failures"]
                backoff = min(180, 60 * cb_count)  # 180s max (antes 300s)
                state_ref["_consecutive_failures"] = min(cb_count, 5)  # Cap para reducir ruido Slack
                await _send_slack_worker_alert(
                    "\u26a1", f"HAB SCRAPE CIRCUIT BREAKER \u2014 {tier_name}",
                    {
                        "Consecutive failures": str(cb_count),
                        "Last cycle errors": str(cycle_errors),
                        "Backoff": f"{backoff}s",
                    },
                    category="scrape_updates",
                    is_alert=True,
                )
                logger.warning(f"[{label}] Circuit breaker: {cb_count} failures, backoff {backoff}s")
                state_ref["status"] = f"circuit-breaker ({backoff}s)"
                await asyncio.sleep(backoff)
                continue  # Restart cycle with new client
                _audit_err("hab-scrape-updates", "circuit_breaker", f"{tier_name}: {cb_count} failures")

            # Sleep hasta siguiente ciclo
            sleep_secs = _hab_scrape_updates_sleep(tier_name)
            actual_sleep = max(30, sleep_secs - cycle_time)
            state_ref["status"] = f"sleeping {actual_sleep:.0f}s"
            logger.info(f"[{label}] Sleeping {actual_sleep:.0f}s")
            await asyncio.sleep(actual_sleep)
            state_ref["status"] = "running"

        except Exception as e:
            logger.error(f"[{label}] CRASH: {e}", exc_info=True)
            state_ref["status"] = f"crashed: {str(e)[:60]}"
            totals["errors"] += 1
            _audit_err("hab-scrape-updates", "CRASH", str(e)[:120])

            await _send_slack_worker_alert(
                "\U0001f4a5", f"HAB SCRAPE CRASH \u2014 {label}",
                {"Error": str(e)[:200]},
                category="scrape_updates",
                is_alert=True,
            )

            await asyncio.sleep(cfg["restart_delay"])
            state_ref["status"] = "restarting"

    state_ref["status"] = "stopped"
    logger.info(f"[{label}] Stopped")


async def _hab_scrape_updates_periodic_summary():
    """Envía resumen periódico (cada 6h) de scrape-updates a Slack.
    Lee del acumulador y lo resetea."""
    acc = hab_scrape_updates_state["accumulated"]
    totals = hab_scrape_updates_state["totals"]

    if not acc:
        return

    # Filtrar provincias con actividad
    active = {k: v for k, v in acc.items()
              if v["inserted"] > 0 or v["errors"] > 0 or v["max_pages_zones"]}

    if not active:
        # Resetear acumulador aunque no haya actividad
        hab_scrape_updates_state["accumulated"] = {}
        return

    total_ins = sum(v["inserted"] for v in acc.values())
    total_upd = sum(v["updated"] for v in acc.values())
    total_enr = sum(v["enriched"] for v in acc.values())
    total_max_pages = sum(len(v["max_pages_zones"]) for v in acc.values())

    lines = []
    problems = {k: v for k, v in active.items() if v["errors"] > 0 or v["max_pages_zones"]}
    for pn in sorted(problems.keys(), key=lambda k: -(len(problems[k]["max_pages_zones"]) * 1000 + problems[k]["errors"])):
        v = problems[pn]
        emoji = "\u26a0\ufe0f" if v["max_pages_zones"] else "\u26a0\ufe0f"
        line = f"{emoji} *{pn}*: {v['inserted']} new, {v['updated']} upd, {v['enriched']} enr"
        if v["max_pages_zones"]:
            unique_zones = sorted(set(v["max_pages_zones"]))
            line += f"\n   \u21b3 Max pages: {', '.join(unique_zones)}"
        if v["errors"] > 0:
            line += f" ({v['errors']} err)"
        lines.append(line)

    # Solo enviar si hay max_pages_hits
    if total_max_pages > 0:
        detail = "\n".join(lines) if lines else None
        await _send_slack_worker_alert(
            "\u26a0\ufe0f", "HAB SCRAPE — MAX PAGES ALCANZADO",
            {
                "Inserted": str(total_ins),
                "Max pages hit": str(total_max_pages),
                "Total cycles": str(totals["cycles"]),
            },
            detail=detail,
            category="scrape_updates",
            is_alert=total_max_pages > 0,
        )

    # Resetear acumulador
    hab_scrape_updates_state["accumulated"] = {}
    logger.info(f"[hab-scrape-updates] Periodic summary sent: {total_ins} new, {total_max_pages} max_pages_hits")


async def _start_hab_scrape_updates():
    """Lanza 3 workers de Habitaclia scrape-updates (1 por tier)."""
    hab_scrape_updates_state["running"] = True
    hab_scrape_updates_state["started_at"] = datetime.now(timezone.utc).isoformat()
    hab_scrape_updates_state["last_summary_at"] = time.time()
    hab_scrape_updates_state["accumulated"] = {}
    hab_scrape_updates_state["totals"] = {
        "cycles": 0, "provinces_scraped": 0, "inserted": 0,
        "updated": 0, "enriched": 0, "max_pages_hits": 0, "errors": 0,
    }

    tiers = ["capitals", "main_provinces", "rest"]
    for worker_id, tier_name in enumerate(tiers):
        hab_scrape_updates_state["workers"][worker_id] = {
            "status": "starting",
            "tier": tier_name,
            "cycles_done": 0,
            "last_cycle_at": None,
        }
        asyncio.create_task(_hab_scrape_updates_worker(worker_id, tier_name))

    logger.info("[hab-scrape-updates] Started 3 workers (capitals, main_provinces, rest)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IDEALISTA SCRAPE UPDATES — Workers continuos
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3 workers (1 por tier). Cada uno recorre sus provincias, llama a
# search_properties_by_location_id, upsert, y detecta gaps.
# NO hace enrichment inline — el enrich worker existente se encarga.
# Requiere TLS client + JWT + proxy.

async def _id_scrape_updates_worker(worker_id: int, tier_name: str):
    """Worker continuo de Idealista scrape-updates para un tier."""
    label = f"id-scrape-{tier_name}-w{worker_id}"
    cfg = ID_SCRAPE_UPDATES_CONFIG
    schedule = ID_SCRAPE_UPDATES_SCHEDULE[tier_name]
    provinces = PROVINCES_BY_TIER.get(tier_name, [])
    state_ref = id_scrape_updates_state["workers"][worker_id]
    totals = id_scrape_updates_state["totals"]

    logger.info(f"[{label}] Starting with {len(provinces)} provinces")
    state_ref["status"] = "running"

    # --- Proxy: empezar con regular, fallback a premium ---
    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
    _consecutive_proxy_errors = 0
    _fallback_at = 0

    while id_scrape_updates_state["running"]:
        try:
            # --- Rebounce ---
            if _proxy_label == "premium" and _fallback_at and PROXY_URL_REGULAR:
                if time.time() - _fallback_at > PROXY_REBOUNCE_SECONDS:
                    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
                    _fallback_at = 0
                    logger.info(f"[{label}] PROXY REBOUNCE: premium → regular")

            # JWT siempre via premium, luego recrear con proxy activo
            temp_idealista, _, _ = await _get_idealista_client()
            jwt = temp_idealista.jwt_token
            proxy_url = _active_proxy
            tls_impl = TlsClientImpl(proxy_url=proxy_url)
            idealista = Idealista(tls_impl)
            idealista.jwt_token = jwt
            request_count = 0

            cycle_start = time.time()
            cycle_inserted = 0
            cycle_updated = 0
            cycle_gaps = 0
            cycle_errors = 0
            cycle_new_codes = []
            province_results = []

            for prov in provinces:
                if not id_scrape_updates_state["running"]:
                    break

                prov_name = prov["name"]
                location_ids = prov["idealista_location_ids"]
                prov_inserted = 0
                prov_updated = 0

                try:
                    # Rotar TLS session periódicamente
                    request_count += 1
                    if request_count % cfg["session_rotate_every"] == 0:
                        tls_impl = TlsClientImpl(proxy_url=proxy_url)
                        idealista = Idealista(tls_impl)
                        idealista.jwt_token = jwt_token_cache["token"]
                        logger.debug(f"[{label}] TLS session rotated after {request_count} requests")

                    # Renovar JWT si está por expirar
                    await _refresh_jwt_if_needed(idealista)

                    # --- Página 1 ---
                    success, response = await idealista.search_properties_by_location_id(
                        country="es",
                        max_items=cfg["max_items"],
                        location_ids=location_ids,
                        property_type="homes",
                        operation="sale",
                        num_page=1,
                        order="publicationDate",
                        locale="es",
                        quality="high",
                        gallery=True,
                    )

                    if not success:
                        logger.warning(f"[{label}] {prov_name}: search failed: {response}")
                        cycle_errors += 1
                        _audit_err("id-scrape-updates", "search_failed", f"{prov_name}: {str(response)[:60]}")
                        province_results.append({
                            "name": prov_name, "inserted": 0, "updated": 0,
                            "errors": 1, "max_pages_hit": False,
                        })
                        await asyncio.sleep(cfg["inter_province_delay"])
                        continue

                    elements = response.get("elementList", [])

                    if not elements:
                        province_results.append({
                            "name": prov_name, "inserted": 0, "updated": 0,
                            "errors": 0, "max_pages_hit": False,
                        })
                        await asyncio.sleep(cfg["inter_province_delay"])
                        continue

                    # Upsert página 1
                    async with httpx.AsyncClient() as client:
                        ok, ins, upd, err_msg = await upsert_to_supabase(client, elements, _new_codes_out=cycle_new_codes)

                    prov_inserted += ins
                    prov_updated += upd

                    if ins > 0:
                        _trigger_mirror()

                    # --- Adaptive paging: seguir si la mayoría son nuevas ---
                    prov_max_pages_hit = False
                    total_pages = 1

                    if ins >= cfg["continue_threshold"]:
                        logger.info(
                            f"[{label}] {prov_name}: {ins}>={cfg['continue_threshold']} new on p1. "
                            f"Continuing pagination..."
                        )

                        for extra_page in range(2, cfg["max_pages"] + 1):
                            # Renovar JWT antes de cada página extra
                            await _refresh_jwt_if_needed(idealista)

                            success_r, response_r = await idealista.search_properties_by_location_id(
                                country="es",
                                max_items=cfg["max_items"],
                                location_ids=location_ids,
                                property_type="homes",
                                operation="sale",
                                num_page=extra_page,
                                order="publicationDate",
                                locale="es",
                                quality="high",
                                gallery=True,
                            )

                            if not success_r:
                                logger.warning(f"[{label}] p{extra_page} {prov_name}: failed")
                                break

                            extra_elements = response_r.get("elementList", [])
                            if not extra_elements:
                                break

                            async with httpx.AsyncClient() as client:
                                ok_r, ins_r, upd_r, err_r = await upsert_to_supabase(
                                    client, extra_elements, _new_codes_out=cycle_new_codes
                                )

                            prov_inserted += ins_r
                            prov_updated += upd_r
                            total_pages += 1

                            if ins_r > 0:
                                _trigger_mirror()

                            logger.info(
                                f"[{label}] p{extra_page} {prov_name}: "
                                f"+{ins_r} new, +{upd_r} upd"
                            )

                            # Condición de parada: <5 nuevas → ya hemos cruzado lo conocido
                            if ins_r < cfg["stop_threshold"]:
                                break

                            await asyncio.sleep(1)

                        # Safety cap: alertar si llegamos al máximo de páginas
                        if total_pages >= cfg["max_pages"]:
                            prov_max_pages_hit = True
                            logger.warning(
                                f"[{label}] MAX_PAGES_REACHED {prov_name}: "
                                f"{total_pages} pages, {prov_inserted} total inserted"
                            )
                            await _send_slack_worker_alert(
                                "\u26a0\ufe0f", f"ID SCRAPE — MAX PAGES REACHED",
                                {
                                    "Province": prov_name,
                                    "Pages scraped": str(total_pages),
                                    "Total inserted": str(prov_inserted),
                                },
                                category="scrape_updates",
                                is_alert=True,
                            )
                            _audit_err("id-scrape-updates", "max_pages_reached", f"{prov_name}: {prov_inserted} ins in {total_pages}p")

                except Exception as e:
                    logger.error(f"[{label}] Exception {prov_name}: {e}")
                    cycle_errors += 1
                    _audit_err("id-scrape-updates", "exception", f"{prov_name}: {str(e)[:60]}")
                    province_results.append({
                        "name": prov_name, "inserted": 0, "updated": 0,
                        "errors": 1, "max_pages_hit": False,
                    })
                    await asyncio.sleep(cfg["inter_province_delay"])
                    continue

                province_results.append({
                    "name": prov_name,
                    "inserted": prov_inserted,
                    "updated": prov_updated,
                    "errors": 0,
                    "max_pages_hit": prov_max_pages_hit,
                })

                cycle_inserted += prov_inserted
                cycle_updated += prov_updated
                if prov_max_pages_hit:
                    cycle_gaps += 1

                # Flush webhook por provincia (no esperar al final del ciclo)
                if cycle_new_codes:
                    await notify_new_properties(cycle_new_codes)
                    cycle_new_codes = []

                # Per-province overflow alert
                if prov_inserted > cfg["overflow_threshold"]:
                    await _send_slack_worker_alert(
                        "\U0001f6a8", f"ID SCRAPE OVERFLOW — {prov_name}",
                        {
                            "Province": prov_name,
                            "Tier": tier_name,
                            "Inserted": str(prov_inserted),
                        },
                        category="scrape_updates",
                        is_alert=True,
                    )

                await asyncio.sleep(cfg["inter_province_delay"])

            # --- Fin del ciclo ---
            cycle_time = time.time() - cycle_start
            totals["cycles"] += 1
            totals["provinces_scraped"] += len(provinces)
            totals["inserted"] += cycle_inserted
            totals["updated"] += cycle_updated
            totals["max_pages_hits"] += cycle_gaps
            totals["errors"] += cycle_errors

            state_ref["last_cycle_at"] = datetime.now(timezone.utc).isoformat()
            state_ref["last_cycle_time"] = round(cycle_time, 1)
            state_ref["last_cycle_inserted"] = cycle_inserted
            state_ref["last_cycle_gaps"] = cycle_gaps
            state_ref["cycles_done"] = state_ref.get("cycles_done", 0) + 1

            logger.info(
                f"[{label}] Cycle #{state_ref['cycles_done']} done in {cycle_time:.0f}s: "
                f"{cycle_inserted} new, {cycle_updated} upd, "
                f"{cycle_gaps} gaps, {cycle_errors} err"
            )

            # Acumular resultados por provincia para resumen periódico
            acc = id_scrape_updates_state["accumulated"]
            for pr in province_results:
                pn = pr["name"]
                if pn not in acc:
                    acc[pn] = {"inserted": 0, "updated": 0, "errors": 0, "max_pages_hit": False}
                acc[pn]["inserted"] += pr["inserted"]
                acc[pn]["updated"] += pr["updated"]
                acc[pn]["errors"] += pr["errors"]
                if pr["max_pages_hit"]:
                    acc[pn]["max_pages_hit"] = True

            # Resumen Slack cada 6h (solo worker 0)
            now_ts = time.time()
            interval = id_scrape_updates_state["summary_interval"]
            if worker_id == 0 and (now_ts - id_scrape_updates_state["last_summary_at"]) >= interval:
                await _id_scrape_updates_periodic_summary()
                id_scrape_updates_state["last_summary_at"] = now_ts

            # --- Circuit breaker: detect sustained failures ---
            if cycle_inserted == 0 and cycle_errors > 0:
                state_ref["_consecutive_failures"] = state_ref.get("_consecutive_failures", 0) + 1
            else:
                state_ref["_consecutive_failures"] = 0

            if state_ref.get("_consecutive_failures", 0) >= 3:
                cb_count = state_ref["_consecutive_failures"]
                # Si estamos en regular proxy, cambiar a premium antes de hacer backoff
                if _proxy_label == "regular":
                    _active_proxy, _proxy_label = _proxy_select(prefer_regular=False)
                    _fallback_at = time.time()
                    logger.warning(f"[{label}] PROXY FALLBACK: regular → premium (circuit breaker)")
                    await _send_slack_worker_alert(
                        "\U0001f504", f"ID SCRAPE — Proxy fallback a PREMIUM ({tier_name})",
                        {"Motivo": f"{cb_count} circuit breaker failures"},
                        category="scrape_updates", is_alert=True,
                    )
                backoff = min(180, 60 * cb_count)  # 180s max (antes 300s)
                state_ref["_consecutive_failures"] = min(cb_count, 5)  # Cap para reducir ruido Slack
                await _send_slack_worker_alert(
                    "\u26a1", f"ID SCRAPE CIRCUIT BREAKER \u2014 {tier_name}",
                    {
                        "Consecutive failures": str(cb_count),
                        "Last cycle errors": str(cycle_errors),
                        "Backoff": f"{backoff}s",
                    },
                    category="scrape_updates",
                    is_alert=True,
                )
                logger.warning(f"[{label}] Circuit breaker: {cb_count} failures, backoff {backoff}s")
                state_ref["status"] = f"circuit-breaker ({backoff}s)"
                await asyncio.sleep(backoff)
                continue  # Restart cycle with new TLS client + JWT
                _audit_err("id-scrape-updates", "circuit_breaker", f"{tier_name}: {cb_count} failures")

            # Sleep adaptativo por hora UTC
            utc_hour = datetime.now(timezone.utc).hour
            sleep_secs = schedule.get(utc_hour, 1800)
            state_ref["status"] = f"sleeping {sleep_secs}s"
            logger.info(f"[{label}] Sleeping {sleep_secs}s (UTC hour {utc_hour})")
            await asyncio.sleep(sleep_secs)
            state_ref["status"] = "running"

        except Exception as e:
            logger.error(f"[{label}] CRASH: {e}", exc_info=True)
            state_ref["status"] = "crashed"
            _audit_err("id-scrape-updates", "CRASH", str(e)[:120])
            await _send_slack_worker_alert(
                "\U0001f6a8", f"ID SCRAPE CRASH — {tier_name}",
                {"Error": str(e)[:200]},
                category="scrape_updates",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])

    state_ref["status"] = "stopped"
    logger.info(f"[{label}] Stopped")


async def _id_scrape_updates_periodic_summary():
    """Envía resumen periódico (cada 6h) de Idealista scrape-updates a Slack."""
    acc = id_scrape_updates_state["accumulated"]
    totals = id_scrape_updates_state["totals"]

    if not acc:
        return

    active = {k: v for k, v in acc.items()
              if v["inserted"] > 0 or v["errors"] > 0 or v["max_pages_hit"]}

    if not active:
        id_scrape_updates_state["accumulated"] = {}
        return

    total_ins = sum(v["inserted"] for v in acc.values())
    total_upd = sum(v["updated"] for v in acc.values())
    total_max_pages = sum(1 for v in acc.values() if v["max_pages_hit"])

    lines = []
    problems = {k: v for k, v in active.items() if v["errors"] > 0 or v["max_pages_hit"]}
    for pn in sorted(problems.keys(), key=lambda k: -(int(problems[k]["max_pages_hit"]) * 1000 + problems[k]["errors"])):
        v = problems[pn]
        emoji = "\u26a0\ufe0f" if v["max_pages_hit"] else "\u26a0\ufe0f"
        line = f"{emoji} *{pn}*: {v['inserted']} new, {v['updated']} upd"
        if v["max_pages_hit"]:
            line += " \u2014 MAX PAGES"
        if v["errors"] > 0:
            line += f" ({v['errors']} err)"
        lines.append(line)

    # Solo enviar si hay max_pages_hits
    if total_max_pages > 0:
        detail = "\n".join(lines) if lines else None
        await _send_slack_worker_alert(
            "\u26a0\ufe0f", "ID SCRAPE — MAX PAGES ALCANZADO",
            {
                "Inserted": str(total_ins),
                "Max pages hit": str(total_max_pages),
                "Total cycles": str(totals["cycles"]),
            },
            detail=detail,
            category="scrape_updates",
            is_alert=total_max_pages > 0,
        )

    id_scrape_updates_state["accumulated"] = {}
    logger.info(f"[id-scrape-updates] Periodic summary sent: {total_ins} new, {total_max_pages} max_pages_hits")


async def _start_id_scrape_updates():
    """Lanza 3 workers de Idealista scrape-updates (1 por tier)."""
    id_scrape_updates_state["running"] = True
    id_scrape_updates_state["started_at"] = datetime.now(timezone.utc).isoformat()
    id_scrape_updates_state["last_summary_at"] = time.time()
    id_scrape_updates_state["accumulated"] = {}
    id_scrape_updates_state["totals"] = {
        "cycles": 0, "provinces_scraped": 0, "inserted": 0,
        "updated": 0, "max_pages_hits": 0, "errors": 0,
    }

    tiers = ["capitals", "main_provinces", "rest"]
    for worker_id, tier_name in enumerate(tiers):
        id_scrape_updates_state["workers"][worker_id] = {
            "status": "starting",
            "tier": tier_name,
            "cycles_done": 0,
            "last_cycle_at": None,
        }
        asyncio.create_task(_id_scrape_updates_worker(worker_id, tier_name))

    logger.info("[id-scrape-updates] Started 3 workers (capitals, main_provinces, rest)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOTOCASA SCRAPE UPDATES — Workers continuos
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3 workers (1 por tier). Cada uno recorre sus provincias, llama a
# search_properties_by_locations (2 páginas × 36), upsert, y detecta gaps.
# NO usa token Hetzner (search no lo requiere).
# NO hace enrichment inline — el fc-enrich worker existente se encarga.
# Usa httpx + proxy + CSRF session.

async def _fc_scrape_updates_worker(worker_id: int, tier_name: str):
    """Worker continuo de Fotocasa scrape-updates para un tier."""
    label = f"fc-scrape-{tier_name}-w{worker_id}"
    cfg = FC_SCRAPE_UPDATES_CONFIG
    schedule = FC_SCRAPE_UPDATES_SCHEDULE[tier_name]
    provinces = PROVINCES_BY_TIER.get(tier_name, [])
    state_ref = fc_scrape_updates_state["workers"][worker_id]
    totals = fc_scrape_updates_state["totals"]

    logger.info(f"[{label}] Starting with {len(provinces)} provinces")
    state_ref["status"] = "running"

    # --- Proxy: empezar con regular, fallback a premium ---
    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
    _fallback_at = 0

    def _create_fc_client():
        """Crea httpx client con proxy activo para Fotocasa search."""
        kw = {"timeout": 30}
        if _active_proxy:
            kw["proxies"] = _active_proxy
        hc = httpx.AsyncClient(**kw)
        fc = Fotocasa(HttpxClient(hc))
        return hc, fc

    async def _init_session(fc, context=""):
        """Inicializa sesión Fotocasa para obtener CSRF token."""
        try:
            ok, _ = await fc.initialize_session()
            if ok:
                logger.debug(f"[{label}] CSRF session OK{context}")
            else:
                logger.warning(f"[{label}] CSRF session failed{context}")
        except Exception as e:
            logger.warning(f"[{label}] CSRF session error{context}: {e}")

    while fc_scrape_updates_state["running"]:
        try:
            # --- Rebounce ---
            if _proxy_label == "premium" and _fallback_at and PROXY_URL_REGULAR:
                if time.time() - _fallback_at > PROXY_REBOUNCE_SECONDS:
                    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
                    _fallback_at = 0
                    logger.info(f"[{label}] PROXY REBOUNCE: premium → regular")

            http_client, fotocasa = _create_fc_client()
            await _init_session(fotocasa, " (startup)")
            proxy_req_count = 0

            cycle_start = time.time()
            cycle_inserted = 0
            cycle_updated = 0
            cycle_gaps = 0
            cycle_errors = 0
            cycle_new_codes = []
            province_results = []

            for prov in provinces:
                if not fc_scrape_updates_state["running"]:
                    break

                prov_name = prov["name"]
                fc_locations = prov["fotocasa_locations"]
                prov_inserted = 0
                prov_updated = 0

                try:
                    # Rotar proxy periódicamente
                    proxy_req_count += 1
                    if proxy_req_count % cfg["proxy_rotate_every"] == 0:
                        await http_client.aclose()
                        http_client, fotocasa = _create_fc_client()
                        await _init_session(fotocasa, " (rotate)")

                    # --- Scrapear páginas base (1 y 2) ---
                    all_page_props = []
                    for page_num in range(1, cfg["pages_per_province"] + 1):
                        success, response = await fotocasa.search_properties_by_locations(
                            property_type="HOME",
                            transaction_type="SALE",
                            locations=fc_locations,
                            page=page_num,
                            page_size=cfg["page_size"],
                            sort=8,  # más recientes
                            conservation_states="UNDEFINED",
                        )

                        if not success:
                            logger.warning(f"[{label}] {prov_name} page {page_num} failed: {response}")
                            cycle_errors += 1
                            _audit_err("fc-scrape-updates", "search_failed", f"{prov_name} p{page_num}: {str(response)[:60]}")
                            break

                        # Extraer propiedades
                        page_props = []
                        for ph in response.get("placeholders", []):
                            if ph.get("type") == "PROPERTY" and ph.get("property"):
                                page_props.append(ph["property"])
                        for ph in response.get("newConstructionPlaceholders", []):
                            if ph.get("type") == "PROPERTY" and ph.get("property"):
                                page_props.append(ph["property"])

                        all_page_props.extend(page_props)

                        if page_num < cfg["pages_per_province"]:
                            await asyncio.sleep(cfg["inter_page_delay"])

                    if not all_page_props:
                        province_results.append({
                            "name": prov_name, "inserted": 0, "updated": 0,
                            "errors": 0, "max_pages_hit": False,
                        })
                        await asyncio.sleep(cfg["inter_province_delay"])
                        continue

                    # Upsert páginas base
                    async with httpx.AsyncClient() as upsert_client:
                        ok, ins, upd, err_msg = await upsert_fotocasa_to_supabase(
                            upsert_client, all_page_props, _new_codes_out=cycle_new_codes
                        )

                    prov_inserted += ins
                    prov_updated += upd

                    if ins > 0:
                        _trigger_mirror()

                    # --- Adaptive paging: seguir si la mayoría son nuevas ---
                    prov_max_pages_hit = False
                    total_pages = cfg["pages_per_province"]

                    if ins >= cfg["continue_threshold"]:
                        logger.info(
                            f"[{label}] {prov_name}: {ins}>={cfg['continue_threshold']} new in base pages. "
                            f"Continuing pagination..."
                        )

                        next_page = cfg["pages_per_province"] + 1
                        while total_pages < cfg["max_pages"]:
                            # Check per-province overflow
                            if prov_inserted >= cfg["zone_overflow_threshold"]:
                                logger.warning(
                                    f"[{label}] PROVINCE OVERFLOW {prov_name}: "
                                    f"{prov_inserted}>={cfg['zone_overflow_threshold']}. Stopping."
                                )
                                await _send_slack_worker_alert(
                                    "\U0001f6a8", "FC SCRAPE PROVINCE OVERFLOW",
                                    {
                                        "Province": prov_name,
                                        "Total inserted": str(prov_inserted),
                                    },
                                    category="scrape_updates",
                                    is_alert=True,
                                )
                                break

                            success_r, response_r = await fotocasa.search_properties_by_locations(
                                property_type="HOME",
                                transaction_type="SALE",
                                locations=fc_locations,
                                page=next_page,
                                page_size=cfg["page_size"],
                                sort=8,
                                conservation_states="UNDEFINED",
                            )

                            if not success_r:
                                break

                            extra_props = []
                            for ph in response_r.get("placeholders", []):
                                if ph.get("type") == "PROPERTY" and ph.get("property"):
                                    extra_props.append(ph["property"])
                            for ph in response_r.get("newConstructionPlaceholders", []):
                                if ph.get("type") == "PROPERTY" and ph.get("property"):
                                    extra_props.append(ph["property"])

                            if not extra_props:
                                break

                            async with httpx.AsyncClient() as upsert_client:
                                ok_r, ins_r, upd_r, err_r = await upsert_fotocasa_to_supabase(
                                    upsert_client, extra_props, _new_codes_out=cycle_new_codes
                                )

                            prov_inserted += ins_r
                            prov_updated += upd_r
                            total_pages += 1

                            if ins_r > 0:
                                _trigger_mirror()

                            logger.info(
                                f"[{label}] p{next_page} {prov_name}: "
                                f"+{ins_r} new, +{upd_r} upd "
                                f"(prov total: {prov_inserted})"
                            )

                            # Condición de parada: <5 nuevas → ya hemos cruzado lo conocido
                            if ins_r < cfg["stop_threshold"]:
                                break

                            next_page += 1
                            await asyncio.sleep(cfg["inter_page_delay"])

                        # Safety cap: alertar si llegamos al máximo de páginas
                        if total_pages >= cfg["max_pages"]:
                            prov_max_pages_hit = True
                            logger.warning(
                                f"[{label}] MAX_PAGES_REACHED {prov_name}: "
                                f"{total_pages} pages, {prov_inserted} total inserted"
                            )
                            await _send_slack_worker_alert(
                                "\u26a0\ufe0f", f"FC SCRAPE — MAX PAGES REACHED",
                                {
                                    "Province": prov_name,
                                    "Pages scraped": str(total_pages),
                                    "Total inserted": str(prov_inserted),
                                },
                                category="scrape_updates",
                                is_alert=True,
                            )
                            _audit_err("fc-scrape-updates", "max_pages_reached", f"{prov_name}: {prov_inserted} ins in {total_pages}p")

                except Exception as e:
                    logger.error(f"[{label}] Exception {prov_name}: {e}")
                    cycle_errors += 1
                    _audit_err("fc-scrape-updates", "exception", f"{prov_name}: {str(e)[:60]}")
                    province_results.append({
                        "name": prov_name, "inserted": 0, "updated": 0,
                        "errors": 1, "max_pages_hit": False,
                    })
                    await asyncio.sleep(cfg["inter_province_delay"])
                    continue

                province_results.append({
                    "name": prov_name,
                    "inserted": prov_inserted,
                    "updated": prov_updated,
                    "errors": 0,
                    "max_pages_hit": prov_max_pages_hit,
                })

                cycle_inserted += prov_inserted
                cycle_updated += prov_updated
                if prov_max_pages_hit:
                    cycle_gaps += 1

                # Flush webhook por provincia (no esperar al final del ciclo)
                if cycle_new_codes:
                    await notify_new_properties(cycle_new_codes)
                    cycle_new_codes = []

                await asyncio.sleep(cfg["inter_province_delay"])

            # --- Cerrar httpx client del ciclo ---
            try:
                await http_client.aclose()
            except Exception:
                pass

            # --- Fin del ciclo ---
            cycle_time = time.time() - cycle_start
            totals["cycles"] += 1
            totals["provinces_scraped"] += len(provinces)
            totals["inserted"] += cycle_inserted
            totals["updated"] += cycle_updated
            totals["max_pages_hits"] += cycle_gaps
            totals["errors"] += cycle_errors

            state_ref["last_cycle_at"] = datetime.now(timezone.utc).isoformat()
            state_ref["last_cycle_time"] = round(cycle_time, 1)
            state_ref["last_cycle_inserted"] = cycle_inserted
            state_ref["last_cycle_gaps"] = cycle_gaps
            state_ref["cycles_done"] = state_ref.get("cycles_done", 0) + 1

            logger.info(
                f"[{label}] Cycle #{state_ref['cycles_done']} done in {cycle_time:.0f}s: "
                f"{cycle_inserted} new, {cycle_updated} upd, "
                f"{cycle_gaps} gaps, {cycle_errors} err"
            )

            # Acumular para resumen periódico
            acc = fc_scrape_updates_state["accumulated"]
            for pr in province_results:
                pn = pr["name"]
                if pn not in acc:
                    acc[pn] = {"inserted": 0, "updated": 0, "errors": 0, "max_pages_hit": False}
                acc[pn]["inserted"] += pr["inserted"]
                acc[pn]["updated"] += pr["updated"]
                acc[pn]["errors"] += pr["errors"]
                if pr["max_pages_hit"]:
                    acc[pn]["max_pages_hit"] = True

            # Resumen Slack cada 6h (solo worker 0)
            now_ts = time.time()
            interval = fc_scrape_updates_state["summary_interval"]
            if worker_id == 0 and (now_ts - fc_scrape_updates_state["last_summary_at"]) >= interval:
                await _fc_scrape_updates_periodic_summary()
                fc_scrape_updates_state["last_summary_at"] = now_ts

            # --- Circuit breaker: detect sustained failures ---
            if cycle_inserted == 0 and cycle_errors > 0:
                state_ref["_consecutive_failures"] = state_ref.get("_consecutive_failures", 0) + 1
            else:
                state_ref["_consecutive_failures"] = 0

            if state_ref.get("_consecutive_failures", 0) >= 3:
                cb_count = state_ref["_consecutive_failures"]
                # Si estamos en regular proxy, cambiar a premium
                if _proxy_label == "regular":
                    _active_proxy, _proxy_label = _proxy_select(prefer_regular=False)
                    _fallback_at = time.time()
                    logger.warning(f"[{label}] PROXY FALLBACK: regular → premium (circuit breaker)")
                    await _send_slack_worker_alert(
                        "\U0001f504", f"FC SCRAPE — Proxy fallback a PREMIUM ({tier_name})",
                        {"Motivo": f"{cb_count} circuit breaker failures"},
                        category="scrape_updates", is_alert=True,
                    )
                backoff = min(300, 60 * cb_count)  # 180s, 240s, 300s max
                await _send_slack_worker_alert(
                    "\u26a1", f"FC SCRAPE CIRCUIT BREAKER \u2014 {tier_name}",
                    {
                        "Consecutive failures": str(cb_count),
                        "Last cycle errors": str(cycle_errors),
                        "Backoff": f"{backoff}s",
                    },
                    category="scrape_updates",
                    is_alert=True,
                )
                logger.warning(f"[{label}] Circuit breaker: {cb_count} failures, backoff {backoff}s")
                state_ref["status"] = f"circuit-breaker ({backoff}s)"
                try:
                    await http_client.aclose()
                except Exception:
                    pass
                await asyncio.sleep(backoff)
                continue  # Restart cycle with new CSRF session + proxy
                _audit_err("fc-scrape-updates", "circuit_breaker", f"{tier_name}: {cb_count} failures")

            # Sleep adaptativo
            utc_hour = datetime.now(timezone.utc).hour
            sleep_secs = schedule.get(utc_hour, 1800)
            state_ref["status"] = f"sleeping {sleep_secs}s"
            logger.info(f"[{label}] Sleeping {sleep_secs}s (UTC hour {utc_hour})")
            await asyncio.sleep(sleep_secs)
            state_ref["status"] = "running"

        except Exception as e:
            logger.error(f"[{label}] CRASH: {e}", exc_info=True)
            state_ref["status"] = "crashed"
            _audit_err("fc-scrape-updates", "CRASH", str(e)[:120])
            await _send_slack_worker_alert(
                "\U0001f6a8", f"FC SCRAPE CRASH — {tier_name}",
                {"Error": str(e)[:200]},
                category="scrape_updates",
                is_alert=True,
            )
            # Cerrar client si existe
            try:
                await http_client.aclose()
            except Exception:
                pass
            await asyncio.sleep(cfg["restart_delay"])

    state_ref["status"] = "stopped"
    logger.info(f"[{label}] Stopped")


async def _fc_scrape_updates_periodic_summary():
    """Resumen periódico (cada 6h) de Fotocasa scrape-updates."""
    acc = fc_scrape_updates_state["accumulated"]
    totals = fc_scrape_updates_state["totals"]

    if not acc:
        return

    active = {k: v for k, v in acc.items()
              if v["inserted"] > 0 or v["errors"] > 0 or v["max_pages_hit"]}

    if not active:
        fc_scrape_updates_state["accumulated"] = {}
        return

    total_ins = sum(v["inserted"] for v in acc.values())
    total_upd = sum(v["updated"] for v in acc.values())
    total_max_pages = sum(1 for v in acc.values() if v["max_pages_hit"])

    lines = []
    problems = {k: v for k, v in active.items() if v["errors"] > 0 or v["max_pages_hit"]}
    for pn in sorted(problems.keys(), key=lambda k: -(int(problems[k]["max_pages_hit"]) * 1000 + problems[k]["errors"])):
        v = problems[pn]
        emoji = "\u26a0\ufe0f" if v["max_pages_hit"] else "\u26a0\ufe0f"
        line = f"{emoji} *{pn}*: {v['inserted']} new, {v['updated']} upd"
        if v["max_pages_hit"]:
            line += " \u2014 MAX PAGES"
        if v["errors"] > 0:
            line += f" ({v['errors']} err)"
        lines.append(line)

    # Solo enviar si hay max_pages_hits
    if total_max_pages > 0:
        detail = "\n".join(lines) if lines else None
        await _send_slack_worker_alert(
            "\u26a0\ufe0f", "FC SCRAPE — MAX PAGES ALCANZADO",
            {
                "Inserted": str(total_ins),
                "Max pages hit": str(total_max_pages),
                "Total cycles": str(totals["cycles"]),
            },
            detail=detail,
            category="scrape_updates",
            is_alert=total_max_pages > 0,
        )

    fc_scrape_updates_state["accumulated"] = {}
    logger.info(f"[fc-scrape-updates] Periodic summary sent: {total_ins} new, {total_max_pages} max_pages_hits")


async def _start_fc_scrape_updates():
    """Lanza 3 workers de Fotocasa scrape-updates (1 por tier)."""
    fc_scrape_updates_state["running"] = True
    fc_scrape_updates_state["started_at"] = datetime.now(timezone.utc).isoformat()
    fc_scrape_updates_state["last_summary_at"] = time.time()
    fc_scrape_updates_state["accumulated"] = {}
    fc_scrape_updates_state["totals"] = {
        "cycles": 0, "provinces_scraped": 0, "inserted": 0,
        "updated": 0, "max_pages_hits": 0, "errors": 0,
    }

    tiers = ["capitals", "main_provinces", "rest"]
    for worker_id, tier_name in enumerate(tiers):
        fc_scrape_updates_state["workers"][worker_id] = {
            "status": "starting",
            "tier": tier_name,
            "cycles_done": 0,
            "last_cycle_at": None,
        }
        asyncio.create_task(_fc_scrape_updates_worker(worker_id, tier_name))

    logger.info("[fc-scrape-updates] Started 3 workers (capitals, main_provinces, rest)")


async def _start_hab_enrich():
    """Lanza el worker de Habitaclia Enrich."""
    hab_enrich_state["running"] = True
    hab_enrich_state["started_at"] = datetime.now(timezone.utc).isoformat()
    hab_enrich_state["totals"] = {"enriched": 0, "deactivated": 0, "errors": 0, "batches": 0}
    hab_enrich_state["status"] = "starting"
    asyncio.create_task(_hab_enrich_worker())
    logger.info("[hab-enrich] Enrich worker launched")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ENRICH — Proceso continuo (1 worker)
#  SELECTs via asyncpg, UPDATEs via httpx/REST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _enrich_worker():
    """
    Enriquece propiedades pendientes (detail_enriched_at IS NULL).
    Retry 3x en 404/410 → desactivar. Loop continuo, sleep cuando no hay pendientes.
    """
    cfg = ENRICH_CONFIG
    logger.info("[enrich] Worker starting")

    # --- Proxy: empezar con regular, fallback a premium ---
    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
    _consecutive_proxy_errors = 0
    _fallback_at = 0
    logger.info(f"[enrich] Using {_proxy_label} proxy")

    while enrich_state["running"]:
        try:
            # --- Rebounce ---
            if _proxy_label == "premium" and _fallback_at and PROXY_URL_REGULAR:
                if time.time() - _fallback_at > PROXY_REBOUNCE_SECONDS:
                    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
                    _consecutive_proxy_errors = 0
                    _fallback_at = 0
                    logger.info("[enrich] PROXY REBOUNCE: premium → regular")

            temp_idealista, _, _ = await _get_idealista_client()
            jwt = temp_idealista.jwt_token
            proxy_url = _active_proxy
            tls_impl = TlsClientImpl(proxy_url=proxy_url)
            idealista = Idealista(tls_impl)
            idealista.jwt_token = jwt
            headers = _supabase_headers()
            request_count = 0

            async with httpx.AsyncClient() as client:
                while enrich_state["running"]:
                    await _refresh_jwt_if_needed(idealista)

                    # SELECT via asyncpg (sin timeout)
                    try:
                        rows = await db_pool.fetch("""
                            SELECT propertycode FROM async_properties
                            WHERE detail_enriched_at IS NULL
                            AND source = 'idealista'
                            AND adisactive = true
                            LIMIT $1
                        """, cfg["batch_size"])
                        batch = [{"propertycode": r["propertycode"]} for r in rows]
                    except Exception as e:
                        logger.warning(f"[enrich] SQL error: {e}")
                        await asyncio.sleep(cfg["restart_delay"])
                        continue

                    if not batch:
                        enrich_state["status"] = "idle"
                        logger.info(f"[enrich] No pending properties, sleeping {cfg['sleep_when_idle']}s")
                        await asyncio.sleep(cfg["sleep_when_idle"])
                        continue

                    enrich_state["status"] = f"enriching ({len(batch)} props)"
                    batch_start = time.time()
                    batch_enriched = 0
                    batch_deactivated = 0
                    batch_errors = 0
                    batch_proxy_errors = 0
                    batch_skipped = 0  # NEW: contar propiedades saltadas

                    for prop in batch:
                        if not enrich_state["running"]:
                            break

                        property_code = prop["propertycode"]

                        # Skip propiedades que ya fallaron N veces en esta sesion
                        if _enrich_skip.get(property_code, 0) >= ENRICH_MAX_RETRIES:
                            batch_skipped += 1  # NEW: contar en vez de solo continue
                            continue

                        if cfg["delay_min"] > 0:
                            await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))

                        request_count += 1
                        if request_count % cfg["session_rotate_every"] == 0:
                            tls_impl = TlsClientImpl(proxy_url=proxy_url)
                            idealista = Idealista(tls_impl)
                            idealista.jwt_token = jwt_token_cache["token"]

                        status, detail_data = await _verify_is_active(idealista, property_code)

                        try:
                            if status == "active":
                                # UPDATE via REST (enrichment tiene campos dinamicos)
                                enrichment = transform_detail_to_enrichment(detail_data)
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                await client.patch(url, headers=headers, json=enrichment, timeout=30)
                                batch_enriched += 1

                            elif status == "inactive":
                                deactivate_data = {
                                    "adisactive": False,
                                    "recheck_count": 0,
                                    "detail_enriched_at": datetime.now(timezone.utc).isoformat(),
                                    "updated_at": datetime.now(timezone.utc).isoformat(),
                                }
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                await client.patch(url, headers=headers, json=deactivate_data, timeout=30)
                                batch_deactivated += 1

                            else:
                                batch_errors += 1
                                _enrich_skip[property_code] = _enrich_skip.get(property_code, 0) + 1
                                if _enrich_skip[property_code] >= ENRICH_MAX_RETRIES:
                                    logger.info(f"[enrich] Skipping {property_code} after {ENRICH_MAX_RETRIES} failures (until restart)")

                                err_code = detail_data.get("status_code", 0) if isinstance(detail_data, dict) else 0
                                _audit_err("id-enrich", f"HTTP {err_code}")
                                if err_code in (0, 407):
                                    batch_proxy_errors += 1
                                if err_code == 407:
                                    tls_impl = TlsClientImpl(proxy_url=proxy_url)
                                    idealista = Idealista(tls_impl)
                                    idealista.jwt_token = jwt_token_cache["token"]
                                    request_count = 0
                                elif err_code == 401:
                                    # JWT expirado/revocado — forzar refresh
                                    logger.info("[enrich] 401 → refreshing JWT")
                                    jwt_token_cache["expires_at"] = 0
                                    await _refresh_jwt_if_needed(idealista)
                                    idealista.jwt_token = jwt_token_cache["token"]

                        except Exception as e:
                            logger.warning(f"[enrich] DB error {property_code}: {e}")
                            batch_errors += 1
                            _audit_err("id-enrich", "DB error", str(e)[:80])

                    # Contadores
                    enrich_state["totals"]["enriched"] += batch_enriched
                    enrich_state["totals"]["deactivated"] += batch_deactivated
                    enrich_state["totals"]["errors"] += batch_errors
                    enrich_state["totals"]["batches"] += 1

                    batch_time = time.time() - batch_start
                    logger.info(
                        f"[enrich] Batch: {batch_enriched}E {batch_deactivated}D {batch_errors}err "
                        f"({batch_time:.0f}s) | Total: {enrich_state['totals']['enriched']} enriched"
                    )

                    # Si todo el batch fue skipped (todas en _enrich_skip), dormir para no hacer tight loop
                    if batch_skipped == len(batch):
                        logger.warning(f"[enrich] All {len(batch)} props skipped (in _enrich_skip), sleeping {cfg['sleep_when_idle']}s")
                        await asyncio.sleep(cfg["sleep_when_idle"])

                    elif batch_errors > len(batch) * 0.5 and len(batch) >= 30:
                        logger.warning(f"[enrich] High error rate, sleeping {cfg['error_sleep']}s")
                        await _send_slack_worker_alert(
                            emoji="\u26a0\ufe0f",
                            title="[IDEALISTA] Enrich — Error rate alto",
                            fields={
                                "Batch": f"{batch_enriched}E {batch_deactivated}D {batch_errors}err / {len(batch)} total",
                                "Error rate": f"{batch_errors/len(batch)*100:.0f}%",
                                "Total enriched": str(enrich_state["totals"]["enriched"]),
                            },
                            category="deactivation",
                            is_alert=True,
                        )
                        await asyncio.sleep(cfg["error_sleep"])

                    # --- Proxy fallback: solo errores de proxy (0/407), no JWT (401) ---
                    if _proxy_label == "regular" and len(batch) >= 10:
                        if batch_proxy_errors > len(batch) * 0.3:
                            _consecutive_proxy_errors += 1
                            if _consecutive_proxy_errors >= 2:
                                _active_proxy, _proxy_label = _proxy_select(prefer_regular=False)
                                _consecutive_proxy_errors = 0
                                _fallback_at = time.time()
                                logger.warning("[enrich] PROXY FALLBACK: regular → premium")
                                await _send_slack_worker_alert(
                                    "\U0001f504", "[IDEALISTA] Enrich — Proxy fallback a PREMIUM",
                                    {"Motivo": "2 batches >30% errors"},
                                    category="deactivation", is_alert=True,
                                )
                                break
                        else:
                            _consecutive_proxy_errors = 0

        except Exception as e:
            logger.error(f"[enrich] Worker crashed: {e}", exc_info=True)
            enrich_state["status"] = "crashed"
            _audit_err("id-enrich", "CRASH", str(e)[:120])
            await _send_slack_worker_alert(
                emoji="\U0001f6a8",
                title="[IDEALISTA] Enrich — CRASH",
                fields={
                    "Error": str(e)[:200],
                    "Accion": f"Reiniciando en {cfg['restart_delay']}s",
                    "Total enriched": str(enrich_state["totals"]["enriched"]),
                },
                category="deactivation",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info("[enrich] Restarting worker...")

    logger.info("[enrich] Worker stopped")


async def _start_enrich():
    """Lanza el worker de Enrich."""
    enrich_state["running"] = True
    enrich_state["started_at"] = datetime.now(timezone.utc).isoformat()
    enrich_state["totals"] = {"enriched": 0, "deactivated": 0, "errors": 0, "batches": 0}
    enrich_state["status"] = "starting"
    asyncio.create_task(_enrich_worker())
    logger.info("[enrich] Worker launched")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOTOCASA — Enrich Worker (background, con token Hetzner)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Cache de tokens Hetzner: 4 slots, uno por Redroid
# Slot 1 → enrich worker 0, Slot 2 → enrich worker 1
# Slot 3 → check-active worker 0 + recheck, Slot 4 → check-active worker 1
def _new_fc_token_cache():
    return {
        "token": None,
        "obtained_at": 0,
        "requests_since": 0,
        "last_force_refresh_at": 0,
        "consecutive_403s": 0,
        "device_info": None,
    }

_fc_token_slots = {i: _new_fc_token_cache() for i in range(1, 17)}
# Lock asyncio por slot: solo 1 force_refresh a la vez por slot
_fc_token_locks = {i: asyncio.Lock() for i in range(1, 17)}

# Mapeo slot_id → URL del Hetzner Redroid (16 slots)
_FC_SLOT_URLS = {
    1:  HETZNER_TOKEN_URL,
    2:  HETZNER_TOKEN_URL_2,
    3:  HETZNER_TOKEN_URL_3,
    4:  HETZNER_TOKEN_URL_4,
    5:  HETZNER_TOKEN_URL_5,
    6:  HETZNER_TOKEN_URL_6,
    7:  HETZNER_TOKEN_URL_7,
    8:  HETZNER_TOKEN_URL_8,
    9:  HETZNER_TOKEN_URL_9,
    10: HETZNER_TOKEN_URL_10,
    11: HETZNER_TOKEN_URL_11,
    12: HETZNER_TOKEN_URL_12,
    13: HETZNER_TOKEN_URL_13,
    14: HETZNER_TOKEN_URL_14,
    15: HETZNER_TOKEN_URL_15,
    16: HETZNER_TOKEN_URL_16,
}


async def _get_fc_hetzner_token(slot_id=1, force_refresh=False):
    """Obtiene token Hetzner del slot Redroid indicado (1-16).

    Asignación de slots:
    slots 1-3        → enrich workers 0-2 (:8001-:8003)
    slots 4-6        → check-active capitals workers 0-2 (:8004-:8006)
    slots 7-9, 13-15 → check-active main_provinces workers 0-5 (:8007-:8009, :8013-:8015)
    slots 10-11      → check-active rest workers 0-1 (:8010-:8011)
    slots 12, 16     → recheck workers 0-1 (:8012, :8016)

    Protecciones anti-cascada:
    - Lock asyncio: solo 1 refresh simultaneo por slot, los demas esperan
    - Cooldown 30s: tras un refresh exitoso, no permite otro en 30s
    - Rate-limit detection: token fresco + 403 = backoff, no refresh
    """
    cache = _fc_token_slots[slot_id]
    lock = _fc_token_locks[slot_id]
    base_url = _FC_SLOT_URLS[slot_id]
    slot_label = f"slot{slot_id}-redroid"
    now = time.time()
    age = now - cache["obtained_at"]

    # --- CASO 1: force_refresh pero token reciente (<30s) = rate-limit de IP ---
    if force_refresh and age < 30 and cache["token"]:
        cache["consecutive_403s"] += 1
        logger.info(
            f"[fc-token/{slot_label}] 403 con token fresco ({age:.0f}s) - rate-limit de IP, "
            f"no refresh (consecutive_403s={cache['consecutive_403s']})"
        )
        # Si llevamos muchos 403s seguidos con token fresco, devolver None
        # para que el worker haga backoff
        if cache["consecutive_403s"] >= 5:
            logger.warning(f"[fc-token/{slot_label}] 5+ 403s con token fresco, devolviendo None para backoff")
            return None
        cache["requests_since"] += 1
        return cache["token"]

    # --- CASO 2: force_refresh con cooldown activo (<30s desde ultimo refresh) ---
    if force_refresh and cache["token"]:
        cooldown_elapsed = now - cache["last_force_refresh_at"]
        if cooldown_elapsed < 30:
            logger.info(
                f"[fc-token/{slot_label}] Cooldown activo ({cooldown_elapsed:.0f}s < 30s), "
                f"reutilizando token actual (age={age:.0f}s)"
            )
            cache["requests_since"] += 1
            return cache["token"]

    # --- CASO 3: necesita refresh real (forzado, sin token, expirado, o por conteo de requests) ---
    proactive = (not force_refresh
                 and cache["token"]
                 and cache["requests_since"] >= FC_PROACTIVE_REFRESH_REQUESTS)
    if proactive:
        logger.info(
            f"[fc-token/{slot_label}] Proactive refresh: {cache['requests_since']} requests since last refresh "
            f"(threshold={FC_PROACTIVE_REFRESH_REQUESTS}, age={age:.0f}s)"
        )
    if (force_refresh
            or not cache["token"]
            or age > 480
            or proactive):

        # Lock: solo 1 refresh a la vez
        if lock.locked():
            # Otro proceso ya esta refrescando, esperar y reutilizar
            logger.info(f"[fc-token/{slot_label}] Refresh en curso por otro proceso, esperando...")
            async with lock:
                pass  # Solo esperar a que termine
            # Tras la espera, el token deberia estar fresco
            if cache["token"] and (time.time() - cache["obtained_at"]) < 30:
                logger.info(f"[fc-token/{slot_label}] Token refrescado por otro proceso, reutilizando")
                cache["requests_since"] += 1
                return cache["token"]

        async with lock:
            # Double-check: quizas otro proceso ya lo refresco mientras esperabamos
            fresh_age = time.time() - cache["obtained_at"]
            if fresh_age < 15 and cache["token"] and force_refresh:
                logger.info(f"[fc-token/{slot_label}] Token ya refrescado ({fresh_age:.0f}s), reutilizando")
                cache["requests_since"] += 1
                return cache["token"]

            url = base_url
            if force_refresh or proactive:
                url += "?force_refresh=true"
            logger.info(f"[fc-token/{slot_label}] Requesting Hetzner token (force={force_refresh}, proactive={proactive}, age={age:.0f}s)...")
            try:
                async with httpx.AsyncClient(timeout=300.0) as client:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        data = resp.json()
                        cache["token"] = data.get("token")
                        cache["obtained_at"] = time.time()
                        cache["requests_since"] = 0
                        cache["consecutive_403s"] = 0  # Reset 403 counter
                        if force_refresh or proactive:
                            cache["last_force_refresh_at"] = time.time()
                        # v3: capturar device_info (android_id) si Hetzner lo devuelve
                        di = data.get("device_info")
                        if di and isinstance(di, dict) and di.get("android_id"):
                            cache["device_info"] = di
                            logger.info(f"[fc-token/{slot_label}] Device android_id: {di['android_id']}")
                        exec_time = data.get("execution_time_seconds", "?")
                        logger.info(f"[fc-token/{slot_label}] Token obtained in {exec_time}s")
                    else:
                        logger.error(f"[fc-token/{slot_label}] Hetzner failed: {resp.status_code}")
                        return None
            except Exception as e:
                logger.error(f"[fc-token/{slot_label}] Hetzner error: {e}")
                return None

    # Reset 403 counter on successful use
    cache["consecutive_403s"] = 0
    cache["requests_since"] += 1
    return cache["token"]


async def _fc_verify_is_active(fotocasa, property_code, token, transaction_type="SALE"):
    """
    Verifica si una propiedad Fotocasa sigue activa vía detail API.
    Returns: ("active", detail_data) | ("inactive", None) | ("token_expired", {...}) | ("error", {...})
    FC false-inactive rate is ~0.1% — 2 retries are sufficient.
    """
    # Extraer ID numérico y determinar transactionType
    fc_id = property_code.replace("fc-", "")

    for attempt in range(2):
        try:
            success, response = await fotocasa.get_property_detail(
                property_id=fc_id,
                transaction_type=transaction_type,
                external_token=token,
            )
        except Exception as e:
            logger.warning(f"[fc-verify] Network error for {property_code}: {e}")
            return "error", {"status_code": 0, "reason": "network"}

        if success and response:
            return "active", response

        error_status = 0
        if isinstance(response, dict):
            error_status = response.get("status_code", 0)

        # 403 = token expirado/invalidado por Cloudflare — no reintentar, pedir nuevo token
        if error_status == 403:
            return "token_expired", {"status_code": 403, "reason": "cloudflare_blocked"}

        if error_status in (404, 410):
            if attempt < 1:
                await asyncio.sleep(2.0)
                continue
            return "inactive", None
        else:
            logger.warning(f"[fc-verify] HTTP {error_status} for {property_code}")
            return "error", {"status_code": error_status, "reason": f"http_{error_status}"}

    return "inactive", None


async def _fc_enrich_worker(worker_id: int, slot_id: int):
    """
    Enriquece propiedades Fotocasa pendientes (detail_enriched_at IS NULL).
    Usa token Hetzner slot dedicado + rotacion de proxy cada 20 requests.
    worker_id: identificador del worker (0, 1, ...)
    slot_id: slot Redroid a usar (1=:8001, 2=:8002, ...)
    """
    cfg = FC_ENRICH_CONFIG
    # Ordenar ASC/DESC para que 2 workers empiecen por extremos opuestos
    order_dir = "ASC" if worker_id % 2 == 0 else "DESC"
    logger.info(f"[fc-enrich-w{worker_id}] Worker starting (token slot={slot_id}, order={order_dir})")

    # --- Proxy: empezar con regular, fallback a premium ---
    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
    _consecutive_proxy_errors = 0
    _fallback_at = 0

    def _create_fc_http_client():
        """Crea nuevo httpx client con proxy activo (nueva IP)."""
        kw = {"timeout": 30}
        if _active_proxy:
            kw["proxies"] = _active_proxy
        hc = httpx.AsyncClient(**kw)
        fc = Fotocasa(HttpxClient(hc))
        _randomize_fc_device(fc, token_cache=_fc_token_slots[slot_id])
        return hc, HttpxClient(hc), fc

    async def _init_fc_session(fotocasa, label=""):
        """Inicializa sesión Fotocasa para obtener CSRF token."""
        try:
            init_ok, _ = await fotocasa.initialize_session()
            if init_ok:
                logger.info(f"[fc-enrich-w{worker_id}] Session initialized OK{label}")
            else:
                logger.warning(f"[fc-enrich-w{worker_id}] Session init failed{label} (continuing without CSRF)")
        except Exception as e:
            logger.warning(f"[fc-enrich-w{worker_id}] Session init error{label}: {e}")

    while fc_enrich_state["running"]:
        try:
            # --- Rebounce ---
            if _proxy_label == "premium" and _fallback_at and PROXY_URL_REGULAR:
                if time.time() - _fallback_at > PROXY_REBOUNCE_SECONDS:
                    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
                    _consecutive_proxy_errors = 0
                    _fallback_at = 0
                    logger.info(f"[fc-enrich-w{worker_id}] PROXY REBOUNCE: premium → regular")

            http_client, httpx_client, fotocasa = _create_fc_http_client()
            db_client = httpx.AsyncClient(timeout=30)  # Sin proxy — para Supabase REST
            await _init_fc_session(fotocasa, " (startup)")  # FIX: obtener CSRF token
            proxy_req_count = 0

            while fc_enrich_state["running"]:
                    # Obtener token Hetzner slot dedicado
                    token = await _get_fc_hetzner_token(slot_id=slot_id)
                    if not token:
                        logger.warning(f"[fc-enrich-w{worker_id}] No Hetzner token (slot {slot_id}), sleeping 60s")
                        await asyncio.sleep(60)
                        continue

                    ip_success_count = 0  # Reset per-batch (usado en flex check-active y enrich)

                    # --- FLEX DECISION: enrich o check-active? ---
                    flex_threshold = cfg["flex_thresholds"][worker_id] if worker_id < len(cfg["flex_thresholds"]) else 0
                    enrich_pending = 0
                    try:
                        row = await db_pool.fetchrow(
                            "SELECT COUNT(*) as cnt FROM async_properties "
                            "WHERE source = 'fotocasa' AND adisactive = true AND detail_enriched_at IS NULL"
                        )
                        enrich_pending = row["cnt"] if row else 0
                    except Exception as e:
                        logger.warning(f"[fc-enrich-w{worker_id}] Count query error: {e}")
                        enrich_pending = 999  # Fallback: asumir que hay trabajo de enrich

                    do_enrich = enrich_pending > flex_threshold
                    batch = []

                    if do_enrich:
                        # --- MODO ENRICH ---
                        try:
                            rows = await db_pool.fetch("""
                                SELECT propertycode, operation, fotocasa_specific
                                FROM async_properties
                                WHERE detail_enriched_at IS NULL
                                AND source = 'fotocasa'
                                AND adisactive = true
                                LIMIT $1
                            """, cfg["batch_size"])
                            batch = [
                                {
                                    "propertycode": r["propertycode"],
                                    "operation": r["operation"],
                                    "fotocasa_specific": json.loads(r["fotocasa_specific"]) if r["fotocasa_specific"] and isinstance(r["fotocasa_specific"], str) else r["fotocasa_specific"],
                                }
                                for r in rows
                            ]
                        except Exception as e:
                            logger.warning(f"[fc-enrich-w{worker_id}] SQL error: {e}")
                            await asyncio.sleep(cfg["restart_delay"])
                            continue

                    if not batch:
                        # --- MODO FLEX: check-active main_provinces ---
                        fc_enrich_state["workers"][worker_id]["status"] = f"flex:check-active-main (slot {slot_id})"
                        try:
                            ca_batch, ca_tier = await _get_fc_check_active_batch_for_tiers(
                                [1], cfg.get("flex_check_batch_size", 100)
                            )
                        except Exception as e:
                            logger.warning(f"[fc-enrich-w{worker_id}] Flex batch query error: {e}")
                            ca_batch = []

                        if not ca_batch:
                            fc_enrich_state["workers"][worker_id]["status"] = "idle (no enrich, no check-active)"
                            await asyncio.sleep(min(cfg["sleep_when_idle"], 120))
                            continue

                        # Procesar check-active batch
                        headers = _supabase_headers()
                        flex_checked = 0
                        flex_deactivated = 0
                        flex_errors = 0

                        for prop in ca_batch:
                            if not fc_enrich_state["running"]:
                                break

                            property_code = prop["propertycode"]
                            operation = prop.get("operation", "sale")
                            fc_specific = prop.get("fotocasa_specific") or {}

                            # Delay adaptativo: semáforo compartido
                            t_min, t_max = _fc_throttle_get_delay()
                            await asyncio.sleep(random.uniform(t_min, t_max))

                            # Rotar proxy cada N requests
                            proxy_req_count += 1
                            if proxy_req_count % FC_PROXY_ROTATE_EVERY == 0:
                                await http_client.aclose()
                                http_client, httpx_client, fotocasa = _create_fc_http_client()
                                await _init_fc_session(fotocasa, " (flex proxy rotate)")
                                ip_success_count = 0

                            tx_type = "RENT" if operation == "rent" else "SALE"
                            status, detail_data = await _fc_verify_is_active(
                                fotocasa, property_code, token, transaction_type=tx_type
                            )

                            if status == "active":
                                enrichment = transform_fotocasa_detail_to_enrichment(
                                    detail_data, existing_fotocasa_specific=fc_specific,
                                )
                                enrichment["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                                enrichment["adisactive"] = True
                                try:
                                    url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                    await db_client.patch(url, headers=headers, json=enrichment, timeout=30)
                                except Exception as e:
                                    logger.warning(f"[fc-enrich-w{worker_id}] Flex DB error {property_code}: {e}")
                                    flex_errors += 1
                                flex_checked += 1
                                ip_success_count += 1
                                _fc_throttle_record("ok")

                            elif status == "inactive":
                                deactivate_data = {
                                    "adisactive": False,
                                    "recheck_count": 0,
                                    "last_checked_at": datetime.now(timezone.utc).isoformat(),
                                    "updated_at": datetime.now(timezone.utc).isoformat(),
                                }
                                try:
                                    url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                    await db_client.patch(url, headers=headers, json=deactivate_data, timeout=30)
                                except Exception as e:
                                    logger.warning(f"[fc-enrich-w{worker_id}] Flex DB error {property_code}: {e}")
                                    flex_errors += 1
                                flex_deactivated += 1
                                ip_success_count += 1
                                _fc_throttle_record("ok")

                            elif status == "token_expired":
                                _fc_throttle_record("403")
                                _audit_err("fc-check-active", "HTTP 403")
                                had_ok = ip_success_count
                                await http_client.aclose()
                                http_client, httpx_client, fotocasa = _create_fc_http_client()
                                await _init_fc_session(fotocasa, " (flex 403 rotate)")
                                proxy_req_count = 0
                                ip_success_count = 0
                                # Siempre refrescar token en 403
                                token = await _get_fc_hetzner_token(slot_id=slot_id, force_refresh=True)
                                if not token:
                                    await asyncio.sleep(30)
                                    token = await _get_fc_hetzner_token(slot_id=slot_id)
                                flex_errors += 1
                                # Circuit breaker
                                if had_ok == 0:
                                    logger.warning(f"[fc-enrich-w{worker_id}] FLEX CIRCUIT BREAK: 0 OK reqs, backoff 35s")
                                    break
                                t_level = _fc_throttle["current_level"]
                                post_403_delay = {"fast": 3, "cautious": 5, "slow": 10}[t_level]
                                await asyncio.sleep(post_403_delay)
                            else:
                                flex_errors += 1
                                _audit_err("fc-check-active", "HTTP other")

                        # Contribuir a check-active totals
                        flex_batch_total = flex_checked + flex_deactivated + flex_errors
                        fc_check_active_state["totals"]["checked"] += flex_batch_total
                        fc_check_active_state["totals"]["still_active"] += flex_checked
                        fc_check_active_state["totals"]["deactivated"] += flex_deactivated
                        fc_check_active_state["totals"]["errors"] += flex_errors
                        # Per-tier (flex always works on main_provinces)
                        bt = fc_check_active_state["totals"]["by_tier"]
                        if "main_provinces" not in bt:
                            bt["main_provinces"] = {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0}
                        bt["main_provinces"]["checked"] += flex_batch_total
                        bt["main_provinces"]["still_active"] += flex_checked
                        bt["main_provinces"]["deactivated"] += flex_deactivated
                        bt["main_provinces"]["errors"] += flex_errors

                        logger.info(
                            f"[fc-enrich-w{worker_id}] Flex check-active: "
                            f"{flex_checked}A {flex_deactivated}D {flex_errors}err / {len(ca_batch)} "
                            f"(enrich_pending={enrich_pending}, threshold={flex_threshold})"
                        )
                        # Circuit breaker backoff (flex)
                        if flex_errors > 0 and ip_success_count == 0:
                            await asyncio.sleep(35)
                        continue  # Volver al top del loop

                    fc_enrich_state["workers"][worker_id]["status"] = f"enriching ({len(batch)} props)"
                    batch_start = time.time()
                    batch_enriched = 0
                    batch_deactivated = 0
                    batch_errors = 0
                    headers = _supabase_headers()
                    ip_success_count = 0  # Requests exitosas con esta IP
                    _circuit_break = False  # Circuit breaker: 403 con 0 OK reqs

                    for prop in batch:
                        if not fc_enrich_state["running"]:
                            break

                        property_code = prop["propertycode"]

                        # Skip propiedades que ya fallaron N veces
                        if _fc_enrich_skip.get(property_code, 0) >= FC_ENRICH_MAX_RETRIES:
                            continue

                        # Delay entre requests
                        if cfg["delay_min"] > 0:
                            await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))

                        # Rotar proxy cada N requests (nueva IP preventiva)
                        proxy_req_count += 1
                        if proxy_req_count % FC_PROXY_ROTATE_EVERY == 0:
                            logger.info(f"[fc-enrich-w{worker_id}] Proxy rotated (preventive) after {ip_success_count} OK reqs")
                            await http_client.aclose()
                            http_client, httpx_client, fotocasa = _create_fc_http_client()
                            await _init_fc_session(fotocasa, " (proxy rotate)")  # FIX: CSRF
                            ip_success_count = 0

                        # Refrescar token si necesario
                        token = await _get_fc_hetzner_token(slot_id=slot_id)
                        if not token:
                            batch_errors += 1
                            _audit_err("fc-enrich", "no_token")
                            continue

                        status, detail_data = await _fc_verify_is_active(fotocasa, property_code, token)

                        try:
                            if status == "active":
                                existing_fc_specific = prop.get("fotocasa_specific") or {}
                                enrichment = transform_fotocasa_detail_to_enrichment(
                                    detail_data,
                                    existing_fotocasa_specific=existing_fc_specific,
                                )
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                await db_client.patch(url, headers=headers, json=enrichment, timeout=30)
                                batch_enriched += 1
                                ip_success_count += 1

                            elif status == "inactive":
                                deactivate_data = {
                                    "adisactive": False,
                                    "recheck_count": 0,
                                    "detail_enriched_at": datetime.now(timezone.utc).isoformat(),
                                    "updated_at": datetime.now(timezone.utc).isoformat(),
                                }
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                await db_client.patch(url, headers=headers, json=deactivate_data, timeout=30)
                                batch_deactivated += 1
                                ip_success_count += 1

                            elif status == "token_expired":
                                # 403 → rotar proxy + refrescar token — FC rate-limita por token + IP
                                had_ok = ip_success_count
                                logger.info(
                                    f"[fc-enrich-w{worker_id}] 403 → rotating proxy + refreshing token "
                                    f"(had {ip_success_count} OK reqs with current IP)"
                                )
                                await http_client.aclose()
                                http_client, httpx_client, fotocasa = _create_fc_http_client()
                                await _init_fc_session(fotocasa, " (403 proxy rotate)")
                                proxy_req_count = 0
                                ip_success_count = 0
                                # Siempre refrescar token en 403
                                token = await _get_fc_hetzner_token(slot_id=slot_id, force_refresh=True)
                                if not token:
                                    logger.warning(f"[fc-enrich-w{worker_id}] No token after refresh, backoff 30s")
                                    await asyncio.sleep(30)
                                    token = await _get_fc_hetzner_token(slot_id=slot_id)
                                batch_errors += 1
                                _audit_err("fc-enrich", "HTTP 403")
                                # Circuit breaker: 0 OK reqs = fresh combo rejected
                                if had_ok == 0:
                                    logger.warning(f"[fc-enrich-w{worker_id}] CIRCUIT BREAK: 0 OK reqs, backoff 35s")
                                    _circuit_break = True
                                    break
                                await asyncio.sleep(3)

                            else:
                                batch_errors += 1
                                _audit_err("fc-enrich", "HTTP other")
                                _fc_enrich_skip[property_code] = _fc_enrich_skip.get(property_code, 0) + 1
                                if _fc_enrich_skip[property_code] >= FC_ENRICH_MAX_RETRIES:
                                    logger.info(f"[fc-enrich-w{worker_id}] Skipping {property_code} after {FC_ENRICH_MAX_RETRIES} failures")

                        except Exception as e:
                            logger.warning(f"[fc-enrich-w{worker_id}] DB error {property_code}: {e}")
                            batch_errors += 1
                            _audit_err("fc-enrich", "DB error", str(e)[:80])

                    # Contadores
                    fc_enrich_state["totals"]["enriched"] += batch_enriched
                    fc_enrich_state["totals"]["deactivated"] += batch_deactivated
                    fc_enrich_state["totals"]["errors"] += batch_errors
                    fc_enrich_state["totals"]["batches"] += 1
                    fc_enrich_state["workers"][worker_id]["batches_done"] = (
                        fc_enrich_state["workers"][worker_id].get("batches_done", 0) + 1
                    )
                    fc_enrich_state["workers"][worker_id]["last_batch_at"] = (
                        datetime.now(timezone.utc).isoformat()
                    )

                    batch_time = time.time() - batch_start
                    logger.info(
                        f"[fc-enrich-w{worker_id}] Batch: {batch_enriched}E {batch_deactivated}D {batch_errors}err "
                        f"({batch_time:.0f}s) | Total: {fc_enrich_state['totals']['enriched']} enriched"
                    )

                    # Circuit breaker backoff
                    if _circuit_break:
                        await asyncio.sleep(35)
                        continue

                    if batch_errors > len(batch) * 0.5 and len(batch) >= 30:
                        logger.warning(f"[fc-enrich-w{worker_id}] High error rate, sleeping {cfg['error_sleep']}s")
                        await _send_slack_worker_alert(
                            emoji="\u26a0\ufe0f",
                            title=f"[FOTOCASA] Enrich w{worker_id} — Error rate alto",
                            fields={
                                "Worker": f"fc-enrich-w{worker_id} (slot {slot_id})",
                                "Batch": f"{batch_enriched}E {batch_deactivated}D {batch_errors}err / {len(batch)} total",
                                "Error rate": f"{batch_errors/len(batch)*100:.0f}%",
                                "Total enriched": str(fc_enrich_state["totals"]["enriched"]),
                            },
                            category="deactivation",
                            is_alert=True,
                        )
                        await asyncio.sleep(cfg["error_sleep"])

                    # --- Proxy fallback (only for enrich batches, not flex) ---
                    if do_enrich and _proxy_label == "regular" and len(batch) >= 10:
                        if batch_errors > len(batch) * 0.3:
                            _consecutive_proxy_errors += 1
                            if _consecutive_proxy_errors >= 2:
                                _active_proxy, _proxy_label = _proxy_select(prefer_regular=False)
                                _consecutive_proxy_errors = 0
                                _fallback_at = time.time()
                                logger.warning(f"[fc-enrich-w{worker_id}] PROXY FALLBACK: regular → premium")
                                await _send_slack_worker_alert(
                                    "\U0001f504", f"[FOTOCASA] Enrich w{worker_id} — Proxy fallback a PREMIUM",
                                    {"Motivo": "2 batches >30% errors"},
                                    category="deactivation", is_alert=True,
                                )
                                break
                        else:
                            _consecutive_proxy_errors = 0

        except Exception as e:
            logger.error(f"[fc-enrich-w{worker_id}] Worker crashed: {e}", exc_info=True)
            fc_enrich_state["workers"][worker_id]["status"] = "crashed"
            _audit_err("fc-enrich", "CRASH", str(e)[:120])
            try:
                await http_client.aclose()
            except Exception:
                pass
            try:
                await db_client.aclose()
            except Exception:
                pass
            await _send_slack_worker_alert(
                emoji="\U0001f6a8",
                title=f"[FOTOCASA] Enrich w{worker_id} — CRASH",
                fields={
                    "Worker": f"fc-enrich-w{worker_id} (slot {slot_id})",
                    "Error": str(e)[:200],
                    "Accion": f"Reiniciando en {cfg['restart_delay']}s",
                    "Total enriched": str(fc_enrich_state["totals"]["enriched"]),
                },
                category="deactivation",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info(f"[fc-enrich-w{worker_id}] Restarting worker...")

    logger.info(f"[fc-enrich-w{worker_id}] Worker stopped")


async def _start_fc_enrich():
    """Lanza los workers de Enrich Fotocasa (1 por slot Redroid)."""
    fc_enrich_state["running"] = True
    fc_enrich_state["started_at"] = datetime.now(timezone.utc).isoformat()
    fc_enrich_state["totals"] = {"enriched": 0, "deactivated": 0, "errors": 0, "batches": 0}
    fc_enrich_state["status"] = "starting"

    # Worker 0 → slot 1 (:8001), Worker 1 → slot 2 (:8002)
    for i in range(FC_ENRICH_CONFIG["num_workers"]):
        slot = i + 1  # worker 0 → slot 1, worker 1 → slot 2
        fc_enrich_state["workers"][i] = {
            "status": "starting", "slot_id": slot,
            "batches_done": 0, "last_batch_at": None,
        }
        asyncio.create_task(_fc_enrich_worker(worker_id=i, slot_id=slot))

    logger.info(f"[fc-enrich] {FC_ENRICH_CONFIG['num_workers']} workers launched (slots 1-{FC_ENRICH_CONFIG['num_workers']})")



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOTOCASA CHECK ACTIVE — Proceso continuo (2 workers)
#  Similar a Idealista: token Hetzner + proxy + rotación de sesión
#  SELECTs via asyncpg, UPDATEs via httpx/REST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _get_fc_check_active_batch_for_tiers(tier_indices: list, batch_size=100):
    """
    Obtiene batch de propiedades Fotocasa a verificar para tier(s) específico(s).
    Filtra propiedades ya en vuelo por otro worker.
    """
    now_utc = datetime.now(timezone.utc)
    inflight = _fc_ca_inflight
    fetch_size = batch_size * 20

    for tier_index in tier_indices:
        if tier_index >= len(FC_CHECK_ACTIVE_TIERS):
            continue

        tier = FC_CHECK_ACTIVE_TIERS[tier_index]
        cutoff = now_utc - timedelta(hours=tier["cycle_hours"])

        # Query 1: propiedades nunca verificadas (NULL) — prioridad
        query_null = f"""
            SELECT propertycode, operation, fotocasa_specific
            FROM async_properties
            WHERE source = 'fotocasa' AND adisactive = true
            AND {tier['where_clause']}
            AND last_checked_at IS NULL
            LIMIT {fetch_size}
        """
        try:
            rows = await db_pool.fetch(query_null)
            if rows:
                all_props = [
                    {
                        "propertycode": r["propertycode"],
                        "operation": r["operation"],
                        "fotocasa_specific": (
                            json.loads(r["fotocasa_specific"])
                            if r["fotocasa_specific"] and isinstance(r["fotocasa_specific"], str)
                            else r["fotocasa_specific"]
                        ),
                    }
                    for r in rows if r["propertycode"] not in inflight
                ]
                if all_props:
                    return all_props[:batch_size], tier["name"]
        except Exception as e:
            logger.warning(f"[fc-check-active] SQL error (null) tier {tier['name']}: {e}")
            continue

        # Query 2: verificadas hace más del ciclo
        query_old = f"""
            SELECT propertycode, operation, fotocasa_specific
            FROM async_properties
            WHERE source = 'fotocasa' AND adisactive = true
            AND {tier['where_clause']}
            AND last_checked_at < $1
            ORDER BY last_checked_at ASC
            LIMIT {fetch_size}
        """
        try:
            rows = await db_pool.fetch(query_old, cutoff)
            if rows:
                all_props = [
                    {
                        "propertycode": r["propertycode"],
                        "operation": r["operation"],
                        "fotocasa_specific": (
                            json.loads(r["fotocasa_specific"])
                            if r["fotocasa_specific"] and isinstance(r["fotocasa_specific"], str)
                            else r["fotocasa_specific"]
                        ),
                    }
                    for r in rows if r["propertycode"] not in inflight
                ]
                if all_props:
                    return all_props[:batch_size], tier["name"]
        except Exception as e:
            logger.warning(f"[fc-check-active] SQL error (old) tier {tier['name']}: {e}")
            continue

    return [], None

async def _get_fc_check_active_batch(batch_size=50):
    """
    Obtiene el siguiente batch de propiedades Fotocasa activas a verificar.
    Usa asyncpg directo. Incluye operation y fotocasa_specific para enriquecer.
    Prioriza por tier: capitales > provincias principales > resto.
    Returns: (properties_list, tier_name) o ([], None) si todo al día.
    """
    now_utc = datetime.now(timezone.utc)

    for tier in FC_CHECK_ACTIVE_TIERS:
        cutoff = now_utc - timedelta(hours=tier["cycle_hours"])

        # Query 1: propiedades nunca verificadas (NULL)
        query_null = f"""
            SELECT propertycode, operation, fotocasa_specific
            FROM async_properties
            WHERE source = 'fotocasa' AND adisactive = true
            AND {tier['where_clause']}
            AND last_checked_at IS NULL
            LIMIT {batch_size}
        """
        try:
            rows = await db_pool.fetch(query_null)
            if rows:
                return [
                    {
                        "propertycode": r["propertycode"],
                        "operation": r["operation"],
                        "fotocasa_specific": (
                            json.loads(r["fotocasa_specific"])
                            if r["fotocasa_specific"] and isinstance(r["fotocasa_specific"], str)
                            else r["fotocasa_specific"]
                        ),
                    }
                    for r in rows
                ], tier["name"]
        except Exception as e:
            logger.warning(f"[fc-check-active] SQL error (null) tier {tier['name']}: {e}")
            continue

        # Query 2: verificadas hace más del ciclo
        query_old = f"""
            SELECT propertycode, operation, fotocasa_specific
            FROM async_properties
            WHERE source = 'fotocasa' AND adisactive = true
            AND {tier['where_clause']}
            AND last_checked_at < $1
            ORDER BY last_checked_at ASC
            LIMIT {batch_size}
        """
        try:
            rows = await db_pool.fetch(query_old, cutoff)
            if rows:
                return [
                    {
                        "propertycode": r["propertycode"],
                        "operation": r["operation"],
                        "fotocasa_specific": (
                            json.loads(r["fotocasa_specific"])
                            if r["fotocasa_specific"] and isinstance(r["fotocasa_specific"], str)
                            else r["fotocasa_specific"]
                        ),
                    }
                    for r in rows
                ], tier["name"]
        except Exception as e:
            logger.warning(f"[fc-check-active] SQL error (old) tier {tier['name']}: {e}")
            continue

    return [], None


async def _fc_check_active_worker(worker_id: int, slot_id: int, tier_indices: list = None):
    """
    Worker que verifica propiedades Fotocasa continuamente.
    Usa token Hetzner slot dedicado + rotacion de proxy cada 20 requests.
    Auto-reinicio si crashea. Para cuando fc_check_active_state["running"] = False.
    """
    cfg = FC_CHECK_ACTIVE_CONFIG
    logger.info(f"[fc-check-active-w{worker_id}] Worker starting (token slot={slot_id})")

    # --- Proxy: empezar con regular, fallback a premium ---
    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
    _consecutive_proxy_errors = 0
    _fallback_at = 0

    def _create_fc_http_client():
        """Crea nuevo httpx client con proxy activo (nueva IP)."""
        kw = {"timeout": 30}
        if _active_proxy:
            kw["proxies"] = _active_proxy
        hc = httpx.AsyncClient(**kw)
        fc = Fotocasa(HttpxClient(hc))
        _randomize_fc_device(fc, token_cache=_fc_token_slots[slot_id])
        return hc, HttpxClient(hc), fc

    async def _init_fc_session(fotocasa, label=""):
        """Inicializa sesión Fotocasa para obtener CSRF token."""
        try:
            init_ok, _ = await fotocasa.initialize_session()
            if init_ok:
                logger.info(f"[fc-check-active-w{worker_id}] Session initialized OK{label}")
            else:
                logger.warning(f"[fc-check-active-w{worker_id}] Session init failed{label}")
        except Exception as e:
            logger.warning(f"[fc-check-active-w{worker_id}] Session init error{label}: {e}")

    while fc_check_active_state["running"]:
        try:
            # --- Rebounce ---
            if _proxy_label == "premium" and _fallback_at and PROXY_URL_REGULAR:
                if time.time() - _fallback_at > PROXY_REBOUNCE_SECONDS:
                    _active_proxy, _proxy_label = _proxy_select(prefer_regular=True)
                    _consecutive_proxy_errors = 0
                    _fallback_at = 0
                    logger.info(f"[fc-check-active-w{worker_id}] PROXY REBOUNCE: premium → regular")

            headers_rest = _supabase_headers()
            http_client, httpx_client, fotocasa = _create_fc_http_client()
            db_client = httpx.AsyncClient(timeout=30)  # Sin proxy — para Supabase REST
            await _init_fc_session(fotocasa, " (startup)")  # FIX: obtener CSRF token
            proxy_req_count = 0

            while fc_check_active_state["running"]:
                    # Obtener token Hetzner slot dedicado
                    token = await _get_fc_hetzner_token(slot_id=slot_id)
                    if not token:
                        logger.warning(f"[fc-check-active-w{worker_id}] No Hetzner token (slot {slot_id}), sleeping 60s")
                        await asyncio.sleep(60)
                        continue

                    # Usar tiers dedicados si se asignaron, si no buscar en todos
                    if tier_indices is not None:
                        batch, tier_name = await _get_fc_check_active_batch_for_tiers(tier_indices, cfg["batch_size"])
                    else:
                        batch, tier_name = await _get_fc_check_active_batch(cfg["batch_size"])

                    if not batch:
                        # Work-stealing: intentar otros tiers antes de dormir
                        if tier_indices is not None:
                            for other_idx in range(len(FC_CHECK_ACTIVE_TIERS)):
                                if other_idx in tier_indices:
                                    continue
                                batch, tier_name = await _get_fc_check_active_batch_for_tiers(
                                    [other_idx], cfg["batch_size"]
                                )
                                if batch:
                                    logger.info(
                                        f"[fc-check-active-w{worker_id}] Work-stealing: "
                                        f"helping tier {tier_name}"
                                    )
                                    break

                    if not batch:
                        fc_check_active_state["workers"][worker_id]["status"] = "idle"
                        fc_check_active_state["workers"][worker_id]["_last_idle_at"] = time.time()
                        logger.info(
                            f"[fc-check-active-w{worker_id}] All tiers up to date, "
                            f"sleeping {cfg['sleep_when_idle']}s"
                        )
                        await asyncio.sleep(cfg["sleep_when_idle"])
                        continue

                    fc_check_active_state["workers"][worker_id]["status"] = f"checking:{tier_name}"
                    fc_check_active_state["workers"][worker_id]["current_tier"] = tier_name
                    # Accumulate idle time
                    _idle_at = fc_check_active_state["workers"][worker_id].get("_last_idle_at")
                    if _idle_at:
                        fc_check_active_state["workers"][worker_id]["idle_total_s"] = (
                            fc_check_active_state["workers"][worker_id].get("idle_total_s", 0) + (time.time() - _idle_at)
                        )
                        fc_check_active_state["workers"][worker_id]["_last_idle_at"] = None
                    batch_start = time.time()

                    # Track in-flight to prevent duplicate work
                    batch_codes = {p["propertycode"] for p in batch}
                    _fc_ca_inflight.update(batch_codes)

                    to_deactivate = []
                    to_update_active = []
                    batch_errors = 0
                    ip_success_count = 0  # Requests exitosas con esta IP
                    _circuit_break = False  # Circuit breaker: 403 con 0 OK reqs

                    for prop in batch:
                        if not fc_check_active_state["running"]:
                            break

                        property_code = prop["propertycode"]
                        operation = prop.get("operation", "sale")
                        fc_specific = prop.get("fotocasa_specific") or {}

                        # Delay adaptativo: semáforo compartido ajusta velocidad según 403s
                        t_min, t_max = _fc_throttle_get_delay()
                        await asyncio.sleep(random.uniform(t_min, t_max))

                        # Rotar proxy cada N requests (nueva IP preventiva)
                        proxy_req_count += 1
                        if proxy_req_count % FC_PROXY_ROTATE_EVERY == 0:
                            logger.info(
                                f"[fc-check-active-w{worker_id}] Proxy rotated (preventive) "
                                f"after {ip_success_count} OK reqs"
                            )
                            await http_client.aclose()
                            http_client, httpx_client, fotocasa = _create_fc_http_client()
                            await _init_fc_session(fotocasa, " (proxy rotate)")  # FIX: CSRF
                            ip_success_count = 0

                        # Mapear operation a transaction_type Fotocasa
                        tx_type = "RENT" if operation == "rent" else "SALE"
                        status, detail_data = await _fc_verify_is_active(
                            fotocasa, property_code, token, transaction_type=tx_type
                        )

                        if status == "active":
                            enrichment = transform_fotocasa_detail_to_enrichment(
                                detail_data, existing_fotocasa_specific=fc_specific,
                            )
                            enrichment["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                            enrichment["adisactive"] = True
                            to_update_active.append((property_code, enrichment))
                            ip_success_count += 1
                            _fc_throttle_record("ok")
                        elif status == "inactive":
                            to_deactivate.append(property_code)
                            ip_success_count += 1
                            _fc_throttle_record("ok")
                        elif status == "token_expired":
                            # 403 = casi siempre rate-limit de IP, NO token expirado
                            _fc_throttle_record("403")
                            _audit_err("fc-check-active", "HTTP 403")
                            t_level = _fc_throttle["current_level"]
                            had_ok = ip_success_count  # Guardar antes de reset
                            logger.info(
                                f"[fc-check-active-w{worker_id}] 403 → rotating proxy + refreshing token "
                                f"(had {ip_success_count} OK reqs with current IP) "
                                f"[throttle={t_level}]"
                            )
                            await http_client.aclose()
                            http_client, httpx_client, fotocasa = _create_fc_http_client()
                            await _init_fc_session(fotocasa, " (403 proxy rotate)")
                            proxy_req_count = 0
                            ip_success_count = 0
                            # Siempre refrescar token en 403 — FC rate-limita por token + IP
                            token = await _get_fc_hetzner_token(slot_id=slot_id, force_refresh=True)
                            if not token:
                                logger.warning(f"[fc-check-active-w{worker_id}] No token after refresh, backoff 30s")
                                await asyncio.sleep(30)
                                token = await _get_fc_hetzner_token(slot_id=slot_id)
                            batch_errors += 1
                            # Circuit breaker: si 0 OK reqs con IP fresca, FC bloqueó
                            # la combo — no seguir quemando props, cortar batch y backoff
                            if had_ok == 0:
                                logger.warning(
                                    f"[fc-check-active-w{worker_id}] CIRCUIT BREAK: "
                                    f"0 OK reqs with fresh IP+token, aborting batch, backoff 35s"
                                )
                                _circuit_break = True
                                break
                            # Pausa adaptativa post-403 (solo si no circuit break)
                            post_403_delay = {"fast": 3, "cautious": 5, "slow": 10}[t_level]
                            await asyncio.sleep(post_403_delay)
                        else:
                            batch_errors += 1
                            _audit_err("fc-check-active", "HTTP other")

                    # --- UPDATEs via REST (single-row PATCHes, igual que Idealista) ---

                    for code, enrichment in to_update_active:
                        try:
                            url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{code}"
                            await db_client.patch(url, headers=headers_rest, json=enrichment, timeout=30)
                        except Exception as e:
                            logger.warning(f"[fc-check-active-w{worker_id}] DB error {code}: {e}")
                            batch_errors += 1
                            _audit_err("fc-check-active", "DB error", str(e)[:80])

                    if to_deactivate:
                        deactivate_data = {
                            "adisactive": False,
                            "recheck_count": 0,
                            "last_checked_at": datetime.now(timezone.utc).isoformat(),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }
                        codes_csv = ",".join(to_deactivate)
                        url = f"{SUPABASE_URL}/async_properties?propertycode=in.({codes_csv})"
                        try:
                            await db_client.patch(url, headers=headers_rest, json=deactivate_data, timeout=30)
                        except Exception as e:
                            logger.error(f"[fc-check-active-w{worker_id}] Batch deactivate error: {e}")

                    # Contadores
                    num_active = len(to_update_active)
                    num_deactivated = len(to_deactivate)
                    batch_checked = num_active + num_deactivated + batch_errors
                    fc_check_active_state["totals"]["checked"] += batch_checked
                    fc_check_active_state["totals"]["still_active"] += num_active
                    fc_check_active_state["totals"]["deactivated"] += num_deactivated
                    fc_check_active_state["totals"]["errors"] += batch_errors
                    # Per-tier
                    bt = fc_check_active_state["totals"]["by_tier"]
                    if tier_name not in bt:
                        bt[tier_name] = {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0}
                    bt[tier_name]["checked"] += batch_checked
                    bt[tier_name]["still_active"] += num_active
                    bt[tier_name]["deactivated"] += num_deactivated
                    bt[tier_name]["errors"] += batch_errors
                    # Per-worker
                    w_state = fc_check_active_state["workers"][worker_id]
                    w_state["checked"] = w_state.get("checked", 0) + batch_checked
                    w_state["errors"] = w_state.get("errors", 0) + batch_errors
                    w_state["batches_done"] = w_state.get("batches_done", 0) + 1
                    w_state["last_batch_at"] = datetime.now(timezone.utc).isoformat()

                    # Release in-flight codes
                    _fc_ca_inflight.difference_update(batch_codes)

                    batch_time = time.time() - batch_start
                    logger.info(
                        f"[fc-check-active-w{worker_id}] Batch ({tier_name}): "
                        f"{num_active}A {num_deactivated}D {batch_errors}E "
                        f"({batch_time:.0f}s) | Total: {fc_check_active_state['totals']['checked']}"
                    )

                    # Circuit breaker backoff: esperar a que pase el cooldown del token
                    if _circuit_break:
                        await asyncio.sleep(35)
                        continue

                    # Alerta Slack si error rate alto
                    if batch_errors > len(batch) * 0.5 and len(batch) >= 30:
                        logger.warning(
                            f"[fc-check-active-w{worker_id}] High error rate, "
                            f"sleeping {cfg['error_sleep']}s"
                        )
                        await _send_slack_worker_alert(
                            emoji="\u26a0\ufe0f",
                            title=f"[FOTOCASA] Check Active w{worker_id} — Error rate alto",
                            fields={
                                "Worker": f"fc-w{worker_id}",
                                "Tier": tier_name,
                                "Batch": (
                                    f"{num_active}A {num_deactivated}D {batch_errors}E "
                                    f"/ {len(batch)} total"
                                ),
                                "Error rate": f"{batch_errors/len(batch)*100:.0f}%",
                            },
                            category="deactivation",
                            is_alert=True,
                        )
                        await asyncio.sleep(cfg["error_sleep"])

                    # --- Proxy fallback ---
                    if _proxy_label == "regular" and len(batch) >= 10:
                        if batch_errors > len(batch) * 0.3:
                            _consecutive_proxy_errors += 1
                            if _consecutive_proxy_errors >= 2:
                                _active_proxy, _proxy_label = _proxy_select(prefer_regular=False)
                                _consecutive_proxy_errors = 0
                                _fallback_at = time.time()
                                logger.warning(f"[fc-check-active-w{worker_id}] PROXY FALLBACK: regular → premium")
                                await _send_slack_worker_alert(
                                    "\U0001f504", f"[FOTOCASA] Check Active w{worker_id} — Proxy fallback a PREMIUM",
                                    {"Motivo": "2 batches >30% errors"},
                                    category="deactivation", is_alert=True,
                                )
                                break
                        else:
                            _consecutive_proxy_errors = 0

        except Exception as e:
            # Release any in-flight codes on crash
            try:
                _fc_ca_inflight.difference_update(batch_codes)
            except Exception:
                pass
            logger.error(f"[fc-check-active-w{worker_id}] Worker crashed: {e}", exc_info=True)
            fc_check_active_state["workers"][worker_id]["status"] = "crashed"
            _audit_err("fc-check-active", "CRASH", str(e)[:120])
            try:
                await http_client.aclose()
            except Exception:
                pass
            try:
                await db_client.aclose()
            except Exception:
                pass
            await _send_slack_worker_alert(
                emoji="\U0001f6a8",
                title=f"[FOTOCASA] Check Active w{worker_id} — CRASH",
                fields={
                    "Worker": f"fc-w{worker_id}",
                    "Error": str(e)[:200],
                    "Accion": f"Reiniciando en {cfg['restart_delay']}s",
                    "Total checked": str(fc_check_active_state["totals"]["checked"]),
                },
                category="deactivation",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info(f"[fc-check-active-w{worker_id}] Restarting worker...")

    logger.info(f"[fc-check-active-w{worker_id}] Worker stopped")


async def _start_fc_check_active():
    """Lanza los workers de Fotocasa Check Active — dedicados por tier (16 slots Hetzner).

    Distribución:
      slots 4-6        → capitals (3 workers)
      slots 7-9, 13-14 → main_provinces (5 workers)
      slots 10         → second_capitals (1 worker)
      slots 11, 15     → rest (2 workers)
    """
    fc_check_active_state["running"] = True
    fc_check_active_state["started_at"] = datetime.now(timezone.utc).isoformat()
    fc_check_active_state["totals"] = {"checked": 0, "still_active": 0, "deactivated": 0, "errors": 0, "by_tier": {}}
    fc_check_active_state["last_summary_at"] = time.time()

    workers_per_tier = FC_CHECK_ACTIVE_CONFIG["workers_per_tier"]
    # Slots explícitos por tier (no contiguos para main_provinces)
    tier_slots = [
        [4, 5, 6],          # capitals
        [7, 8, 9, 13, 14],  # main_provinces
        [10],                # second_capitals
        [11, 15],            # rest
    ]
    worker_id = 0
    for tier_index, tier in enumerate(FC_CHECK_ACTIVE_TIERS):
        num_workers = workers_per_tier[tier_index] if tier_index < len(workers_per_tier) else 1
        slots = tier_slots[tier_index]
        for w in range(num_workers):
            slot = slots[w]
            fc_check_active_state["workers"][worker_id] = {
                "status": "starting", "current_tier": tier["name"],
                "batches_done": 0, "last_batch_at": None,
                "slot_id": slot,
                "checked": 0, "errors": 0,
                "idle_total_s": 0, "_last_idle_at": None,
            }
            asyncio.create_task(
                _fc_check_active_worker(worker_id, slot_id=slot, tier_indices=[tier_index])
            )
            worker_id += 1

    total_workers = sum(workers_per_tier)
    logger.info(
        f"[fc-check-active] Started {total_workers} workers "
        f"(per tier: {workers_per_tier}, slots 4-15)"
    )

def _create_b2_client():
    """Crea cliente boto3 para Backblaze B2 via S3-compatible API."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_KEY_ID,
        aws_secret_access_key=B2_APP_KEY,
    )


def _upload_to_b2(s3_client, data, key, content_type="image/webp"):
    """Sube un archivo a B2. Retorna True si OK."""
    try:
        s3_client.put_object(
            Bucket=B2_BUCKET_NAME,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        return True
    except Exception as e:
        logger.error(f"[mirror] B2 upload error {key}: {e}")
        return False


async def _mirror_download_image(http, url, semaphore):
    """Descarga una imagen. Retorna bytes o None."""
    async with semaphore:
        try:
            resp = await http.get(url, timeout=30.0)
            return resp.content if resp.status_code == 200 else None
        except Exception:
            return None


async def _mirror_process_property(http, s3_client, propertycode, multimedia, semaphore):
    """
    Procesa una propiedad: descarga imgs → sube a B2 → retorna nuevo multimedia.
    Retorna (n_total, n_uploaded, new_multimedia).
    """
    images = multimedia.get("images", [])
    if not images:
        return 0, 0, multimedia

    total = len(images)
    new_images = []
    uploaded = 0

    # Descargar todas en paralelo
    download_tasks = []
    for img in images:
        url = img.get("url", "")
        if url:
            download_tasks.append(_mirror_download_image(http, url, semaphore))
        else:
            async def _noop():
                return None
            download_tasks.append(_noop())

    results = await asyncio.gather(*download_tasks, return_exceptions=True)

    loop = asyncio.get_event_loop()

    for idx, (img, data) in enumerate(zip(images, results)):
        original_url = img.get("url", "")

        if data is None or isinstance(data, Exception):
            new_images.append(img)  # Mantener URL original
            continue

        # Determinar extensión
        ext = "webp"
        if ".jpg" in original_url or ".jpeg" in original_url:
            ext = "jpg"
        elif ".png" in original_url:
            ext = "png"

        content_type = {"webp": "image/webp", "jpg": "image/jpeg", "png": "image/png"}.get(ext, "image/webp")
        b2_key = f"idealista/{propertycode}/{idx}.{ext}"

        success = await loop.run_in_executor(
            None, _upload_to_b2, s3_client, data, b2_key, content_type
        )

        if success:
            cdn_url = f"{CDN_BASE_URL}/idealista/{propertycode}/{idx}.{ext}"
            new_img = dict(img)
            new_img["url"] = cdn_url
            new_img["original_url"] = original_url
            new_images.append(new_img)
            uploaded += 1
        else:
            new_images.append(img)

    new_multimedia = dict(multimedia)
    new_multimedia["images"] = new_images
    return total, uploaded, new_multimedia


async def _mirror_worker():
    """
    Worker que migra imágenes pendientes de Idealista a B2/CDN.
    Se ejecuta hasta que no quedan pendientes, luego se detiene.
    """
    cfg = MIRROR_CONFIG
    logger.info("[mirror] Worker starting")

    if not all([B2_KEY_ID, B2_APP_KEY, B2_BUCKET_NAME, B2_ENDPOINT, CDN_BASE_URL]):
        logger.error("[mirror] Missing B2/CDN env vars, cannot start")
        mirror_state["status"] = "error: missing env vars"
        mirror_state["running"] = False
        return

    try:
        s3_client = _create_b2_client()
    except Exception as e:
        logger.error(f"[mirror] Failed to create B2 client: {e}")
        mirror_state["status"] = "error: B2 client"
        mirror_state["running"] = False
        return

    semaphore = asyncio.Semaphore(cfg["concurrent"])

    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36",
        "Accept": "image/webp,image/*,*/*",
    }

    async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as http:
        while mirror_state["running"]:
            try:
                # Fetch batch de propiedades pendientes via asyncpg
                rows = await db_pool.fetch("""
                    SELECT propertycode, multimedia
                    FROM async_properties
                    WHERE source = 'idealista'
                      AND adisactive = true
                      AND images_mirrored_at IS NULL
                      AND multimedia IS NOT NULL
                    LIMIT $1
                """, cfg["batch_size"])

                if not rows:
                    logger.info(f"[mirror] No pending properties, worker stopping")
                    break

                mirror_state["status"] = f"processing ({len(rows)} props)"
                batch_uploaded = 0
                batch_errors = 0

                # Procesar todo el batch en paralelo
                async def _process_one(row):
                    pcode = row["propertycode"]
                    multimedia_raw = row["multimedia"]

                    # Parsear multimedia (puede ser str o dict)
                    if isinstance(multimedia_raw, str):
                        try:
                            multimedia = json.loads(multimedia_raw)
                        except json.JSONDecodeError:
                            return pcode, 0, 0, None
                    elif isinstance(multimedia_raw, dict):
                        multimedia = multimedia_raw
                    else:
                        return pcode, 0, 0, None

                    n_total, n_uploaded, new_multimedia = await _mirror_process_property(
                        http, s3_client, pcode, multimedia, semaphore
                    )
                    return pcode, n_total, n_uploaded, new_multimedia

                results = await asyncio.gather(
                    *[_process_one(r) for r in rows],
                    return_exceptions=True,
                )

                # Escribir resultados a DB
                for row, r in zip(rows, results):
                    if isinstance(r, Exception):
                        pcode = row["propertycode"]
                        logger.error(f"[mirror] Property {pcode} error: {r}")
                        batch_errors += 1
                        # Mark as processed to avoid infinite loop
                        try:
                            await db_pool.execute("""
                                UPDATE async_properties
                                SET images_mirrored_at = $1
                                WHERE propertycode = $2
                            """, datetime.now(timezone.utc), pcode)
                            logger.warning(f"[mirror] Skipped {pcode}: exception during processing, marked as mirrored")
                        except Exception as e2:
                            logger.warning(f"[mirror] DB skip-update error {pcode}: {e2}")
                        continue

                    pcode, n_total, n_uploaded, new_multimedia = r

                    if new_multimedia is not None:
                        try:
                            await db_pool.execute("""
                                UPDATE async_properties
                                SET multimedia = $1,
                                    images_mirrored_at = $2
                                WHERE propertycode = $3
                            """, json.dumps(new_multimedia),
                                datetime.now(timezone.utc),
                                pcode,
                            )
                            batch_uploaded += n_uploaded
                        except Exception as e:
                            logger.warning(f"[mirror] DB update error {pcode}: {e}")
                            batch_errors += 1
                    else:
                        # Multimedia unparseable — mark as processed to avoid infinite loop
                        try:
                            await db_pool.execute("""
                                UPDATE async_properties
                                SET images_mirrored_at = $1
                                WHERE propertycode = $2
                            """, datetime.now(timezone.utc), pcode)
                            logger.warning(f"[mirror] Skipped {pcode}: multimedia unparseable, marked as mirrored")
                        except Exception as e:
                            logger.warning(f"[mirror] DB skip-update error {pcode}: {e}")
                            batch_errors += 1

                mirror_state["totals"]["processed"] += len(rows)
                mirror_state["totals"]["images_uploaded"] += batch_uploaded
                mirror_state["totals"]["errors"] += batch_errors

                logger.info(
                    f"[mirror] Batch: {len(rows)} props, {batch_uploaded} imgs uploaded, "
                    f"{batch_errors} errors | Total: {mirror_state['totals']['images_uploaded']} imgs"
                )

                # Pausa entre batches
                await asyncio.sleep(cfg["delay_between_batches"])
                if batch_errors > len(rows) * 0.5 and len(rows) >= 10:
                    await _send_slack_worker_alert(
                        emoji="⚠️",
                        title="[MIRROR] Error rate alto",
                        fields={
                            "Batch": f"{batch_uploaded} imgs OK, {batch_errors} errors / {len(rows)} props",
                            "Total imgs": str(mirror_state["totals"]["images_uploaded"]),
                        },
                        category="mirror",
                        is_alert=True,
                    )

            except Exception as e:
                logger.error(f"[mirror] Worker error: {e}", exc_info=True)
                await _send_slack_worker_alert(
                    emoji="🚨",
                    title="[MIRROR] CRASH",
                    fields={
                        "Error": str(e)[:200],
                        "Procesadas": str(mirror_state["totals"]["processed"]),
                        "Imgs subidas": str(mirror_state["totals"]["images_uploaded"]),
                    },
                    category="mirror",
                    is_alert=True,
                )
                await asyncio.sleep(30)

    mirror_state["running"] = False
    mirror_state["status"] = "stopped"
    logger.info(
        f"[mirror] Worker finished — {mirror_state['totals']['processed']} props, "
        f"{mirror_state['totals']['images_uploaded']} imgs"
    )


def _trigger_mirror():
    """
    Lanza el mirror worker si no está corriendo.
    Llamar desde scrape-updates cuando inserted > 0.
    """
    if mirror_state["running"]:
        logger.debug("[mirror] Already running, skipping trigger")
        return False

    mirror_state["running"] = True
    mirror_state["started_at"] = datetime.now(timezone.utc).isoformat()
    mirror_state["totals"] = {"processed": 0, "images_uploaded": 0, "errors": 0}
    mirror_state["status"] = "starting"
    asyncio.create_task(_mirror_worker())
    logger.info("[mirror] Worker triggered by scrape-updates")
    return True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IDEALISTA RECHECK — Worker continuo 24/7
#  Usa TLS client + proxy + JWT. Session rotation cada 30 requests.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _id_recheck_continuous_worker(worker_id=0, total_workers=1):
    """
    Worker continuo de recheck Idealista.
    Usa TLS client con proxy rotation + JWT refresh.
    Velocidad constante 24/7 (0.3-0.5s delay).
    worker_id: para particionado de batches entre N workers.
    """
    cfg = RECHECK_CONFIG
    wlabel = f"id-recheck-w{worker_id}"
    per_worker_batch = max(100, cfg["batch_size"] // total_workers)
    offset = worker_id * per_worker_batch
    logger.info(f"[{wlabel}] Continuous worker starting (batch={per_worker_batch}, offset={offset})")

    # — Proxy: regular con fallback a premium —
    _proxy_regular = PROXY_URL_REGULAR
    _proxy_premium = PROXY_URL_PREMIUM
    _using_regular = bool(_proxy_regular)
    _consecutive_high_errors = 0
    _fallback_at = 0
    if _using_regular:
        logger.info(f"[{wlabel}] Starting with REGULAR proxy")
    else:
        logger.info(f"[{wlabel}] No PROXY_URL_REGULAR set, using premium proxy")

    while recheck_state["running"]:
        try:
            # --- Rebounce: si llevamos 1h en premium, probar regular ---
            if not _using_regular and _fallback_at and _proxy_regular:
                if time.time() - _fallback_at > PROXY_REBOUNCE_SECONDS:
                    _using_regular = True
                    _consecutive_high_errors = 0
                    _fallback_at = 0
                    logger.info(f"[{wlabel}] PROXY REBOUNCE: premium → regular")

            # Obtener JWT (usa premium internamente, es solo para el token)
            idealista, _, _ = await _get_idealista_client()
            jwt = idealista.jwt_token
            # Recrear con el proxy seleccionado para este ciclo
            active_proxy = _proxy_regular if _using_regular else _proxy_premium
            proxy_label = "regular" if _using_regular else "premium"
            proxy_url = active_proxy
            tls_impl = TlsClientImpl(proxy_url=proxy_url)
            idealista = Idealista(tls_impl)
            idealista.jwt_token = jwt
            logger.info(f"[id-recheck] Cycle starting with {proxy_label} proxy")
            headers_rest = _supabase_headers()
            request_count = 0

            async with httpx.AsyncClient() as client:
                while recheck_state["running"]:
                    # Refrescar JWT si necesario
                    await _refresh_jwt_if_needed(idealista)

                    # Obtener batch de las ventanas de recheck (time-gated)
                    # Check 1: después de 6h    | Check 2: después de 2d
                    # Check 3: después de 5d    | Check 4: después de 14d
                    # Check 5: después de 22d   | Check 6: después de 30d
                    # Mensual: 1 check cada 30 días adicionales
                    now_utc = datetime.now(timezone.utc)
                    windows = [
                        ("check1_6h+", "updated_at <= $1 AND recheck_count < 1",
                         [now_utc - timedelta(hours=6)]),
                        ("check2_2d+", "updated_at <= $1 AND recheck_count < 2",
                         [now_utc - timedelta(days=2)]),
                        ("check3_5d+", "updated_at <= $1 AND recheck_count < 3",
                         [now_utc - timedelta(days=5)]),
                        ("check4_14d+", "updated_at <= $1 AND recheck_count < 4",
                         [now_utc - timedelta(days=14)]),
                        ("check5_22d+", "updated_at <= $1 AND recheck_count < 5",
                         [now_utc - timedelta(days=22)]),
                        ("check6_30d+", "updated_at <= $1 AND recheck_count < 6",
                         [now_utc - timedelta(days=30)]),
                        ("monthly_60d+",
                         "updated_at <= $1 AND recheck_count < (6 + FLOOR(EXTRACT(EPOCH FROM ($2 - updated_at)) / 2592000))",
                         [now_utc - timedelta(days=60), now_utc]),
                    ]

                    inactive_properties = []
                    try:
                        for win_name, where, params in windows:
                            remaining = per_worker_batch - len(inactive_properties)
                            if remaining <= 0:
                                break
                            rows = await db_pool.fetch(f"""
                                SELECT propertycode, recheck_count FROM async_properties
                                WHERE source = 'idealista' AND adisactive = false
                                AND {where}
                                ORDER BY updated_at DESC
                                LIMIT {remaining} OFFSET {offset}
                            """, *params)
                            for r in rows:
                                inactive_properties.append({
                                    "propertycode": r["propertycode"],
                                    "recheck_count": r["recheck_count"] or 0,
                                })
                            if rows:
                                logger.info(f"[{wlabel}] Window {win_name}: {len(rows)} properties")
                    except Exception as e:
                        logger.warning(f"[id-recheck] SQL error: {e}")
                        await asyncio.sleep(cfg["restart_delay"])
                        continue

                    if not inactive_properties:
                        logger.info(f"[id-recheck] No pending properties, sleeping {cfg['sleep_when_idle']}s")
                        await asyncio.sleep(cfg["sleep_when_idle"])
                        continue

                    logger.info(f"[id-recheck] {len(inactive_properties)} to check (delay {cfg['delay_min']}-{cfg['delay_max']}s)")

                    # Procesar batch
                    start_time = time.time()
                    results = {"reactivated": 0, "confirmed_inactive": 0, "errors": 0, "proxy_errors": 0, "error_samples": []}

                    for i, prop in enumerate(inactive_properties):
                        if not recheck_state["running"]:
                            break

                        # Refrescar JWT cada iteración (por si el batch es largo)
                        await _refresh_jwt_if_needed(idealista)

                        # Rotar sesión TLS cada N requests
                        request_count += 1
                        if request_count % cfg["session_rotate_every"] == 0:
                            tls_impl = TlsClientImpl(proxy_url=proxy_url)
                            idealista = Idealista(tls_impl)
                            idealista.jwt_token = jwt_token_cache["token"]

                        property_code = prop["propertycode"]
                        current_recheck_count = prop["recheck_count"]

                        await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))

                        try:
                            status, detail_data = await _verify_is_active(idealista, property_code)

                            if status == "active":
                                enrichment = transform_detail_to_enrichment(detail_data)
                                enrichment["adisactive"] = True
                                enrichment["recheck_count"] = 0
                                enrichment["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                await client.patch(url, headers=headers_rest, json=enrichment, timeout=30)
                                results["reactivated"] += 1
                                logger.info(f"[id-recheck] REACTIVATED {property_code}")

                            elif status == "inactive":
                                confirm_data = {
                                    "recheck_count": current_recheck_count + 1,
                                    "last_checked_at": datetime.now(timezone.utc).isoformat(),
                                }
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                await client.patch(url, headers=headers_rest, json=confirm_data, timeout=30)
                                results["confirmed_inactive"] += 1

                            else:
                                err_code = detail_data.get("status_code", "?") if isinstance(detail_data, dict) else "?"

                                # Si 407 (proxy auth fail), rotar sesión TLS y reintentar 1 vez
                                if err_code == 407:
                                    tls_impl = TlsClientImpl(proxy_url=proxy_url)
                                    idealista = Idealista(tls_impl)
                                    idealista.jwt_token = jwt_token_cache["token"]
                                    request_count = 0
                                    await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))

                                    retry_status, retry_data = await _verify_is_active(idealista, property_code)
                                    if retry_status == "active":
                                        enrichment = transform_detail_to_enrichment(retry_data)
                                        enrichment["adisactive"] = True
                                        enrichment["recheck_count"] = 0
                                        enrichment["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                                        url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                        await client.patch(url, headers=headers_rest, json=enrichment, timeout=30)
                                        results["reactivated"] += 1
                                        logger.info(f"[id-recheck] REACTIVATED {property_code} (407 retry)")
                                    elif retry_status == "inactive":
                                        confirm_data = {
                                            "recheck_count": current_recheck_count + 1,
                                            "last_checked_at": datetime.now(timezone.utc).isoformat(),
                                        }
                                        url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                        await client.patch(url, headers=headers_rest, json=confirm_data, timeout=30)
                                        results["confirmed_inactive"] += 1
                                    else:
                                        results["errors"] += 1
                                        results["proxy_errors"] += 1
                                        if len(results["error_samples"]) < 10:
                                            results["error_samples"].append({"code": property_code, "err": f"HTTP {err_code} (retry failed)"})
                                else:
                                    results["errors"] += 1
                                    # Solo contar HTTP 0/407 como errores de proxy
                                    if err_code in (0, 407):
                                        results["proxy_errors"] += 1
                                    if err_code == 401:
                                        # JWT expirado/revocado — forzar refresh
                                        logger.info(f"[{wlabel}] 401 → refreshing JWT")
                                        jwt_token_cache["expires_at"] = 0
                                        await _refresh_jwt_if_needed(idealista)
                                        idealista.jwt_token = jwt_token_cache["token"]
                                    if len(results["error_samples"]) < 10:
                                        results["error_samples"].append({"code": property_code, "err": f"HTTP {err_code}"})

                        except Exception as e:
                            results["errors"] += 1
                            if len(results["error_samples"]) < 10:
                                results["error_samples"].append({"code": property_code, "err": str(e)[:80]})

                        if (i + 1) % 100 == 0:
                            elapsed = time.time() - start_time
                            logger.info(
                                f"[id-recheck] {i+1}/{len(inactive_properties)} "
                                f"({results['reactivated']}R {results['confirmed_inactive']}C "
                                f"{results['errors']}E) {elapsed:.0f}s"
                            )

                    # Actualizar totales acumulados
                    total_time = time.time() - start_time
                    recheck_state["totals"]["processed"]          += len(inactive_properties)
                    recheck_state["totals"]["reactivated"]        += results["reactivated"]
                    recheck_state["totals"]["confirmed_inactive"] += results["confirmed_inactive"]
                    recheck_state["totals"]["errors"]             += results["errors"]
                    recheck_state["totals"]["batches"]            += 1
                    recheck_state["last_batch_at"] = datetime.now(timezone.utc).isoformat()

                    logger.info(
                        f"[id-recheck] Batch done: {len(inactive_properties)} checked, "
                        f"{results['reactivated']}R {results['confirmed_inactive']}C "
                        f"{results['errors']}E, {total_time:.1f}s"
                    )

                    # Slack solo si error rate alto (>10%)
                    error_rate = results["errors"] / max(len(inactive_properties), 1)
                    if error_rate > 0.25:
                        batch_id = f"id-recheck-{int(time.time())}"
                        error_summary = ", ".join([s["err"] for s in results["error_samples"][:10]]) or "ninguno"
                        await _send_slack_worker_alert(
                            emoji="\u26a0\ufe0f",
                            title=f"[IDEALISTA] Recheck — Error rate alto ({error_rate:.0%})",
                            fields={
                                "Procesadas": str(len(inactive_properties)),
                                "Errores": str(results["errors"]),
                                "Error rate": f"{error_rate:.1%}",
                                "Error samples": error_summary,
                            },
                            category="recheck",
                            is_alert=True,
                        )

                     # — Proxy fallback: solo errores de proxy (0/407), no JWT (401) —
                    proxy_error_rate = results["proxy_errors"] / max(len(inactive_properties), 1)
                    if _using_regular:
                        if proxy_error_rate > 0.30:
                            _consecutive_high_errors += 1
                            logger.warning(
                                f"[id-recheck] Regular proxy high errors: "
                                f"{_consecutive_high_errors}/2 consecutive ({error_rate:.0%})"
                            )
                            if _consecutive_high_errors >= 2:
                                _using_regular = False
                                _fallback_at = time.time()
                                logger.warning("[id-recheck] PROXY FALLBACK: regular → premium")
                                await _send_slack_worker_alert(
                                    emoji="\U0001f504",
                                    title="[IDEALISTA] Recheck — Proxy fallback a PREMIUM",
                                    fields={
                                        "Motivo": "2 batches consecutivos con error rate > 30%",
                                        "Último error rate": f"{error_rate:.1%}",
                                        "Acción": "Cambiado a proxy premium, reiniciando ciclo",
                                    },
                                    category="recheck",
                                    is_alert=True,
                                )
                                break  # Salir del inner loop → outer recrea client con premium
                        else:
                            _consecutive_high_errors = 0  # Reset en batch bueno

                    # Pausa extra si error rate alto
                    if results["errors"] > len(inactive_properties) * 0.5:
                        logger.warning(f"[id-recheck] High error rate, sleeping {cfg['error_sleep']}s")
                        await asyncio.sleep(cfg["error_sleep"])

        except Exception as e:
            logger.error(f"[id-recheck] Worker crashed: {e}", exc_info=True)
            await _send_slack_worker_alert(
                emoji="\U0001f6a8",
                title="[IDEALISTA] Recheck — CRASH",
                fields={"Error": str(e)[:200], "Accion": f"Reiniciando en {cfg['restart_delay']}s"},
                category="recheck",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info("[id-recheck] Restarting worker...")

    logger.info("[id-recheck] Continuous worker stopped")


async def _start_id_recheck():
    """Lanza 6 workers continuos de recheck Idealista (todos regular con fallback)."""
    recheck_state["running"] = True
    recheck_state["totals"] = {
        "processed": 0, "reactivated": 0, "confirmed_inactive": 0, "errors": 0, "batches": 0
    }
    recheck_state["last_batch_at"] = None
    num_workers = 6
    for i in range(num_workers):
        asyncio.create_task(_id_recheck_continuous_worker(worker_id=i, total_workers=num_workers))
    logger.info(f"[id-recheck] {num_workers} workers launched (all regular with fallback)")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HABITACLIA RECHECK — Revisa inactivas para detectar reactivaciones
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HAB_RECHECK_CONFIG = {
    "batch_size": 2000,         # Sin anti-bot → batches grandes
    "delay_min": 0.05,          # Sin proxy/token → delays minimos
    "delay_max": 0.15,
    "sleep_when_idle": 300,     # 5 min si no hay pendientes
    "restart_delay": 30,        # Espera antes de reiniciar tras crash
    "error_sleep": 60,          # Espera si batch tiene >50% errores
}

hab_recheck_state = {
    "running": False,
    "mode": "continuous",       # Siempre continuo (sin dia/noche)
    "totals": {"processed": 0, "reactivated": 0, "confirmed_inactive": 0, "errors": 0, "batches": 0},
    "last_batch_at": None,
}

# --- Configuración de Fotocasa Recheck ---
# Slots 12 + 16 dedicados. Sin throttle dia/noche — velocidad máxima 24/7.
FC_RECHECK_CONFIG = {
    "batch_size": 500,
    "delay_min": 0.3,
    "delay_max": 0.5,
    # Comun
    "sleep_when_idle": 300,
    "restart_delay": 60,
    "error_sleep": 120,
}

fc_recheck_state = {
    "running": False,
    "mode": None,          # "daytime" | "nighttime"
    "totals": {"processed": 0, "reactivated": 0, "confirmed_inactive": 0, "errors": 0, "batches": 0},
    "last_batch_at": None,
}


async def _hab_recheck_continuous_worker(worker_id=0, total_workers=1):
    """
    Worker continuo de recheck Habitaclia.
    Sin JWT/TLS/proxy — usa HttpxClient directo.
    Velocidad constante 24/7 (sin modos dia/noche porque no hay anti-bot).
    worker_id: para particionado de batches entre N workers.
    """
    cfg = HAB_RECHECK_CONFIG
    wlabel = f"hab-recheck-w{worker_id}"
    per_worker_batch = max(100, cfg["batch_size"] // total_workers)
    offset = worker_id * per_worker_batch
    logger.info(f"[{wlabel}] Continuous worker starting (batch={per_worker_batch}, offset={offset})")

    while hab_recheck_state["running"]:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                httpx_client = HttpxClient(client)
                hab = Habitaclia(httpx_client)

                while hab_recheck_state["running"]:
                    # Obtener batch de las ventanas de recheck (time-gated)
                    now_utc = datetime.now(timezone.utc)
                    windows = [
                        ("check1_6h+", "updated_at <= $1 AND recheck_count < 1",
                         [now_utc - timedelta(hours=6)]),
                        ("check2_2d+", "updated_at <= $1 AND recheck_count < 2",
                         [now_utc - timedelta(days=2)]),
                        ("check3_5d+", "updated_at <= $1 AND recheck_count < 3",
                         [now_utc - timedelta(days=5)]),
                        ("check4_14d+", "updated_at <= $1 AND recheck_count < 4",
                         [now_utc - timedelta(days=14)]),
                        ("check5_22d+", "updated_at <= $1 AND recheck_count < 5",
                         [now_utc - timedelta(days=22)]),
                        ("check6_30d+", "updated_at <= $1 AND recheck_count < 6",
                         [now_utc - timedelta(days=30)]),
                        ("monthly_60d+",
                         "updated_at <= $1 AND recheck_count < (6 + FLOOR(EXTRACT(EPOCH FROM ($2 - updated_at)) / 2592000))",
                         [now_utc - timedelta(days=60), now_utc]),
                    ]

                    inactive_properties = []
                    try:
                        for win_name, where, params in windows:
                            remaining = per_worker_batch - len(inactive_properties)
                            if remaining <= 0:
                                break
                            rows = await db_pool.fetch(f"""
                                SELECT propertycode, recheck_count,
                                       (habitaclia_specific->>'CodEmp')::int AS cod_emp,
                                       (habitaclia_specific->>'CodInm')::int AS cod_inm
                                FROM async_properties
                                WHERE source = 'habitaclia' AND adisactive = false
                                AND habitaclia_specific->>'CodEmp' IS NOT NULL
                                AND habitaclia_specific->>'CodInm' IS NOT NULL
                                AND {where}
                                ORDER BY updated_at DESC
                                LIMIT {remaining} OFFSET {offset}
                            """, *params)
                            for r in rows:
                                inactive_properties.append({
                                    "propertycode": r["propertycode"],
                                    "recheck_count": r["recheck_count"] or 0,
                                    "cod_emp": r["cod_emp"],
                                    "cod_inm": r["cod_inm"],
                                })
                            logger.info(f"[hab-recheck] Window {win_name}: {len(rows)} properties")
                    except Exception as e:
                        logger.warning(f"[hab-recheck] SQL error: {e}")
                        await asyncio.sleep(cfg["restart_delay"])
                        continue

                    if not inactive_properties:
                        logger.info(f"[hab-recheck] No pending properties, sleeping {cfg['sleep_when_idle']}s")
                        await asyncio.sleep(cfg["sleep_when_idle"])
                        continue

                    logger.info(f"[hab-recheck] {len(inactive_properties)} to check (delay {cfg['delay_min']}-{cfg['delay_max']}s)")

                    # Procesar batch
                    start_time = time.time()
                    results = {"reactivated": 0, "confirmed_inactive": 0, "errors": 0, "error_samples": []}
                    headers_rest = _supabase_headers()

                    for i, prop in enumerate(inactive_properties):
                        if not hab_recheck_state["running"]:
                            break

                        property_code = prop["propertycode"]
                        cod_emp = prop["cod_emp"]
                        cod_inm = prop["cod_inm"]
                        current_recheck_count = prop["recheck_count"]

                        await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))

                        try:
                            status, detail_data = await _hab_verify_is_active(hab, cod_emp, cod_inm)

                            if status == "active":
                                enrichment = transform_habitaclia_detail_to_enrichment(detail_data)
                                enrichment["adisactive"] = True
                                enrichment["recheck_count"] = 0
                                enrichment["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                await client.patch(url, headers=headers_rest, json=enrichment, timeout=30)
                                results["reactivated"] += 1
                                logger.info(f"[hab-recheck] REACTIVATED {property_code}")

                            elif status == "inactive":
                                confirm_data = {
                                    "recheck_count": current_recheck_count + 1,
                                    "last_checked_at": datetime.now(timezone.utc).isoformat(),
                                }
                                url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                                await client.patch(url, headers=headers_rest, json=confirm_data, timeout=30)
                                results["confirmed_inactive"] += 1

                            else:
                                results["errors"] += 1
                                err_detail = str(detail_data)[:80] if detail_data else "unknown"
                                if len(results["error_samples"]) < 10:
                                    results["error_samples"].append({"code": property_code, "err": err_detail})
                                _audit_err("hab-recheck", f"HTTP {err_detail[:40]}")

                        except Exception as e:
                            results["errors"] += 1
                            if len(results["error_samples"]) < 10:
                                results["error_samples"].append({"code": property_code, "err": str(e)[:80]})
                            _audit_err("hab-recheck", "exception", str(e)[:80])

                        if (i + 1) % 500 == 0:
                            elapsed = time.time() - start_time
                            logger.info(
                                f"[hab-recheck] {i+1}/{len(inactive_properties)} "
                                f"({results['reactivated']}R {results['confirmed_inactive']}C "
                                f"{results['errors']}E) {elapsed:.0f}s"
                            )

                    # Actualizar totales acumulados
                    total_time = time.time() - start_time
                    hab_recheck_state["totals"]["processed"]          += len(inactive_properties)
                    hab_recheck_state["totals"]["reactivated"]        += results["reactivated"]
                    hab_recheck_state["totals"]["confirmed_inactive"] += results["confirmed_inactive"]
                    hab_recheck_state["totals"]["errors"]             += results["errors"]
                    hab_recheck_state["totals"]["batches"]            += 1
                    hab_recheck_state["last_batch_at"] = datetime.now(timezone.utc).isoformat()

                    logger.info(
                        f"[hab-recheck] Batch done: {len(inactive_properties)} checked, "
                        f"{results['reactivated']}R {results['confirmed_inactive']}C "
                        f"{results['errors']}E, {total_time:.1f}s"
                    )

                    # Slack solo si error rate alto (>10%)
                    error_rate = results["errors"] / max(len(inactive_properties), 1)
                    if error_rate > 0.25:
                        batch_id = f"hab-recheck-{int(time.time())}"
                        error_summary = ", ".join([s["err"] for s in results["error_samples"][:10]]) or "ninguno"
                        await _send_slack_worker_alert(
                            emoji="\u26a0\ufe0f",
                            title=f"[HABITACLIA] Recheck — Error rate alto ({error_rate:.0%})",
                            fields={
                                "Procesadas": str(len(inactive_properties)),
                                "Errores": str(results["errors"]),
                                "Error rate": f"{error_rate:.1%}",
                                "Error samples": error_summary,
                            },
                            category="recheck",
                            is_alert=True,
                        )

                    # Pausa extra si error rate alto
                    if results["errors"] > len(inactive_properties) * 0.5:
                        logger.warning(f"[hab-recheck] High error rate, sleeping {cfg['error_sleep']}s")
                        await asyncio.sleep(cfg["error_sleep"])

        except Exception as e:
            logger.error(f"[hab-recheck] Worker crashed: {e}", exc_info=True)
            await _send_slack_worker_alert(
                emoji="\U0001f6a8",
                title="[HABITACLIA] Recheck — CRASH",
                fields={"Error": str(e)[:200], "Accion": f"Reiniciando en {cfg['restart_delay']}s"},
                category="recheck",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info("[hab-recheck] Restarting worker...")

    logger.info("[hab-recheck] Continuous worker stopped")


async def _start_hab_recheck():
    """Lanza workers continuos de recheck Habitaclia."""
    hab_recheck_state["running"] = True
    hab_recheck_state["totals"] = {
        "processed": 0, "reactivated": 0, "confirmed_inactive": 0, "errors": 0, "batches": 0
    }
    hab_recheck_state["last_batch_at"] = None
    num_workers = 5
    for i in range(num_workers):
        asyncio.create_task(_hab_recheck_continuous_worker(worker_id=i, total_workers=num_workers))
    logger.info(f"[hab-recheck] {num_workers} continuous workers launched")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOTOCASA — Recheck continuo (2 workers: slots 12 + 16)
#  Velocidad adaptativa: mas rapido de noche (01-06 UTC),
#  mas lento de dia para dejar margen al slot compartido.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _fc_recheck_continuous_worker(slot_id=12, worker_id=0, total_workers=1):
    """
    Worker continuo de recheck Fotocasa.
    - Modo nocturno 01:00-06:00 UTC: rapido (delay 0.3-0.5s, batch 500)
    - Modo diurno resto del dia: lento (delay 0.8-1.2s, batch 200)
    slot_id: slot Redroid a usar. worker_id/total_workers: para OFFSET partitioning.
    """
    cfg = FC_RECHECK_CONFIG
    wlabel = f"fc-recheck-w{worker_id}"
    logger.info(f"[{wlabel}] Continuous worker starting (slot {slot_id})")

    def _is_nighttime():
        hour = datetime.now(timezone.utc).hour
        return cfg["nighttime_start_hour"] <= hour < cfg["nighttime_end_hour"]

    # — A/B proxy test: empezar con regular, fallback a premium si error rate alto —
    _proxy_regular = PROXY_URL_REGULAR
    _proxy_premium = PROXY_URL_PREMIUM
    _using_regular = bool(_proxy_regular)
    _consecutive_high_errors = 0
    _fallback_at = 0
    if _using_regular:
        logger.info(f"[{wlabel}] Starting with REGULAR proxy")
    else:
        logger.info(f"[{wlabel}] No PROXY_URL_REGULAR set, using premium proxy")

    def _create_fc_client():
        active_proxy = _proxy_regular if _using_regular else _proxy_premium
        kw = {"timeout": 30}
        if active_proxy:
            kw["proxies"] = active_proxy
        hc = httpx.AsyncClient(**kw)
        fc = Fotocasa(HttpxClient(hc))
        _randomize_fc_device(fc, token_cache=_fc_token_slots[slot_id])
        return hc, fc

    while fc_recheck_state["running"]:
        try:
            # --- Rebounce ---
            if not _using_regular and _fallback_at and _proxy_regular:
                if time.time() - _fallback_at > PROXY_REBOUNCE_SECONDS:
                    _using_regular = True
                    _consecutive_high_errors = 0
                    _fallback_at = 0
                    logger.info(f"[{wlabel}] PROXY REBOUNCE: premium → regular")

            http_client, fotocasa = _create_fc_client()
            db_client = httpx.AsyncClient(timeout=30)  # Sin proxy — para Supabase REST
            proxy_req_count = 0

            try:
                init_ok, _ = await fotocasa.initialize_session()
                if init_ok:
                    logger.info(f"[{wlabel}] Session initialized OK")
                else:
                    logger.warning(f"[{wlabel}] Session init failed (continuing without CSRF)")
            except Exception as e:
                logger.warning(f"[{wlabel}] Session init error: {e}")

            while fc_recheck_state["running"]:
                # Velocidad constante 24/7 (slots dedicados)
                batch_size = cfg["batch_size"]
                delay_min  = cfg["delay_min"]
                delay_max  = cfg["delay_max"]
                fc_recheck_state["mode"] = "continuous"

                # Obtener token Hetzner slot dedicado
                token = await _get_fc_hetzner_token(slot_id=slot_id)
                if not token:
                    logger.warning(f"[{wlabel}] No Hetzner token (slot {slot_id}), sleeping 60s")
                    await asyncio.sleep(60)
                    continue

                # Obtener batch de las ventanas de recheck (time-gated)
                now_utc = datetime.now(timezone.utc)
                windows = [
                    ("check1_6h+", "updated_at <= $1 AND recheck_count < 1",
                     [now_utc - timedelta(hours=6)]),
                    ("check2_2d+", "updated_at <= $1 AND recheck_count < 2",
                     [now_utc - timedelta(days=2)]),
                    ("check3_5d+", "updated_at <= $1 AND recheck_count < 3",
                     [now_utc - timedelta(days=5)]),
                    ("check4_14d+", "updated_at <= $1 AND recheck_count < 4",
                     [now_utc - timedelta(days=14)]),
                    ("check5_22d+", "updated_at <= $1 AND recheck_count < 5",
                     [now_utc - timedelta(days=22)]),
                    ("check6_30d+", "updated_at <= $1 AND recheck_count < 6",
                     [now_utc - timedelta(days=30)]),
                    ("monthly_60d+",
                     "updated_at <= $1 AND recheck_count < (6 + FLOOR(EXTRACT(EPOCH FROM ($2 - updated_at)) / 2592000))",
                     [now_utc - timedelta(days=60), now_utc]),
                ]

                # Particionado entre workers
                per_worker_batch = max(50, batch_size // total_workers)
                offset = worker_id * per_worker_batch

                inactive_properties = []
                try:
                    for win_name, where, params in windows:
                        remaining = per_worker_batch - len(inactive_properties)
                        if remaining <= 0:
                            break
                        rows = await db_pool.fetch(f"""
                            SELECT propertycode, recheck_count, operation, fotocasa_specific
                            FROM async_properties
                            WHERE source = 'fotocasa' AND adisactive = false
                            AND {where}
                            ORDER BY updated_at DESC
                            LIMIT {remaining} OFFSET {offset}
                        """, *params)
                        for r in rows:
                            fc_spec = r["fotocasa_specific"] or {}
                            if isinstance(fc_spec, str):
                                fc_spec = json.loads(fc_spec)
                            inactive_properties.append({
                                "propertycode": r["propertycode"],
                                "recheck_count": r["recheck_count"] or 0,
                                "operation": r["operation"] or "sale",
                                "fotocasa_specific": fc_spec,
                            })
                        if rows:
                            logger.info(f"[{wlabel}] Window {win_name}: {len(rows)} properties")
                except Exception as e:
                    logger.warning(f"[{wlabel}] SQL error: {e}")
                    await asyncio.sleep(cfg["restart_delay"])
                    continue

                if not inactive_properties:
                    logger.info(f"[{wlabel}] No pending properties, sleeping {cfg['sleep_when_idle']}s")
                    await asyncio.sleep(cfg["sleep_when_idle"])
                    continue

                logger.info(f"[{wlabel}] {len(inactive_properties)} to check (delay {delay_min}-{delay_max}s)")

                # Procesar batch
                start_time = time.time()
                results = {"reactivated": 0, "confirmed_inactive": 0, "errors": 0, "error_samples": []}
                _consecutive_403 = 0  # Circuit breaker counter

                for i, prop in enumerate(inactive_properties):
                    if not fc_recheck_state["running"]:
                        break

                    property_code = prop["propertycode"]
                    current_recheck_count = prop["recheck_count"]
                    operation = prop.get("operation", "sale")
                    fc_specific = prop.get("fotocasa_specific") or {}
                    tx_type = "RENT" if operation == "rent" else "SALE"

                    await asyncio.sleep(random.uniform(delay_min, delay_max))

                    # Rotar proxy cada N requests
                    proxy_req_count += 1
                    if proxy_req_count % FC_PROXY_ROTATE_EVERY == 0:
                        logger.info(f"[{wlabel}] Proxy rotated at req {proxy_req_count}")
                        await http_client.aclose()
                        http_client, fotocasa = _create_fc_client()
                        try:
                            await fotocasa.initialize_session()
                        except Exception:
                            pass

                    # Refrescar token si necesario
                    token = await _get_fc_hetzner_token(slot_id=slot_id)
                    if not token:
                        results["errors"] += 1
                        if len(results["error_samples"]) < 10:
                            results["error_samples"].append({"code": property_code, "err": "no token"})
                        _audit_err("fc-recheck", "no_token")
                        continue

                    headers_rest = _supabase_headers()
                    try:
                        status, detail_data = await _fc_verify_is_active(
                            fotocasa, property_code, token, transaction_type=tx_type
                        )

                        if status == "active":
                            enrichment = transform_fotocasa_detail_to_enrichment(
                                detail_data, existing_fotocasa_specific=fc_specific,
                            )
                            enrichment["adisactive"] = True
                            enrichment["recheck_count"] = 0
                            enrichment["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                            url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                            await db_client.patch(url, headers=headers_rest, json=enrichment, timeout=30)
                            results["reactivated"] += 1
                            _consecutive_403 = 0
                            logger.info(f"[{wlabel}] REACTIVATED {property_code}")

                        elif status == "inactive":
                            confirm_data = {
                                "recheck_count": current_recheck_count + 1,
                                "last_checked_at": datetime.now(timezone.utc).isoformat(),
                            }
                            url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{property_code}"
                            await db_client.patch(url, headers=headers_rest, json=confirm_data, timeout=30)
                            results["confirmed_inactive"] += 1
                            _consecutive_403 = 0

                        elif status == "token_expired":
                            # 403 → rotar proxy + refrescar token — FC rate-limita por token + IP
                            _consecutive_403 += 1
                            logger.info(f"[{wlabel}] 403 → rotating proxy + refreshing token (consecutive={_consecutive_403})")
                            await http_client.aclose()
                            http_client, fotocasa = _create_fc_client()
                            try:
                                await fotocasa.initialize_session()
                            except Exception:
                                pass
                            proxy_req_count = 0
                            # Siempre refrescar token en 403
                            token = await _get_fc_hetzner_token(slot_id=slot_id, force_refresh=True)
                            if not token:
                                logger.warning(f"[{wlabel}] No token after refresh, backoff 30s")
                                await asyncio.sleep(30)
                                token = await _get_fc_hetzner_token(slot_id=slot_id)
                            results["errors"] += 1
                            if len(results["error_samples"]) < 10:
                                results["error_samples"].append({"code": property_code, "err": "403 ip_ratelimit"})
                            _audit_err("fc-recheck", "HTTP 403")
                            # Circuit breaker: 2 consecutive 403s = fresh combo rejected
                            if _consecutive_403 >= 2:
                                logger.warning(f"[{wlabel}] CIRCUIT BREAK: 2 consecutive 403s, backoff 35s")
                                break
                            await asyncio.sleep(3)

                        else:
                            results["errors"] += 1
                            _consecutive_403 = 0
                            err_code = detail_data.get("status_code", "?") if isinstance(detail_data, dict) else "?"
                            if len(results["error_samples"]) < 10:
                                results["error_samples"].append({"code": property_code, "err": f"HTTP {err_code}"})
                            _audit_err("fc-recheck", f"HTTP {err_code}")

                    except Exception as e:
                        results["errors"] += 1
                        if len(results["error_samples"]) < 10:
                            results["error_samples"].append({"code": property_code, "err": str(e)[:80]})
                        _audit_err("fc-recheck", "exception", str(e)[:80])

                    if (i + 1) % 100 == 0:
                        elapsed = time.time() - start_time
                        logger.info(
                            f"[{wlabel}] {i+1}/{len(inactive_properties)} "
                            f"({results['reactivated']}R {results['confirmed_inactive']}C "
                            f"{results['errors']}E) {elapsed:.0f}s"
                        )

                # Actualizar totales acumulados
                total_time = time.time() - start_time
                fc_recheck_state["totals"]["processed"]          += len(inactive_properties)
                fc_recheck_state["totals"]["reactivated"]        += results["reactivated"]
                fc_recheck_state["totals"]["confirmed_inactive"] += results["confirmed_inactive"]
                fc_recheck_state["totals"]["errors"]             += results["errors"]
                fc_recheck_state["totals"]["batches"]            += 1
                fc_recheck_state["last_batch_at"] = datetime.now(timezone.utc).isoformat()

                logger.info(
                    f"[{wlabel}] Batch done: {len(inactive_properties)} checked, "
                    f"{results['reactivated']}R {results['confirmed_inactive']}C "
                    f"{results['errors']}E, {total_time:.1f}s"
                )

                # Circuit breaker backoff
                if _consecutive_403 >= 2:
                    await asyncio.sleep(35)
                    continue

                # Slack solo si error rate alto (>10%)
                error_rate = results["errors"] / max(len(inactive_properties), 1)
                if error_rate > 0.25:
                    batch_id = f"fc-recheck-{int(time.time())}"
                    error_summary = ", ".join([s["err"] for s in results["error_samples"][:10]]) or "ninguno"
                    await _send_slack_worker_alert(
                        emoji="\u26a0\ufe0f",
                        title=f"[FOTOCASA] Recheck — Error rate alto ({error_rate:.0%})",
                        fields={
                            "Procesadas": str(len(inactive_properties)),
                            "Errores": str(results["errors"]),
                            "Error rate": f"{error_rate:.1%}",
                            "Error samples": error_summary,
                        },
                        category="recheck",
                        is_alert=True,
                    )
                 # — A/B proxy test: fallback a premium si 2 batches consecutivos >30% errores —
                if _using_regular:
                    if error_rate > 0.30:
                        _consecutive_high_errors += 1
                        logger.warning(
                            f"[{wlabel}] Regular proxy high errors: "
                            f"{_consecutive_high_errors}/2 consecutive ({error_rate:.0%})"
                        )
                        if _consecutive_high_errors >= 2:
                            _using_regular = False
                            _fallback_at = time.time()
                            logger.warning("[{wlabel}] PROXY FALLBACK: regular → premium")
                            await _send_slack_worker_alert(
                                emoji="\U0001f504",
                                title="[FOTOCASA] Recheck — Proxy fallback a PREMIUM",
                                fields={
                                    "Motivo": "2 batches consecutivos con error rate > 30%",
                                    "Último error rate": f"{error_rate:.1%}",
                                    "Acción": "Cambiado a proxy premium, reiniciando ciclo",
                                },
                                category="recheck",
                                is_alert=True,
                            )
                            try:
                                await http_client.aclose()
                            except Exception:
                                pass
                            try:
                                await db_client.aclose()
                            except Exception:
                                pass
                            break  # Salir del inner loop → outer recrea client con premium
                    else:
                        _consecutive_high_errors = 0  # Reset en batch bueno

                # Pausa extra si error rate alto
                if results["errors"] > len(inactive_properties) * 0.5:
                    logger.warning(f"[{wlabel}] High error rate, sleeping {cfg['error_sleep']}s")
                    await asyncio.sleep(cfg["error_sleep"])

        except Exception as e:
            logger.error(f"[{wlabel}] Worker crashed: {e}", exc_info=True)
            try:
                await http_client.aclose()
            except Exception:
                pass
            try:
                await db_client.aclose()
            except Exception:
                pass
            await _send_slack_worker_alert(
                emoji="\U0001f6a8",
                title="[FOTOCASA] Recheck — CRASH",
                fields={"Error": str(e)[:200], "Accion": f"Reiniciando en {cfg['restart_delay']}s"},
                category="recheck",
                is_alert=True,
            )
            await asyncio.sleep(cfg["restart_delay"])
            logger.info("[{wlabel}] Restarting worker...")

    logger.info("[{wlabel}] Continuous worker stopped")


async def _start_fc_recheck():
    """Lanza workers continuos de recheck Fotocasa (slots 12 + 16)."""
    fc_recheck_state["running"] = True
    fc_recheck_state["mode"] = None
    fc_recheck_state["totals"] = {
        "processed": 0, "reactivated": 0, "confirmed_inactive": 0, "errors": 0, "batches": 0
    }
    fc_recheck_state["last_batch_at"] = None
    recheck_slots = [12, 16]
    for i, slot in enumerate(recheck_slots):
        asyncio.create_task(_fc_recheck_continuous_worker(slot_id=slot, worker_id=i, total_workers=len(recheck_slots)))
    logger.info(f"[fc-recheck] {len(recheck_slots)} continuous workers launched (slots {recheck_slots})")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IDEALISTA — Endpoints


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WORKERS AUDIT — Endpoint de auditoría unificado
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/workers-audit")
async def workers_audit():
    """
    Auditoría completa de todos los workers. Devuelve:
    - Estado de salud de cada worker
    - Velocidades actuales vs auditoría anterior
    - Errores deduplicados desde última auditoría
    - Backlogs pendientes por proceso/tier
    
    Al llamar: guarda snapshot actual como "anterior" y resetea error store.
    """
    global _audit_last_snapshot, _audit_last_at
    
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    
    # ── 1. Recoger estado actual de cada plataforma ──
    
    # IDEALISTA
    id_ca_elapsed = 0
    if check_active_state["started_at"]:
        id_ca_elapsed = (now - datetime.fromisoformat(check_active_state["started_at"])).total_seconds()
    id_ca_hours = id_ca_elapsed / 3600 if id_ca_elapsed > 0 else 1
    id_ca_speed = round(check_active_state["totals"]["checked"] / id_ca_hours)
    
    id_en_elapsed = 0
    if enrich_state["started_at"]:
        id_en_elapsed = (now - datetime.fromisoformat(enrich_state["started_at"])).total_seconds()
    id_en_hours = id_en_elapsed / 3600 if id_en_elapsed > 0 else 1
    
    id_su_elapsed = 0
    if id_scrape_updates_state["started_at"]:
        id_su_elapsed = (now - datetime.fromisoformat(id_scrape_updates_state["started_at"])).total_seconds()
    
    # FOTOCASA
    fc_ca_elapsed = 0
    if fc_check_active_state["started_at"]:
        fc_ca_elapsed = (now - datetime.fromisoformat(fc_check_active_state["started_at"])).total_seconds()
    fc_ca_hours = fc_ca_elapsed / 3600 if fc_ca_elapsed > 0 else 1
    fc_ca_speed = round(fc_check_active_state["totals"]["checked"] / fc_ca_hours)
    
    fc_su_elapsed = 0
    if fc_scrape_updates_state["started_at"]:
        fc_su_elapsed = (now - datetime.fromisoformat(fc_scrape_updates_state["started_at"])).total_seconds()
    
    # HABITACLIA
    hab_ca_elapsed = 0
    if hab_check_active_state["started_at"]:
        hab_ca_elapsed = (now - datetime.fromisoformat(hab_check_active_state["started_at"])).total_seconds()
    hab_ca_hours = hab_ca_elapsed / 3600 if hab_ca_elapsed > 0 else 1
    hab_ca_speed = round(hab_check_active_state["totals"]["checked"] / hab_ca_hours)
    
    hab_su_elapsed = 0
    if hab_scrape_updates_state["started_at"]:
        hab_su_elapsed = (now - datetime.fromisoformat(hab_scrape_updates_state["started_at"])).total_seconds()
    
    # ── 2. Workers health summary ──
    def _worker_statuses(workers_dict):
        """Formato: 'checking:main_provinces' o 'checking:main_provinces[P]' si premium."""
        result = {}
        for wid, w in workers_dict.items():
            status = w.get("status", "?")
            proxy = w.get("proxy")
            if proxy == "premium":
                status = f"{status}[P]"
            result[f"w{wid}"] = status
        return result
    
    # ── 3. Velocidades actuales ──
    current_speeds = {
        "id_check_active": id_ca_speed,
        "id_enrich": round(enrich_state["totals"]["enriched"] / id_en_hours) if id_en_hours > 0 else 0,
        "fc_check_active": fc_ca_speed,
        "fc_enrich": round(fc_enrich_state["totals"]["enriched"] / max((fc_ca_elapsed / 3600), 1)),
        "hab_check_active": hab_ca_speed,
        "hab_enrich": round(hab_enrich_state["totals"]["enriched"] / max((hab_ca_elapsed / 3600), 1)),
    }

    # Per-tier speed breakdown
    tier_speeds = {}
    for label, state, hours in [
        ("id", check_active_state, id_ca_hours),
        ("fc", fc_check_active_state, max(fc_ca_elapsed / 3600, 0.1)),
        ("hab", hab_check_active_state, max(hab_ca_elapsed / 3600, 0.1)),
    ]:
        bt = state["totals"].get("by_tier", {})
        for tname, tdata in bt.items():
            tier_speeds[f"{label}_{tname}"] = {
                "checked_per_h": round(tdata["checked"] / hours) if hours > 0 else 0,
                "errors_per_h": round(tdata["errors"] / hours) if hours > 0 else 0,
                "error_rate": round(tdata["errors"] / max(tdata["checked"], 1) * 100, 1),
            }
    
    # ── 4. Comparar con snapshot anterior ──
    speed_comparison = {}
    if _audit_last_snapshot and "speeds" in _audit_last_snapshot:
        prev = _audit_last_snapshot["speeds"]
        for key in current_speeds:
            curr = current_speeds[key]
            prev_val = prev.get(key, 0)
            if prev_val > 0:
                delta_pct = round((curr - prev_val) / prev_val * 100, 1)
                speed_comparison[key] = {
                    "current": curr,
                    "previous": prev_val,
                    "delta": f"{'+' if delta_pct > 0 else ''}{delta_pct}%",
                }
            else:
                speed_comparison[key] = {"current": curr, "previous": 0, "delta": "N/A"}
    else:
        for key in current_speeds:
            speed_comparison[key] = {"current": current_speeds[key], "previous": None, "delta": "first audit"}
    
    # ── 5. Totals actuales ──
    current_totals = {
        "idealista": {
            "check_active": dict(check_active_state["totals"]),
            "enrich": dict(enrich_state["totals"]),
            "scrape_updates": dict(id_scrape_updates_state["totals"]),
            "recheck": dict(recheck_state.get("totals") or {}),
        },
        "fotocasa": {
            "check_active": dict(fc_check_active_state["totals"]),
            "enrich": dict(fc_enrich_state["totals"]),
            "scrape_updates": dict(fc_scrape_updates_state["totals"]),
            "recheck": dict(fc_recheck_state["totals"]),
        },
        "habitaclia": {
            "check_active": dict(hab_check_active_state["totals"]),
            "enrich": dict(hab_enrich_state["totals"]),
            "scrape_updates": dict(hab_scrape_updates_state["totals"]),
            "recheck": dict(hab_recheck_state["totals"]),
        },
    }
    
    # ── 6. Backlogs via SQL (queries individuales, timeout 60s, no bloquean el resto) ──
    backlogs = {}
    backlogs_start = time.time()
    if db_pool:
        # Check-active backlogs por plataforma y tier
        for source, tiers_config, label in [
            ("idealista", CHECK_ACTIVE_TIERS, "id"),
            ("fotocasa", FC_CHECK_ACTIVE_TIERS, "fc"),
            ("habitaclia", HAB_CHECK_ACTIVE_TIERS, "hab"),
        ]:
            for tier in tiers_config:
                tier_name = tier["name"]
                cycle_h = tier["cycle_hours"]
                cutoff = now - timedelta(hours=cycle_h)
                key = f"{label}_check_active_{tier_name}"
                try:
                    row = await asyncio.wait_for(
                        db_pool.fetchrow(f"""
                            SELECT COUNT(*) as cnt FROM async_properties
                            WHERE source = $1 AND adisactive = true
                            AND ({tier['where_clause']})
                            AND (last_checked_at IS NULL OR last_checked_at < $2)
                        """, source, cutoff),
                        timeout=60.0,
                    )
                    backlogs[key] = row["cnt"] if row else "?"
                except asyncio.TimeoutError:
                    backlogs[key] = "timeout_60s"
                except Exception as e:
                    backlogs[key] = f"error: {str(e)[:60]}"
        
        # Enrich backlogs
        for source, label in [("idealista", "id"), ("fotocasa", "fc"), ("habitaclia", "hab")]:
            key = f"{label}_enrich_pending"
            try:
                row = await asyncio.wait_for(
                    db_pool.fetchrow(
                        "SELECT COUNT(*) as cnt FROM async_properties "
                        "WHERE source = $1 AND adisactive = true AND detail_enriched_at IS NULL",
                        source,
                    ),
                    timeout=60.0,
                )
                backlogs[key] = row["cnt"] if row else "?"
            except asyncio.TimeoutError:
                backlogs[key] = "timeout_60s"
            except Exception as e:
                backlogs[key] = f"error: {str(e)[:60]}"
        
        # Recheck backlogs por time gate (distribución clara, sin solapamiento)
        now_utc = datetime.now(timezone.utc)
        for source, label in [("idealista", "id"), ("fotocasa", "fc"), ("habitaclia", "hab")]:
            recheck_detail = {}
            # Gates exclusivos: cada prop aparece en exactamente 1 bucket
            gates = [
                # Props recién desactivadas — aún no elegibles
                ("waiting_lt6h", "recheck_count = 0 AND updated_at > $2", [now_utc - timedelta(hours=6)]),
                # Elegibles para procesamiento AHORA
                ("due_check1",   "recheck_count = 0 AND updated_at <= $2", [now_utc - timedelta(hours=6)]),
                ("due_check2",   "recheck_count = 1 AND updated_at <= $2", [now_utc - timedelta(days=2)]),
                ("due_check3",   "recheck_count = 2 AND updated_at <= $2", [now_utc - timedelta(days=5)]),
                ("due_check4",   "recheck_count = 3 AND updated_at <= $2", [now_utc - timedelta(days=14)]),
                ("due_check5",   "recheck_count = 4 AND updated_at <= $2", [now_utc - timedelta(days=22)]),
                ("due_check6",   "recheck_count = 5 AND updated_at <= $2", [now_utc - timedelta(days=30)]),
                # Esperando a que se cumpla la edad para el siguiente gate
                ("waiting_check2", "recheck_count = 1 AND updated_at > $2", [now_utc - timedelta(days=2)]),
                ("waiting_check3", "recheck_count = 2 AND updated_at > $2", [now_utc - timedelta(days=5)]),
                ("waiting_check4", "recheck_count = 3 AND updated_at > $2", [now_utc - timedelta(days=14)]),
                ("waiting_check5", "recheck_count = 4 AND updated_at > $2", [now_utc - timedelta(days=22)]),
                ("waiting_check6", "recheck_count = 5 AND updated_at > $2", [now_utc - timedelta(days=30)]),
                # Mensual + completadas
                ("due_monthly",  "recheck_count >= 6 AND updated_at <= $2", [now_utc - timedelta(days=60)]),
                ("waiting_monthly", "recheck_count >= 6 AND updated_at > $2", [now_utc - timedelta(days=60)]),
            ]
            try:
                for gate_name, where, params in gates:
                    row = await asyncio.wait_for(
                        db_pool.fetchrow(
                            f"SELECT COUNT(*) as cnt FROM async_properties "
                            f"WHERE source = $1 AND adisactive = false AND {where}",
                            source, *params,
                        ),
                        timeout=30.0,
                    )
                    cnt = row["cnt"] if row else 0
                    if cnt > 0:
                        recheck_detail[gate_name] = cnt
            except asyncio.TimeoutError:
                recheck_detail["_error"] = "timeout"
            except Exception as e:
                recheck_detail["_error"] = str(e)[:60]
            backlogs[f"{label}_recheck"] = recheck_detail
        
        backlogs["_query_time_seconds"] = round(time.time() - backlogs_start, 1)
    else:
        backlogs["_error"] = "no db_pool"
    
    # ── 7. Error store (deduplicado) ──
    errors_list = []
    for sig, entry in sorted(_audit_error_store.items(), key=lambda x: -x[1]["count"]):
        errors_list.append({
            "signature": sig,
            "process": entry["process"],
            "error_type": entry["error_type"],
            "count": entry["count"],
            "first_at": entry["first_at"],
            "last_at": entry["last_at"],
            "timestamps_sample": entry["timestamps"][:30],  # Mostrar max 30 en respuesta
            "sample_detail": entry["sample_detail"],
        })
    
    # ── 8. Workers health ──
    health = {
        "idealista": {
            "check_active": {
                "running": check_active_state["running"],
                "uptime_h": round(id_ca_elapsed / 3600, 1),
                "workers": _worker_statuses(check_active_state["workers"]),
            },
            "enrich": {
                "running": enrich_state["running"],
                "status": enrich_state["status"],
            },
            "scrape_updates": {
                "running": id_scrape_updates_state["running"],
                "workers": _worker_statuses(id_scrape_updates_state["workers"]),
            },
            "recheck": {"running": recheck_state["running"]},
            "mirror": {
                "running": mirror_state["running"],
                "status": mirror_state["status"],
            },
        },
        "fotocasa": {
            "check_active": {
                "running": fc_check_active_state["running"],
                "uptime_h": round(fc_ca_elapsed / 3600, 1),
                "workers": _worker_statuses(fc_check_active_state["workers"]),
                "throttle_level": _fc_throttle["current_level"],
            },
            "enrich": {
                "running": fc_enrich_state["running"],
                "status": fc_enrich_state["status"],
            },
            "scrape_updates": {
                "running": fc_scrape_updates_state["running"],
                "workers": _worker_statuses(fc_scrape_updates_state["workers"]),
            },
            "recheck": {
                "running": fc_recheck_state["running"],
                "mode": fc_recheck_state["mode"],
            },
            "token_slots": {
                f"s{i}": {
                    "age_s": round(time.time() - _fc_token_slots[i]["obtained_at"]) if _fc_token_slots[i]["obtained_at"] else None,
                    "reqs": _fc_token_slots[i]["requests_since"],
                }
                for i in range(1, 17)
            },
        },
        "habitaclia": {
            "check_active": {
                "running": hab_check_active_state["running"],
                "uptime_h": round(hab_ca_elapsed / 3600, 1),
                "workers": _worker_statuses(hab_check_active_state["workers"]),
            },
            "enrich": {
                "running": hab_enrich_state["running"],
                "status": hab_enrich_state["status"],
            },
            "scrape_updates": {
                "running": hab_scrape_updates_state["running"],
                "workers": _worker_statuses(hab_scrape_updates_state["workers"]),
            },
            "recheck": {
                "running": hab_recheck_state["running"],
                "mode": hab_recheck_state["mode"],
            },
        },
    }
    
    # Per-worker stats for debugging HTTP 0 cascades
    worker_stats = {}
    now_ts = time.time()
    for label, state, hours in [
        ("id", check_active_state, id_ca_hours),
        ("fc", fc_check_active_state, max(fc_ca_elapsed / 3600, 0.1)),
        ("hab", hab_check_active_state, max(hab_ca_elapsed / 3600, 0.1)),
    ]:
        uptime_s = hours * 3600
        for wid, w in state["workers"].items():
            checked = w.get("checked", 0)
            errors = w.get("errors", 0)
            # Calculate idle time (accumulated + current if idle now)
            idle_s = w.get("idle_total_s", 0)
            if w.get("_last_idle_at"):
                idle_s += now_ts - w["_last_idle_at"]
            idle_pct = round(idle_s / max(uptime_s, 1) * 100, 1)
            worker_stats[f"{label}_w{wid}"] = {
                "tier": w.get("current_tier", "?"),
                "checked": checked,
                "errors": errors,
                "per_h": round(checked / hours) if hours > 0 else 0,
                "err_rate": round(errors / max(checked, 1) * 100, 1),
                "batches": w.get("batches_done", 0),
                "idle_pct": idle_pct,
            }

    # ── 9. Construir respuesta ──
    result = {
        "audit_at": now_iso,
        "previous_audit_at": _audit_last_at,
        "uptime_since_deploy": check_active_state["started_at"],  # Proxy del deploy time
        "health": health,
        "speeds": speed_comparison,
        "tier_speeds": tier_speeds,
        "worker_stats": worker_stats,
        "totals": current_totals,
        "backlogs": backlogs,
        "errors_since_last_audit": errors_list,
        "error_count_total": sum(e["count"] for e in errors_list),
    }
    
    # ── 10. Guardar snapshot y resetear ──
    _audit_last_snapshot = {
        "speeds": current_speeds,
        "totals": current_totals,
        "audit_at": now_iso,
    }
    _audit_last_at = now_iso
    _audit_error_store.clear()
    
    logger.info(f"[workers-audit] Audit completed. {len(errors_list)} error signatures, {result['error_count_total']} total errors.")
    
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/idealista/workers-status")
async def idealista_workers_status():
    """Estado unificado de todos los procesos de Idealista."""
    ca_elapsed = 0
    if check_active_state["started_at"]:
        start = datetime.fromisoformat(check_active_state["started_at"])
        ca_elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    ca_hours = ca_elapsed / 3600 if ca_elapsed > 0 else 1
    ca_speed = check_active_state["totals"]["checked"] / ca_hours

    en_elapsed = 0
    if enrich_state["started_at"]:
        start = datetime.fromisoformat(enrich_state["started_at"])
        en_elapsed = (datetime.now(timezone.utc) - start).total_seconds()

    return {
        "check_active": {
            "running": check_active_state["running"],
            "started_at": check_active_state["started_at"],
            "elapsed_hours": round(ca_elapsed / 3600, 1),
            "speed_per_hour": round(ca_speed),
            "workers": check_active_state["workers"],
            "totals": check_active_state["totals"],
            "tiers": [{"name": t["name"], "cycle_hours": t["cycle_hours"]} for t in CHECK_ACTIVE_TIERS],
        },
        "enrich": {
            "running": enrich_state["running"],
            "started_at": enrich_state["started_at"],
            "elapsed_hours": round(en_elapsed / 3600, 1),
            "status": enrich_state["status"],
            "totals": enrich_state["totals"],
        },
        "recheck": {
            "running": recheck_state["running"],
            "mode": recheck_state["mode"],
            "totals": recheck_state["totals"],
            "last_batch_at": recheck_state["last_batch_at"],
        },
        "mirror": {
            "running": mirror_state["running"],
            "started_at": mirror_state["started_at"],
            "status": mirror_state["status"],
            "totals": mirror_state["totals"],
        },
        "scrape_updates": {
            "running": id_scrape_updates_state["running"],
            "started_at": id_scrape_updates_state["started_at"],
            "workers": id_scrape_updates_state["workers"],
            "totals": id_scrape_updates_state["totals"],
        },
    }


@app.post("/idealista/workers-stop")
async def idealista_workers_stop():
    """Detiene todos los workers de Idealista (check-active, enrich, scrape-updates, recheck, mirror)."""
    check_active_state["running"] = False
    enrich_state["running"] = False
    mirror_state["running"] = False
    id_scrape_updates_state["running"] = False
    recheck_state["running"] = False
    return {"success": True, "message": "Stop signal sent to all Idealista workers (including recheck)"}


@app.post("/idealista/workers-restart")
async def idealista_workers_restart():
    """Reinicia todos los workers de Idealista (check-active, enrich, scrape-updates, recheck, mirror)."""
    check_active_state["running"] = False
    enrich_state["running"] = False
    id_scrape_updates_state["running"] = False
    recheck_state["running"] = False
    mirror_state["running"] = False
    await asyncio.sleep(5)
    await _start_check_active()
    await _start_enrich()
    await _start_id_scrape_updates()
    await _start_id_recheck()
    return {"success": True, "message": "All Idealista workers restarted (including recheck)"}


@app.post("/idealista/backfill")
async def idealista_backfill(request: IdealistaBackfillRequest):
    """
    One-shot: search + upsert para backfill de una provincia.
    Scrapea N páginas ordenadas por fecha y hace upsert.
    """
    start = time.time()
    logger.info(f"[id-backfill] Starting: {request.locationIds}, {request.numPages} pages")

    try:
        idealista_client, tls_impl, proxy_url = await _get_idealista_client()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Could not get Idealista client: {e}")

    all_props = []
    pages_ok = 0

    for page in range(1, request.numPages + 1):
        try:
            success, response = await idealista_client.search_properties_by_location_id(
                location_ids=request.locationIds,
                num_page=page,
                max_items=request.maxItems,
                order="publicationDate",
            )
            if not success:
                logger.warning(f"[id-backfill] Page {page} failed: {response}")
                break

            elements = response.get("elementList", [])
            all_props.extend(elements)
            pages_ok += 1
            logger.info(f"[id-backfill] Page {page}: {len(elements)} props")

            if len(elements) < request.maxItems:
                break

            await asyncio.sleep(1)

            # Rotar TLS session cada 3 páginas
            if page % 3 == 0:
                tls_impl.close()
                idealista_client, tls_impl, proxy_url = await _get_idealista_client()

        except Exception as e:
            logger.error(f"[id-backfill] Page {page} error: {e}")
            break

    # Upsert
    inserted = 0
    updated = 0
    if all_props:
        async with httpx.AsyncClient() as upsert_client:
            ok, inserted, updated, err = await upsert_to_supabase(upsert_client, all_props)

    elapsed = time.time() - start
    logger.info(f"[id-backfill] Done: {len(all_props)} scraped, {inserted} new, {updated} updated in {elapsed:.1f}s")

    return {
        "success": True,
        "locationIds": request.locationIds,
        "pages_scraped": pages_ok,
        "total_scraped": len(all_props),
        "inserted": inserted,
        "updated": updated,
        "time_seconds": round(elapsed, 1),
    }
    
@app.post("/idealista/recheck")
@app.get("/idealista/recheck")
async def idealista_recheck():
    """
    El recheck de Idealista ahora es un worker continuo.
    Este endpoint devuelve el estado actual.
    """
    return {
        "running": recheck_state["running"],
        "mode": recheck_state["mode"],
        "totals": recheck_state["totals"],
        "last_batch_at": recheck_state["last_batch_at"],
        "message": "ID recheck runs continuously as a background worker",
    }


# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
#  IDEALISTA ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Endpoints (sin cambios)
# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

@app.post("/scrape-updates")
async def scrape_updates(request: ScrapeUpdatesRequest):
    """
    Obtiene las propiedades mÃƒÆ’Ã‚Â¡s recientes (por fecha de publicaciÃƒÆ’Ã‚Â³n)
    y las inserta/actualiza en Supabase.
    DiseÃƒÆ’Ã‚Â±ado para ser llamado por n8n via CRON.
    """
    start_time = time.time()

    logger.info(f"[scrape-updates] Starting for locationIds: {request.locationIds}")

    proxy_url = os.environ.get("PROXY_URL")
    client_kwargs = {}
    if proxy_url:
        client_kwargs["proxies"] = proxy_url

    async with httpx.AsyncClient() as client:
        tls_impl = TlsClientImpl(proxy_url=proxy_url)
        idealista = Idealista(tls_impl)

        now = time.time()
        if jwt_token_cache["token"] and jwt_token_cache["expires_at"] > now + 60:
            idealista.jwt_token = jwt_token_cache["token"]
        else:
            logger.info("[scrape-updates] Requesting new JWT token...")
            success, result = await idealista.update_token()
            if not success:
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to get JWT token: {result}"
                )
            jwt_token_cache["token"] = result["token"]
            parsed = parse_jwt_token(result["token"])
            if parsed:
                jwt_token_cache["expires_at"] = parsed.get("exp", 0)
            idealista.jwt_token = result["token"]

        success, response = await idealista.search_properties_by_location_id(
            country="es",
            max_items=request.maxItems,
            location_ids=request.locationIds,
            property_type=request.propertyType,
            operation=request.operation,
            num_page=1,
            order="publicationDate",
            locale="es",
            quality="high",
            gallery=True,
        )

        if not success:
            raise HTTPException(
                status_code=502,
                detail=f"Idealista search failed: {response}"
            )

        elements = response.get("elementList", [])
        total_available = response.get("total", 0)
        scrape_time = time.time() - start_time

        logger.info(f"[scrape-updates] Scraped {len(elements)} properties in {scrape_time:.1f}s")

        upsert_start = time.time()
        success, inserted, updated, error_msg = await upsert_to_supabase(client, elements)
        upsert_time = time.time() - upsert_start

        total_time = time.time() - start_time

        result = {
            "success": success,
            "locationIds": request.locationIds,
            "scraped": len(elements),
            "total_available": total_available,
            "inserted": inserted,
            "updated": updated,
            "scrape_time_seconds": round(scrape_time, 2),
            "upsert_time_seconds": round(upsert_time, 2),
            "total_time_seconds": round(total_time, 2),
        }

        if error_msg:
            result["error"] = error_msg

        logger.info(f"[scrape-updates] Completed: {inserted} inserted, {updated} updated in {total_time:.1f}s")
        # --- Trigger mirror si hay nuevas propiedades ---
        if inserted > 0:
            _trigger_mirror()
        return result


@app.post("/scrape")
async def scrape(request: ScrapeRequest):
    use_location_mode = request.locationIds is not None

    if use_location_mode:
        logger.info(f"Using LOCATION ID mode: {request.locationIds}")
    else:
        if request.coordinates is None:
            raise HTTPException(status_code=400, detail="Must provide either 'coordinates' or 'locationIds'")
        try:
            coordinates = json.loads(request.coordinates)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid coordinates JSON: {e}")

        if not isinstance(coordinates, list) or len(coordinates) < 3:
            raise HTTPException(status_code=400, detail="Need at least 3 coordinate points")

        logger.info(f"Using COORDINATES mode: {len(coordinates)} points")

    proxy_url = os.environ.get("PROXY_URL")

    async with httpx.AsyncClient() as client:
        tls_impl = TlsClientImpl(proxy_url=proxy_url)
        idealista = Idealista(tls_impl)

        now = time.time()
        if jwt_token_cache["token"] and jwt_token_cache["expires_at"] > now + 60:
            idealista.jwt_token = jwt_token_cache["token"]
            logger.info("Using cached JWT token")
        else:
            logger.info("Requesting new JWT token...")
            success, result = await idealista.update_token()
            if not success:
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to get JWT token: {result}"
                )

            jwt_token_cache["token"] = result["token"]
            parsed = parse_jwt_token(result["token"])
            if parsed:
                jwt_token_cache["expires_at"] = parsed.get("exp", 0)

            idealista.jwt_token = result["token"]
            logger.info("JWT token obtained successfully")

        if use_location_mode:
            logger.info(f"Searching by locationId: {request.locationIds}, op={request.operation}, page={request.numPage}")

            success, response = await idealista.search_properties_by_location_id(
                country="es",
                max_items=request.maxItems,
                location_ids=request.locationIds,
                property_type=request.propertyType,
                operation=request.operation,
                num_page=request.numPage,
                order=request.order,
                locale="es",
                quality="high",
                gallery=True,
                exterior=request.exterior,
                minPrice=request.minPrice,
                maxPrice=request.maxPrice,
                minSize=request.minSize,
                maxSize=request.maxSize,
                bedrooms=request.bedrooms,
                bathrooms=request.bathrooms,
                preservations=request.preservations,
                flat=request.flat,
                onlyFlats=request.onlyFlats,
                duplex=request.duplex,
                penthouse=request.penthouse,
                chalet=request.chalet,
                countryHouse=request.countryHouse,
                independentHouse=request.independentHouse,
                semidetachedHouse=request.semidetachedHouse,
                terracedHouse=request.terracedHouse,
                loftType=request.loftType,
                casaBajaType=request.casaBajaType,
                villaType=request.villaType,
                apartamentoType=request.apartamentoType,
                accessible=request.accessible,
                airConditioning=request.airConditioning,
                storeRoom=request.storeRoom,
                builtinWardrobes=request.builtinWardrobes,
                garage=request.garage,
                swimmingPool=request.swimmingPool,
                elevator=request.elevator,
                luxury=request.luxury,
                terrace=request.terrace,
                garden=request.garden,
                floorHeights=request.floorHeights,
                currentOccupationType=request.currentOccupationType,
            )
        else:
            logger.info(f"Searching by coordinates: {len(coordinates)} points, op={request.operation}, page={request.numPage}")

            success, response = await idealista.search_properties_by_coordinates(
                country="es",
                max_items=request.maxItems,
                coordinates=coordinates,
                property_type=request.propertyType,
                operation=request.operation,
                num_page=request.numPage,
                order=request.order,
                locale="es",
                quality="high",
                gallery=True,
                exterior=request.exterior,
                minPrice=request.minPrice,
                maxPrice=request.maxPrice,
                minSize=request.minSize,
                maxSize=request.maxSize,
                bedrooms=request.bedrooms,
                bathrooms=request.bathrooms,
                preservations=request.preservations,
                flat=request.flat,
                onlyFlats=request.onlyFlats,
                duplex=request.duplex,
                penthouse=request.penthouse,
                chalet=request.chalet,
                countryHouse=request.countryHouse,
                independentHouse=request.independentHouse,
                semidetachedHouse=request.semidetachedHouse,
                terracedHouse=request.terracedHouse,
                loftType=request.loftType,
                casaBajaType=request.casaBajaType,
                villaType=request.villaType,
                apartamentoType=request.apartamentoType,
                accessible=request.accessible,
                airConditioning=request.airConditioning,
                storeRoom=request.storeRoom,
                builtinWardrobes=request.builtinWardrobes,
                garage=request.garage,
                swimmingPool=request.swimmingPool,
                elevator=request.elevator,
                luxury=request.luxury,
                terrace=request.terrace,
                garden=request.garden,
                floorHeights=request.floorHeights,
                currentOccupationType=request.currentOccupationType,
            )

        if not success:
            raise HTTPException(
                status_code=502,
                detail=f"Idealista search failed: {response}"
            )

        total = response.get("total", 0)
        elements = response.get("elementList", [])
        logger.info(f"Found {len(elements)} properties (total available: {total})")

        return response


# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â
#  FOTOCASA ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â Endpoints
# ÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚ÂÃƒÂ¢Ã¢â‚¬Â¢Ã‚Â

@app.post("/fotocasa/scrape")
async def fotocasa_scrape(request: FotocasaScrapeRequest):
    """
    Busca propiedades en Fotocasa por coordenadas (boundingBox).
    Equivalente al endpoint de Apify search_properties_by_bounding_box.
    """
    logger.info(f"[fotocasa/scrape] Starting coordinates search: lat={request.latitude}, lng={request.longitude}")

    proxy_url = os.environ.get("PROXY_URL")
    tls_impl = TlsClientImpl(proxy_url=proxy_url, client_identifier="chrome_120")
    fotocasa = Fotocasa(tls_impl)

    # Auto-calcular mapBoundingBox si hay polígono y no se envió explícitamente
    if request.polygon and not request.mapBoundingBox:
        _, _, auto_bbox = fotocasa._calculate_polygon_center_and_bbox(request.polygon)
        if auto_bbox:
            request.mapBoundingBox = auto_bbox
            logger.info(f"[fotocasa/scrape] Auto-calculated mapBoundingBox from polygon")

    # Inicializar sesion (obtener CSRF token)
    init_ok, init_result = await fotocasa.initialize_session()
    if not init_ok:
        logger.warning(f"[fotocasa/scrape] Session init failed: {init_result} - continuing anyway")

    # Ejecutar busqueda
    success, response = await fotocasa.search_properties_by_coordinates(
        property_type=request.propertyType,
        transaction_type=request.transactionType,
        latitude=request.latitude,
        longitude=request.longitude,
        polygon=request.polygon,
        map_bounding_box=request.mapBoundingBox,
        page=request.page,
        page_size=request.pageSize,
        sort=request.sort,
        zoom=request.zoom,
        purchase_type=request.purchaseType,
        rental_duration=request.rentalDuration,
        property_sub_types=request.propertySubTypes,
        rooms_from=request.roomsFrom,
        rooms_to=request.roomsTo,
        bathrooms_from=request.bathroomsFrom,
        bathrooms_to=request.bathroomsTo,
        price_from=request.priceFrom,
        price_to=request.priceTo,
        surface_from=request.surfaceFrom,
        surface_to=request.surfaceTo,
        conservation_states=request.conservationStates,
        extras=request.extras,
        orientations=request.orientations,
        occupancy_status=request.occupancyStatus,
    )

    if not success:
        raise HTTPException(
            status_code=502,
            detail=f"Fotocasa search failed: {response}"
        )

    logger.info(f"[fotocasa/scrape] Search completed successfully")
    return response


@app.post("/fotocasa/scrape-locations")
async def fotocasa_scrape_locations(request: FotocasaLocationsRequest):
    """
    Busca propiedades en Fotocasa por location codes.
    Ej: Madrid provincia = "724,14,28,0,0,0,0,0,0"
    """
    logger.info(f"[fotocasa/scrape-locations] Starting location search: {request.locations}")

    proxy_url = os.environ.get("PROXY_URL")
    client_kwargs = {}
    if proxy_url:
        client_kwargs["proxies"] = proxy_url

    async with httpx.AsyncClient(**client_kwargs) as client:
        httpx_client = HttpxClient(client)
        fotocasa = Fotocasa(httpx_client)

        # Inicializar sesiÃƒÆ’Ã‚Â³n
        init_ok, init_result = await fotocasa.initialize_session()
        if not init_ok:
            logger.warning(f"[fotocasa/scrape-locations] Session init failed: {init_result} ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â continuing anyway")

        # Ejecutar bÃƒÆ’Ã‚Âºsqueda
        success, response = await fotocasa.search_properties_by_locations(
            property_type=request.propertyType,
            transaction_type=request.transactionType,
            locations=request.locations,
            page=request.page,
            page_size=request.pageSize,
            sort=request.sort,
            conservation_states=request.conservationStates,
            purchase_type=request.purchaseType,
            rental_duration=request.rentalDuration,
            property_sub_types=request.propertySubTypes,
            rooms_from=request.roomsFrom,
            rooms_to=request.roomsTo,
            bathrooms_from=request.bathroomsFrom,
            bathrooms_to=request.bathroomsTo,
            price_from=request.priceFrom,
            price_to=request.priceTo,
            surface_from=request.surfaceFrom,
            surface_to=request.surfaceTo,
            extras=request.extras,
            occupancy_status=request.occupancyStatus,
        )

        if not success:
            raise HTTPException(
                status_code=502,
                detail=f"Fotocasa location search failed: {response}"
            )

        logger.info(f"[fotocasa/scrape-locations] Search completed successfully")
        return response


@app.post("/fotocasa/detail")
async def fotocasa_detail(request: FotocasaDetailRequest):
    """
    Obtiene detalle completo de una propiedad de Fotocasa.
    Opcionalmente obtiene token fresco del servidor Hetzner.
    NOTA: Si useHetznerToken=true, la respuesta puede tardar ~2 minutos.
    """
    logger.info(f"[fotocasa/detail] Getting detail for property: {request.propertyId}")

    external_token = None

    # 1. Obtener token de Hetzner si se solicita
    if request.useHetznerToken:
        # Usa slot 12 (compartido con recheck — no interferir con workers dedicados)
        detail_token_url = _FC_SLOT_URLS[12]
        logger.info(f"[fotocasa/detail] Requesting token from Hetzner slot 12: {detail_token_url}")
        try:
            async with httpx.AsyncClient(timeout=300.0) as token_client:
                token_response = await token_client.get(detail_token_url)
                if token_response.status_code == 200:
                    token_data = token_response.json()
                    external_token = token_data.get("token")
                    exec_time = token_data.get("execution_time_seconds", "unknown")
                    logger.info(f"[fotocasa/detail] Hetzner token obtained in {exec_time}s")
                else:
                    logger.warning(f"[fotocasa/detail] Hetzner token failed: {token_response.status_code}")
        except Exception as e:
            logger.error(f"[fotocasa/detail] Hetzner token error: {e}")
            raise HTTPException(
                status_code=502,
                detail=f"Failed to get Hetzner token: {str(e)}"
            )

    # 2. Hacer la peticiÃƒÆ’Ã‚Â³n de detalle
    proxy_url = os.environ.get("PROXY_URL")
    client_kwargs = {}
    if proxy_url:
        client_kwargs["proxies"] = proxy_url

    async with httpx.AsyncClient(**client_kwargs) as client:
        httpx_client = HttpxClient(client)
        fotocasa = Fotocasa(httpx_client)

        success, response = await fotocasa.get_property_detail(
            property_id=request.propertyId,
            transaction_type=request.transactionType,
            external_token=external_token,
        )

        if not success:
            raise HTTPException(
                status_code=502,
                detail=f"Fotocasa detail failed: {response}"
            )

        logger.info(f"[fotocasa/detail] Detail obtained for property {request.propertyId}")
        return response


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOTOCASA — Enrich Endpoints (control del worker)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/fotocasa/enrich-status")
async def fotocasa_enrich_status():
    """Estado del worker de enrich Fotocasa."""
    pending = 0
    if db_pool:
        try:
            row = await db_pool.fetchrow(
                "SELECT COUNT(*) as cnt FROM async_properties WHERE source='fotocasa' AND detail_enriched_at IS NULL AND adisactive=true"
            )
            pending = row["cnt"] if row else 0
        except Exception:
            pass

    return {
        "running": fc_enrich_state["running"],
        "status": fc_enrich_state["status"],
        "started_at": fc_enrich_state["started_at"],
        "totals": fc_enrich_state["totals"],
        "pending": pending,
        "token_slots": {
            f"slot{i}": {
                "cache_age": round(time.time() - _fc_token_slots[i]["obtained_at"]) if _fc_token_slots[i]["obtained_at"] else None,
                "requests_since_refresh": _fc_token_slots[i]["requests_since"],
            }
            for i in range(1, 17)
        },
        "slot_assignment": "enrich→slots 1-3 | check-active→slots 4-11 (3cap+3main+2rest) | recheck+detail→slot 12",
    }


@app.post("/fotocasa/enrich-start")
async def fotocasa_enrich_start():
    """Arranca el worker de enrich Fotocasa (si no está corriendo)."""
    if fc_enrich_state["running"]:
        return {"success": True, "message": "FC Enrich already running"}
    await _start_fc_enrich()
    return {"success": True, "message": "FC Enrich worker started"}


@app.post("/fotocasa/enrich-stop")
async def fotocasa_enrich_stop():
    """Detiene el worker de enrich Fotocasa."""
    fc_enrich_state["running"] = False
    fc_enrich_state["status"] = "stopping"
    return {"success": True, "message": "FC Enrich worker stopping"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FOTOCASA — Check Active Endpoints (control de workers)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/fotocasa/workers-status")
async def fotocasa_workers_status():
    """Estado de los workers de Fotocasa Check Active + Enrich."""
    fc_ca_elapsed = 0
    if fc_check_active_state["started_at"]:
        start = datetime.fromisoformat(fc_check_active_state["started_at"])
        fc_ca_elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    fc_ca_hours = fc_ca_elapsed / 3600 if fc_ca_elapsed > 0 else 1
    fc_ca_speed = fc_check_active_state["totals"]["checked"] / fc_ca_hours

    # Pendientes de check en cada tier — query única con CASE (evita 3 COUNTs lentos)
    pending_by_tier = {}
    if db_pool:
        try:
            now_utc = datetime.now(timezone.utc)
            cutoff_12  = now_utc - timedelta(hours=12)
            cutoff_48  = now_utc - timedelta(hours=48)
            cutoff_120 = now_utc - timedelta(hours=120)
            rows = await db_pool.fetch("""
                SELECT tier, COUNT(*) as cnt FROM (
                    SELECT CASE
                        WHEN province IN ('Madrid','Barcelona','Valencia')
                             AND TRIM(municipality) IN ('Madrid Capital','Barcelona Capital','Valencia Capital')
                        THEN 'capitals'
                        WHEN province IN ('Madrid','Barcelona','Valencia','Sevilla','Bizkaia',
                             'Alicante','Málaga','Zaragoza','Illes Balears','Las Palmas')
                        THEN 'main_provinces'
                        ELSE 'rest'
                    END as tier,
                    last_checked_at
                    FROM async_properties
                    WHERE source = 'fotocasa' AND adisactive = true
                ) sub
                WHERE (tier = 'capitals'        AND (last_checked_at IS NULL OR last_checked_at < $1))
                   OR (tier = 'main_provinces'  AND (last_checked_at IS NULL OR last_checked_at < $2))
                   OR (tier = 'rest'            AND (last_checked_at IS NULL OR last_checked_at < $3))
                GROUP BY tier
            """, cutoff_12, cutoff_48, cutoff_120)
            for row in rows:
                pending_by_tier[row["tier"]] = row["cnt"]
            # Asegurar que los 3 tiers aparecen (0 si no hay pendientes)
            for t in ("capitals", "main_provinces", "rest"):
                pending_by_tier.setdefault(t, 0)
        except Exception as e:
            logger.warning(f"[fc-workers-status] Pending count failed: {e}")
            pending_by_tier = {"capitals": "error", "main_provinces": "error", "rest": "error"}

    return {
        "fc_check_active": {
            "running": fc_check_active_state["running"],
            "started_at": fc_check_active_state["started_at"],
            "elapsed_hours": round(fc_ca_elapsed / 3600, 1),
            "speed_per_hour": round(fc_ca_speed),
            "workers": fc_check_active_state["workers"],
            "totals": fc_check_active_state["totals"],
            "tiers": [{"name": t["name"], "cycle_hours": t["cycle_hours"]} for t in FC_CHECK_ACTIVE_TIERS],
            "pending_by_tier": pending_by_tier,
            "throttle": {
                "level": _fc_throttle["current_level"],
                "window_events": len(_fc_throttle["events"]),
                "window_errors": sum(1 for _, e in _fc_throttle["events"] if e == "403"),
                "error_rate": (
                    round(sum(1 for _, e in _fc_throttle["events"] if e == "403") / max(len(_fc_throttle["events"]), 1) * 100, 1)
                ),
            },
        },
        "fc_enrich": {
            "running": fc_enrich_state["running"],
            "status": fc_enrich_state["status"],
            "workers": fc_enrich_state["workers"],
            "totals": fc_enrich_state["totals"],
        },
        "fc_recheck": {
            "running": fc_recheck_state["running"],
            "mode": fc_recheck_state["mode"],
            "totals": fc_recheck_state["totals"],
            "last_batch_at": fc_recheck_state["last_batch_at"],
        },
        "token_slots": {
            f"slot{i}": {
                "age_s": round(time.time() - _fc_token_slots[i]["obtained_at"]) if _fc_token_slots[i]["obtained_at"] else None,
                "reqs": _fc_token_slots[i]["requests_since"],
                "android_id": (_fc_token_slots[i].get("device_info") or {}).get("android_id"),
            }
            for i in range(1, 17)
        },
        "fc_scrape_updates": {
            "running": fc_scrape_updates_state["running"],
            "started_at": fc_scrape_updates_state["started_at"],
            "workers": fc_scrape_updates_state["workers"],
            "totals": fc_scrape_updates_state["totals"],
        },
    }


@app.post("/fotocasa/workers-stop")
async def fotocasa_workers_stop():
    """Detiene todos los workers de Fotocasa (check-active, enrich, scrape-updates, recheck)."""
    fc_check_active_state["running"] = False
    fc_enrich_state["running"] = False
    fc_scrape_updates_state["running"] = False
    fc_recheck_state["running"] = False
    return {"success": True, "message": "Stop signal sent to all Fotocasa workers (including enrich + recheck)"}

@app.post("/fotocasa/workers-restart")
async def fotocasa_workers_restart():
    """Reinicia todos los workers de Fotocasa (check-active, enrich, scrape-updates, recheck)."""
    fc_check_active_state["running"] = False
    fc_enrich_state["running"] = False
    fc_scrape_updates_state["running"] = False
    fc_recheck_state["running"] = False
    await asyncio.sleep(5)
    await _start_fc_check_active()
    await _start_fc_enrich()
    await _start_fc_scrape_updates()
    await _start_fc_recheck()
    return {"success": True, "message": "All Fotocasa workers restarted (including enrich + recheck)"}


@app.post("/fotocasa/recheck")
@app.get("/fotocasa/recheck")
async def fotocasa_recheck():
    """
    El recheck de Fotocasa ahora es un worker continuo (slot 12).
    Este endpoint devuelve el estado actual.
    """
    return {
        "running": fc_recheck_state["running"],
        "mode": fc_recheck_state["mode"],
        "totals": fc_recheck_state["totals"],
        "last_batch_at": fc_recheck_state["last_batch_at"],
        "message": "FC recheck runs continuously as a background worker",
    }


# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â
#  FOTOCASA Ã¢â‚¬â€ Scrape Updates (upsert a Supabase)
# Ã¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢ÂÃ¢â€¢Â

@app.post("/fotocasa/scrape-updates")
async def fotocasa_scrape_updates(request: FotocasaScrapeUpdatesRequest):
    """
    Obtiene las propiedades mas recientes de Fotocasa (sort=8 = mas recientes)
    y las inserta/actualiza en Supabase.
    DiseÃƒÂ±ado para ser llamado por n8n via CRON.
    Scrapea N paginas (default 2) con delay entre ellas.
    """
    import asyncio
    start_time = time.time()

    logger.info(f"[fotocasa/scrape-updates] Starting for locations: {request.locations}, pages: {request.pages}")

    proxy_url = os.environ.get("PROXY_URL")
    client_kwargs = {}
    if proxy_url:
        client_kwargs["proxies"] = proxy_url

    async with httpx.AsyncClient(**client_kwargs) as client:
        httpx_client = HttpxClient(client)
        fotocasa = Fotocasa(httpx_client)

        # Inicializar sesion (obtener CSRF token)
        init_ok, init_result = await fotocasa.initialize_session()
        if not init_ok:
            logger.warning(f"[fotocasa/scrape-updates] Session init failed: {init_result} Ã¢â‚¬â€ continuing anyway")

        all_properties = []
        total_available = 0

        # Scrapear N paginas con sort=8 (mas recientes)
        for page_num in range(1, request.pages + 1):
            logger.info(f"[fotocasa/scrape-updates] Scraping page {page_num}/{request.pages}...")

            success, response = await fotocasa.search_properties_by_locations(
                property_type=request.propertyType,
                transaction_type=request.transactionType,
                locations=request.locations,
                page=page_num,
                page_size=request.pageSize,
                sort=8,  # 8 = ordenar por mas recientes (hardcoded)
                conservation_states="UNDEFINED",
            )

            if not success:
                logger.error(f"[fotocasa/scrape-updates] Page {page_num} failed: {response}")
                continue

            # Extraer propiedades del array placeholders
            placeholders = response.get("placeholders", [])
            page_properties = []
            for ph in placeholders:
                if ph.get("type") == "PROPERTY" and ph.get("property"):
                    page_properties.append(ph["property"])

            # Tambien extraer de newConstructionPlaceholders si existen
            new_construction = response.get("newConstructionPlaceholders", [])
            for ph in new_construction:
                if ph.get("type") == "PROPERTY" and ph.get("property"):
                    page_properties.append(ph["property"])

            all_properties.extend(page_properties)

            # Obtener total disponible (solo de la primera pagina)
            if page_num == 1:
                info = response.get("info", {})
                total_available = int(info.get("count", 0))

            logger.info(f"[fotocasa/scrape-updates] Page {page_num}: {len(page_properties)} properties")

            # Delay entre paginas para evitar rate limiting
            if page_num < request.pages:
                await asyncio.sleep(2)

        scrape_time = time.time() - start_time
        logger.info(f"[fotocasa/scrape-updates] Scraped {len(all_properties)} properties in {scrape_time:.1f}s")

        # Upsert a Supabase
        upsert_start = time.time()
        success, inserted, updated, error_msg = await upsert_fotocasa_to_supabase(client, all_properties)
        upsert_time = time.time() - upsert_start

        total_time = time.time() - start_time

        result = {
            "success": success,
            "locations": request.locations,
            "pages_scraped": request.pages,
            "scraped": len(all_properties),
            "total_available": total_available,
            "inserted": inserted,
            "updated": updated,
            "scrape_time_seconds": round(scrape_time, 2),
            "upsert_time_seconds": round(upsert_time, 2),
            "total_time_seconds": round(total_time, 2),
        }

        if error_msg:
            result["error"] = error_msg

        logger.info(f"[fotocasa/scrape-updates] Completed: {inserted} inserted, {updated} updated in {total_time:.1f}s")

        return result


# Ã¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”Â
#  HABITACLIA Ã¢â‚¬â€ Endpoints
# Ã¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”ÂÃ¢â€”Â

@app.post("/habitaclia/scrape")
async def habitaclia_scrape(request: HabitacliaScrapeRequest):
    """
    Busca propiedades en Habitaclia por coordenadas (bounding box).
    Devuelve clusters con propiedades. Si fetchDetails=true,
    tambiÃƒÂ©n obtiene el detalle fino de cada propiedad encontrada.
    No requiere proxy.
    """
    logger.info(
        f"[habitaclia/scrape] Bounding box: ({request.minLat},{request.minLon}) "
        f"Ã¢â€ â€™ ({request.maxLat},{request.maxLon})"
    )

    async with httpx.AsyncClient() as client:
        httpx_client = HttpxClient(client)
        habitaclia = Habitaclia(httpx_client)

        # 1. BÃƒÂºsqueda por bounding box
        success, response = await habitaclia.search_by_bounding_box(
            min_lat=request.minLat,
            min_lon=request.minLon,
            max_lat=request.maxLat,
            max_lon=request.maxLon,
            operation=request.operation,
            property_types=request.propertyTypes,
            surface_min=request.surfaceMin,
            rooms_min=request.roomsMin,
            price_max=request.priceMax,
            only_with_photos=request.onlyWithPhotos,
            advanced_filters=request.advancedFilters,
            publication_period=request.publicationPeriod,
            zoom_level=request.zoomLevel,
            cluster_distance=request.clusterDistance,
            cod_prov=request.codProv,
            cod_com=request.codCom,
        )

        if not success:
            raise HTTPException(status_code=502, detail=f"Habitaclia search failed: {response}")

        # 2. Extraer propiedades de los clusters
        properties = habitaclia.extract_properties_from_clusters(response)

        # 3. Si fetchDetails, obtener detalle de cada propiedad
        details = []
        if request.fetchDetails and properties:
            logger.info(f"[habitaclia/scrape] Fetching details for {len(properties)} properties...")
            for i, prop in enumerate(properties):
                detail_ok, detail_data = await habitaclia.get_property_detail(
                    prop["CodEmp"], prop["CodInm"]
                )
                if detail_ok:
                    details.append(detail_data)
                else:
                    logger.warning(f"[habitaclia/scrape] Detail failed for {prop['CodEmp']}-{prop['CodInm']}")

                # Delay cada 5 propiedades para evitar rate-limiting
                if (i + 1) % 5 == 0:
                    await asyncio.sleep(1)

        result = {
            "clusters_raw": response,
            "properties_extracted": len(properties),
            "properties": properties,
        }
        if request.fetchDetails:
            result["details_fetched"] = len(details)
            result["details"] = details

        logger.info(f"[habitaclia/scrape] Done: {len(properties)} properties extracted")
        return result


@app.post("/habitaclia/discover-tree")
async def habitaclia_discover_tree(request: HabitacliaDiscoverTreeRequest):
    """
    Descubre el ÃƒÂ¡rbol completo de ubicaciones de una provincia.
    Navega recursivamente por las capas (provincia Ã¢â€ â€™ comarca Ã¢â€ â€™ poblaciÃƒÂ³n)
    hasta llegar a los endpoints scrapeables.

    Guarda los leaf_nodes en Supabase (tabla habitaclia_tree) via upsert.
    """
    start_time = time.time()
    logger.info(f"[habitaclia/discover-tree] Discovering: {request.locationCode}")

    async with httpx.AsyncClient() as client:
        httpx_client = HttpxClient(client)
        habitaclia = Habitaclia(httpx_client)

        success, response = await habitaclia.discover_location_tree(
            location_code=request.locationCode,
            operation=request.operation,
            property_type=request.propertyType,
        )

        if not success:
            raise HTTPException(status_code=502, detail=f"Tree discovery failed: {response}")

        leaf_nodes = response.get("leaf_nodes", [])
        discover_time = time.time() - start_time

        # Upsert a Supabase
        upsert_start = time.time()
        upsert_ok, inserted, updated, error_msg = await upsert_tree_to_supabase(
            client=client,
            leaf_nodes=leaf_nodes,
            location_code=request.locationCode,
            operation=request.operation,
            property_type=request.propertyType,
        )
        upsert_time = time.time() - upsert_start

        total_time = time.time() - start_time

        result = {
            "success": upsert_ok,
            "location_code": request.locationCode,
            "operation": request.operation,
            "property_type": request.propertyType,
            "total_leaf_nodes": len(leaf_nodes),
            "inserted": inserted,
            "updated": updated,
            "discover_time_seconds": round(discover_time, 2),
            "upsert_time_seconds": round(upsert_time, 2),
            "total_time_seconds": round(total_time, 2),
        }

        if error_msg:
            result["error"] = error_msg

        logger.info(
            f"[habitaclia/discover-tree] Done: {len(leaf_nodes)} leaf nodes "
            f"({inserted} new, {updated} updated) in {total_time:.1f}s"
        )
        return result


@app.post("/habitaclia/scrape-listings")
async def habitaclia_scrape_listings(request: HabitacliaListingsRequest):
    """
    Scrapea todos los listings de un slug con paginaciÃƒÂ³n automÃƒÂ¡tica.
    El slug se obtiene del endpoint /habitaclia/discover-tree (campo listing_slug).

    Ejemplo:
      listing_slug="viviendas-albacete" Ã¢â€ â€™ scrapea todas las pÃƒÂ¡ginas de Albacete

    Si fetchDetails=true, obtiene el detalle fino de cada propiedad
    usando CodEmp/CodInm del listing.
    """
    logger.info(f"[habitaclia/scrape-listings] Scraping: {request.listingSlug}")

    async with httpx.AsyncClient() as client:
        httpx_client = HttpxClient(client)
        habitaclia = Habitaclia(httpx_client)

        # 1. Scrape paginado
        success, response = await habitaclia.scrape_listings(
            listing_slug=request.listingSlug,
            sort=request.sort,
            max_pages=request.maxPages,
        )

        if not success:
            raise HTTPException(status_code=502, detail=f"Listings scrape failed: {response}")

        # 2. Si fetchDetails, obtener detalle de cada ad
        details = []
        if request.fetchDetails:
            ads = response.get("ads", [])
            logger.info(f"[habitaclia/scrape-listings] Fetching details for {len(ads)} ads...")

            for i, ad in enumerate(ads):
                cod_emp = ad.get("CodEmp")
                cod_inm = ad.get("CodInm")
                if cod_emp is not None and cod_inm is not None:
                    detail_ok, detail_data = await habitaclia.get_property_detail(cod_emp, cod_inm)
                    if detail_ok:
                        details.append(detail_data)
                    else:
                        logger.warning(
                            f"[habitaclia/scrape-listings] Detail failed for {cod_emp}-{cod_inm}"
                        )

                    # Delay cada 5 propiedades
                    if (i + 1) % 5 == 0:
                        await asyncio.sleep(1)

            response["details_fetched"] = len(details)
            response["details"] = details

        logger.info(
            f"[habitaclia/scrape-listings] Done: {response.get('scraped', 0)} ads, "
            f"{response.get('pages_scraped', 0)} pages"
        )
        return response


@app.post("/habitaclia/detail")
async def habitaclia_detail(request: HabitacliaDetailRequest):
    """
    Obtiene el detalle completo de una propiedad individual de Habitaclia.
    Requiere CodEmp y CodInm (obtenidos de los listings o clusters).
    """
    logger.info(f"[habitaclia/detail] Getting detail: CodEmp={request.codEmp}, CodInm={request.codInm}")

    async with httpx.AsyncClient() as client:
        httpx_client = HttpxClient(client)
        habitaclia = Habitaclia(httpx_client)

        success, response = await habitaclia.get_property_detail(
            cod_emp=request.codEmp,
            cod_inm=request.codInm,
        )

        if not success:
            raise HTTPException(status_code=502, detail=f"Habitaclia detail failed: {response}")

        logger.info(f"[habitaclia/detail] Detail obtained for {request.codEmp}-{request.codInm}")
        return response

@app.post("/habitaclia/scrape-updates")
async def habitaclia_scrape_updates(request: HabitacliaScrapeUpdatesRequest):
    """
    Obtiene las propiedades mÃƒÂ¡s recientes de Habitaclia para una provincia
    y las inserta/actualiza en Supabase.
    DiseÃƒÂ±ado para ser llamado por n8n via CRON.

    Flujo:
    1. Consulta habitaclia_tree en Supabase para la provincia dada
    2. Filtra hijos directos (profundidad 1 en el path)
    3. is_aggregate=false (capitales) Ã¢â€ â€™ scrapea capitalLimit (default 40) mÃƒÂ¡s recientes
    4. is_aggregate=true  (comarcas)  Ã¢â€ â€™ scrapea comarcaLimit (default 10) mÃƒÂ¡s recientes
    5. Upsert POR ZONA y devuelve desglose inserted/updated por zona
    """
    start_time = time.time()
    logger.info(f"[habitaclia/scrape-updates] Starting for: {request.locationCode}")

    # Ã¢â€â‚¬Ã¢â€â‚¬ 1. Consultar habitaclia_tree en Supabase Ã¢â€â‚¬Ã¢â€â‚¬
    async with httpx.AsyncClient() as client:
        headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Content-Type": "application/json",
        }

        tree_url = (
            f"{SUPABASE_URL}/habitaclia_tree"
            f"?location_code=eq.{request.locationCode}"
            f"&select=name,listing_slug,is_aggregate,path,num_ads"
        )
        tree_resp = await client.get(tree_url, headers=headers, timeout=30)

        if tree_resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Error querying habitaclia_tree: {tree_resp.status_code} - {tree_resp.text[:200]}"
            )

        all_rows = tree_resp.json()
        if not all_rows:
            raise HTTPException(
                status_code=404,
                detail=f"No rows found in habitaclia_tree for location_code='{request.locationCode}'. "
                       f"Run /habitaclia/discover-tree first."
            )

        # Ã¢â€â‚¬Ã¢â€â‚¬ 2. Filtrar hijos directos (path con exactamente un " > ") Ã¢â€â‚¬Ã¢â€â‚¬
        direct_children = [r for r in all_rows if r["path"].count(" > ") == 1]

        if not direct_children:
            raise HTTPException(
                status_code=404,
                detail=f"No direct children found for '{request.locationCode}'"
            )

        capitals = [r for r in direct_children if not r["is_aggregate"]]
        all_comarcas = [r for r in direct_children if r["is_aggregate"]]

        # Separar comarcas whitelist (se tratan como capitales) de comarcas regulares
        whitelist_names = HABITACLIA_COMARCAS_WHITELIST.get(request.locationCode, set())
        comarcas_whitelist = [r for r in all_comarcas if r["name"] in whitelist_names]
        comarcas = [r for r in all_comarcas if r["name"] not in whitelist_names]

        logger.info(
            f"[habitaclia/scrape-updates] Found {len(capitals)} capital(s), "
            f"{len(comarcas_whitelist)} whitelist comarca(s), "
            f"{len(comarcas)} regular comarca(s) for '{request.locationCode}'"
        )

        # Ã¢â€â‚¬Ã¢â€â‚¬ 3. Instanciar cliente Habitaclia Ã¢â€â‚¬Ã¢â€â‚¬
        httpx_client = HttpxClient(client)
        habitaclia = Habitaclia(httpx_client)

        zones_scraped = []
        zones_failed = []
        total_inserted = 0
        total_updated = 0
        total_enriched = 0

        # -- Helper: scrapear una zona, hacer upsert y enriquecer nuevas --
        async def scrape_and_upsert_zone(zone, zone_type, ads_limit):
            nonlocal total_inserted, total_updated, total_enriched

            slug = zone["listing_slug"]
            zone_name = zone["name"]

            logger.info(
                f"[habitaclia/scrape-updates] Scraping {zone_type} '{zone_name}' "
                f"-> {slug} (limit {ads_limit})"
            )

            try:
                success, result = await habitaclia.scrape_listings(
                    listing_slug=slug,
                    sort="mas_recientes",
                    max_pages=1,
                )

                if not success:
                    zones_failed.append({"name": zone_name, "type": zone_type, "error": str(result)[:100]})
                    logger.warning(f"[habitaclia/scrape-updates] {zone_type} '{zone_name}' failed: {result}")
                    return

                ads = result.get("ads", [])[:ads_limit]

                if not ads:
                    zones_scraped.append({
                        "name": zone_name,
                        "type": zone_type,
                        "ads": 0, "inserted": 0, "updated": 0, "enriched": 0,
                    })
                    return

                # Upsert esta zona inmediatamente
                upsert_ok, inserted, updated, error_msg, new_codes = await upsert_habitaclia_to_supabase(client, ads)

                total_inserted += inserted
                total_updated += updated

                # --- Enriquecer propiedades recien insertadas ---
                enriched_count = 0
                if new_codes and inserted > 0:
                    # Mapear propertycode -> (CodEmp, CodInm) desde los ads originales
                    code_map = {}
                    for ad in ads:
                        cod_prod = ad.get("CodProd", "")
                        cod_emp = ad.get("CodEmp")
                        cod_inm = ad.get("CodInm")
                        if cod_prod and cod_emp and cod_inm:
                            code_map[f"hab-{cod_prod}"] = (cod_emp, cod_inm)

                    supabase_headers = {
                        "apikey": SUPABASE_ANON_KEY,
                        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                        "Content-Type": "application/json",
                        "Prefer": "return=minimal",
                    }

                    for pcode in new_codes:
                        if pcode not in code_map:
                            continue
                        cod_emp, cod_inm = code_map[pcode]
                        try:
                            detail_ok, detail_data = await habitaclia.get_property_detail(
                                cod_emp=cod_emp, cod_inm=cod_inm
                            )
                            if detail_ok and isinstance(detail_data, dict):
                                enrichment = transform_habitaclia_detail_to_enrichment(detail_data)
                                update_url = f"{SUPABASE_URL}/async_properties?propertycode=eq.{pcode}"
                                enrich_resp = await client.patch(
                                    update_url, headers=supabase_headers, json=enrichment, timeout=30
                                )
                                if enrich_resp.status_code in [200, 204]:
                                    enriched_count += 1
                                else:
                                    logger.warning(f"[habitaclia/enrich] DB update failed for {pcode}: {enrich_resp.status_code}")
                            elif not detail_ok:
                                logger.debug(f"[habitaclia/enrich] Detail failed for {pcode}, will enrich later")
                        except Exception as e:
                            logger.warning(f"[habitaclia/enrich] Error enriching {pcode}: {e}")
                        # Delay para no saturar la API
                        await asyncio.sleep(0.3)

                    total_enriched += enriched_count
                    if enriched_count > 0:
                        logger.info(f"[habitaclia/scrape-updates] Enriched {enriched_count}/{inserted} new props in '{zone_name}'")

                zone_result = {
                    "name": zone_name,
                    "type": zone_type,
                    "ads": len(ads),
                    "inserted": inserted,
                    "updated": updated,
                    "enriched": enriched_count,
                }
                if error_msg:
                    zone_result["error"] = error_msg

                zones_scraped.append(zone_result)

                logger.info(
                    f"[habitaclia/scrape-updates] {zone_type} '{zone_name}': "
                    f"{len(ads)} ads -> {inserted} new, {updated} updated, {enriched_count} enriched"
                )

            except Exception as e:
                zones_failed.append({"name": zone_name, "type": zone_type, "error": str(e)[:100]})
                logger.error(f"[habitaclia/scrape-updates] {zone_type} '{zone_name}' exception: {e}")

        # â€”â€” 4a. Scrapear capitales â€”â€”
        for cap in capitals:
            await scrape_and_upsert_zone(cap, "capital", request.capitalLimit)
            await asyncio.sleep(1)

        # â€”â€” 4b. Scrapear comarcas whitelist (mismo lÃ­mite que capitales) â€”â€”
        for com in comarcas_whitelist:
            await scrape_and_upsert_zone(com, "comarca_whitelist", request.capitalLimit)
            await asyncio.sleep(1)

        # â€”â€” 4c. Scrapear comarcas regulares â€”â€”
        for com in comarcas:
            await scrape_and_upsert_zone(com, "comarca", request.comarcaLimit)
            await asyncio.sleep(1)

        # â€”â€” 5. Calcular resumen â€”â€”
        total_time = time.time() - start_time
        total_ads = sum(z.get("ads", 0) for z in zones_scraped)

        # Zona con mÃ¡s inserts (para alertas en n8n)
        max_inserted_zone = max(zones_scraped, key=lambda z: z.get("inserted", 0)) if zones_scraped else None
        max_inserted_per_zone = max_inserted_zone.get("inserted", 0) if max_inserted_zone else 0

        result = {
            "success": len(zones_failed) == 0,
            "locationCode": request.locationCode,
            "capitals_scraped": len(capitals),
            "comarcas_whitelist_scraped": len(comarcas_whitelist),
            "comarcas_scraped": len(comarcas),
            "total_ads_scraped": total_ads,
            # â€”â€” Totales (compatibilidad con n8n actual) â€”â€”
            "inserted": total_inserted,
            "updated": total_updated,
            "enriched": total_enriched,
            # â€”â€” Para alertas granulares en n8n â€”â€”
            "max_inserted_per_zone": max_inserted_per_zone,
            "max_inserted_zone_name": max_inserted_zone.get("name", "") if max_inserted_zone else "",
            "total_time_seconds": round(total_time, 2),
            # â€”â€” Desglose por zona â€”â€”
            "zones_scraped": zones_scraped,
        }

        if zones_failed:
            result["zones_failed"] = zones_failed

        logger.info(
            f"[habitaclia/scrape-updates] Completed: {total_inserted} inserted, "
            f"{total_updated} updated, {total_enriched} enriched across {len(zones_scraped)} zones in {total_time:.1f}s "
            f"(max insert in single zone: {max_inserted_per_zone} @ {result['max_inserted_zone_name']})"
        )

        return result

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HABITACLIA — Workers Check Active (endpoints de control)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/habitaclia/workers-status")
async def habitaclia_workers_status():
    """Estado unificado de todos los procesos de Habitaclia."""
    hab_elapsed = 0
    if hab_check_active_state["started_at"]:
        start = datetime.fromisoformat(hab_check_active_state["started_at"])
        hab_elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    hab_hours = hab_elapsed / 3600 if hab_elapsed > 0 else 1
    hab_speed = hab_check_active_state["totals"]["checked"] / hab_hours

    en_elapsed = 0
    if hab_enrich_state["started_at"]:
        start = datetime.fromisoformat(hab_enrich_state["started_at"])
        en_elapsed = (datetime.now(timezone.utc) - start).total_seconds()

    # Scrape updates elapsed
    su_elapsed = 0
    if hab_scrape_updates_state["started_at"]:
        start = datetime.fromisoformat(hab_scrape_updates_state["started_at"])
        su_elapsed = (datetime.now(timezone.utc) - start).total_seconds()

    # Pending by tier (misma lógica que Fotocasa workers-status)
    pending_by_tier = {}
    if db_pool:
        try:
            now_utc = datetime.now(timezone.utc)
            cutoff_4   = now_utc - timedelta(hours=4)
            cutoff_48  = now_utc - timedelta(hours=48)
            cutoff_120 = now_utc - timedelta(hours=120)
            rows = await db_pool.fetch("""
                SELECT tier, COUNT(*) as cnt FROM (
                    SELECT CASE
                        WHEN province IN ('madrid','barcelona','valencia')
                             AND municipality IN ('Madrid','Barcelona','Valencia')
                        THEN 'capitals'
                        WHEN province IN ('madrid','barcelona','valencia','sevilla','vizcaya',
                             'alicante','málaga','zaragoza','mallorca','gran canaria')
                        THEN 'main_provinces'
                        ELSE 'rest'
                    END as tier,
                    last_checked_at
                    FROM async_properties
                    WHERE source = 'habitaclia' AND adisactive = true
                      AND habitaclia_specific->>'CodEmp' IS NOT NULL
                      AND habitaclia_specific->>'CodInm' IS NOT NULL
                ) sub
                WHERE (tier = 'capitals'        AND (last_checked_at IS NULL OR last_checked_at < $1))
                   OR (tier = 'main_provinces'  AND (last_checked_at IS NULL OR last_checked_at < $2))
                   OR (tier = 'rest'            AND (last_checked_at IS NULL OR last_checked_at < $3))
                GROUP BY tier
            """, cutoff_4, cutoff_48, cutoff_120)
            for row in rows:
                pending_by_tier[row["tier"]] = row["cnt"]
            for t in ("capitals", "main_provinces", "rest"):
                pending_by_tier.setdefault(t, 0)
        except Exception as e:
            logger.warning(f"[hab-workers-status] Pending count failed: {e}")
            pending_by_tier = {"capitals": "error", "main_provinces": "error", "rest": "error"}

    return {
        "hab_check_active": {
            "running": hab_check_active_state["running"],
            "started_at": hab_check_active_state["started_at"],
            "elapsed_hours": round(hab_elapsed / 3600, 1),
            "speed_per_hour": round(hab_speed),
            "workers": hab_check_active_state["workers"],
            "totals": hab_check_active_state["totals"],
            "tiers": [{"name": t["name"], "cycle_hours": t["cycle_hours"]} for t in HAB_CHECK_ACTIVE_TIERS],
            "pending_by_tier": pending_by_tier,
        },
        "hab_enrich": {
            "running": hab_enrich_state["running"],
            "started_at": hab_enrich_state["started_at"],
            "elapsed_hours": round(en_elapsed / 3600, 1),
            "status": hab_enrich_state["status"],
            "totals": hab_enrich_state["totals"],
        },
        "hab_recheck": {
            "running": hab_recheck_state["running"],
            "mode": hab_recheck_state["mode"],
            "totals": hab_recheck_state["totals"],
            "last_batch_at": hab_recheck_state["last_batch_at"],
        },
        "hab_scrape_updates": {
            "running": hab_scrape_updates_state["running"],
            "started_at": hab_scrape_updates_state["started_at"],
            "elapsed_hours": round(su_elapsed / 3600, 1),
            "workers": hab_scrape_updates_state["workers"],
            "totals": hab_scrape_updates_state["totals"],
        },
    }


@app.post("/habitaclia/workers-stop")
async def habitaclia_workers_stop():
    """Detiene todos los workers de Habitaclia (check-active, enrich, scrape-updates, recheck)."""
    hab_check_active_state["running"] = False
    hab_enrich_state["running"] = False
    hab_scrape_updates_state["running"] = False
    hab_recheck_state["running"] = False
    return {"success": True, "message": "Stop signal sent to all Habitaclia workers (including recheck)"}


@app.post("/habitaclia/workers-restart")
async def habitaclia_workers_restart():
    """Reinicia todos los workers de Habitaclia (check-active, enrich, scrape-updates, recheck)."""
    hab_check_active_state["running"] = False
    hab_enrich_state["running"] = False
    hab_scrape_updates_state["running"] = False
    hab_recheck_state["running"] = False
    await asyncio.sleep(5)
    await _start_hab_check_active()
    await _start_hab_enrich()
    await _start_hab_scrape_updates()
    await _start_hab_recheck()
    return {"success": True, "message": "All Habitaclia workers restarted (including recheck)"}


@app.post("/habitaclia/recheck")
@app.get("/habitaclia/recheck")
async def habitaclia_recheck():
    """
    El recheck de Habitaclia ahora es un worker continuo.
    Este endpoint devuelve el estado actual.
    """
    return {
        "running": hab_recheck_state["running"],
        "mode": hab_recheck_state["mode"],
        "totals": hab_recheck_state["totals"],
        "last_batch_at": hab_recheck_state["last_batch_at"],
        "message": "HAB recheck runs continuously as a background worker",
    }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  OCCUPATION STATUS — AI Agent Worker (Claude Haiku)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_OCCUPATION_AI_PROMPT = """Clasifica esta descripción de inmueble en UNA de estas categorías:
- occupied: ocupada ilegalmente, okupada, sin posesión, REO sin posesión, no se puede visitar
- rented: alquilada con inquilinos, piso alquilado
- bare_ownership: nuda propiedad, usufructo vitalicio, renta vitalicia
- auction: subasta judicial, subasta, puja
- mortgage_assignment: cesión de crédito hipotecario, NPL, crédito hipotecario
- normal: ninguna de las anteriores, vivienda libre

Responde SOLO con la palabra de la categoría. Nada más.

Descripción:
{description}"""

_occupation_ai_state = {
    "running": False,
    "totals": {"processed": 0, "classified": 0, "normal": 0, "errors": 0},
    "last_batch_at": None,
}


async def _occupation_ai_worker():
    """Worker continuo que clasifica occupation_status usando Claude Haiku."""
    if not ANTHROPIC_API_KEY:
        logger.warning("[occupation-ai] ANTHROPIC_API_KEY not set, worker disabled")
        return

    _occupation_ai_state["running"] = True
    logger.info("[occupation-ai] Worker started")

    while True:
        try:
            if not db_pool:
                await asyncio.sleep(30)
                continue

            # Leer batch de propiedades sin clasificar que tengan descripción
            rows = await db_pool.fetch("""
                SELECT propertycode, description
                FROM async_properties
                WHERE occupation_status IS NULL
                  AND description IS NOT NULL
                  AND length(description) > 20
                  AND adisactive = true
                ORDER BY updated_at DESC
                LIMIT 50
            """)

            if not rows:
                logger.info("[occupation-ai] No pending properties, sleeping 10min")
                await asyncio.sleep(600)
                continue

            batch_classified = 0
            batch_normal = 0
            batch_errors = 0

            async with httpx.AsyncClient(timeout=30) as client:
                for row in rows:
                    pcode = row["propertycode"]
                    desc = row["description"][:1500]  # Limitar tokens

                    try:
                        resp = await client.post(
                            "https://api.anthropic.com/v1/messages",
                            headers={
                                "x-api-key": ANTHROPIC_API_KEY,
                                "anthropic-version": "2023-06-01",
                                "content-type": "application/json",
                            },
                            json={
                                "model": "claude-haiku-4-5-20251001",
                                "max_tokens": 20,
                                "messages": [{"role": "user", "content": _OCCUPATION_AI_PROMPT.format(description=desc)}],
                            },
                        )

                        if resp.status_code == 200:
                            data = resp.json()
                            answer = data.get("content", [{}])[0].get("text", "").strip().lower()

                            valid = {"occupied", "rented", "bare_ownership", "auction", "mortgage_assignment", "normal"}
                            if answer not in valid:
                                answer = "normal"

                            await db_pool.execute(
                                "UPDATE async_properties SET occupation_status = $1 WHERE propertycode = $2",
                                answer, pcode,
                            )

                            if answer == "normal":
                                batch_normal += 1
                            else:
                                batch_classified += 1
                        else:
                            batch_errors += 1
                            if resp.status_code == 429:
                                logger.warning("[occupation-ai] Rate limited, sleeping 60s")
                                await asyncio.sleep(60)
                            else:
                                logger.error(f"[occupation-ai] API error {resp.status_code}: {resp.text[:200]}")

                    except Exception as e:
                        batch_errors += 1
                        logger.error(f"[occupation-ai] Error classifying {pcode}: {e}")

                    await asyncio.sleep(0.2)  # ~5 req/s max

            _occupation_ai_state["totals"]["processed"] += len(rows)
            _occupation_ai_state["totals"]["classified"] += batch_classified
            _occupation_ai_state["totals"]["normal"] += batch_normal
            _occupation_ai_state["totals"]["errors"] += batch_errors
            _occupation_ai_state["last_batch_at"] = datetime.now(timezone.utc).isoformat()

            logger.info(
                f"[occupation-ai] Batch done: {batch_classified} classified, "
                f"{batch_normal} normal, {batch_errors} errors / {len(rows)} total"
            )

            await asyncio.sleep(5)  # Pausa entre batches

        except Exception as e:
            logger.error(f"[occupation-ai] Worker crash: {e}", exc_info=True)
            await asyncio.sleep(60)

    _occupation_ai_state["running"] = False


async def _start_occupation_ai():
    """Lanza el worker de clasificación AI."""
    await asyncio.sleep(120)  # Esperar 2min post-startup para no competir con otros workers
    asyncio.create_task(_occupation_ai_worker())


@app.get("/occupation-ai/status")
async def occupation_ai_status():
    """Estado del worker de clasificación AI."""
    pending = 0
    if db_pool:
        try:
            row = await db_pool.fetchrow(
                "SELECT count(*) as cnt FROM async_properties WHERE occupation_status IS NULL AND description IS NOT NULL AND adisactive = true"
            )
            pending = row["cnt"] if row else 0
        except Exception:
            pass
    return {
        **_occupation_ai_state,
        "pending": pending,
    }