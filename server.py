"""
Backend para búsqueda de propiedades
Flujo Idealista-first + datos del Catastro
"""

import os
import json
import httpx
import math
import re
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from dossier import generate_dossier_pdf, generate_dossier_filename
from pydantic import BaseModel
from typing import Dict, Optional
import asyncio

COOKIE_SERVER_URL = os.getenv("COOKIE_SERVER_URL", "http://37.27.8.255:5001")

app = FastAPI(title="Property Search API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],  # ← AÑADIR ESTA LÍNEA
)

class SearchAddressRequest(BaseModel):
    query: str

class GetBuildingRequest(BaseModel):
    city_slug: str
    street_slug: str
    number: str

class GetPropertyRequest(BaseModel):
    city_slug: str
    street_slug: str
    number: str
    floor: str
    door: str

class CadastreRequest(BaseModel):
    cadastre_ref: str

class ExtrasInput(BaseModel):
    elevator: bool = False
    garage: bool = False
    swimming_pool: bool = False
    garden: bool = False
    terrace: bool = False
    balcony: bool = False
    air_conditioning: bool = False
    wardrobes: bool = False
    storage: bool = False
    exterior: bool = False
    luxury: bool = False
    doorman: bool = False

class SpecialSituationInput(BaseModel):
    nuda_propiedad: bool = False
    rented: bool = False
    illegally_occupied: bool = False

class SearchComparablesRequest(BaseModel):
    coordinates: dict
    total_area: float = 0
    rooms: int = 0
    bathrooms: int = 0
    state: str = "good"             # canonical: "good" | "renew" | "newDevelopment" (acepta legacy "buen_estado"/"a_reformar"/"obra_nueva")
    property_type: str = "flat"     # canonical: "flat"/"penthouse"/etc. (acepta legacy "piso"/"atico"/etc.)
    floor: Optional[str] = None     # NEW (para floor_tier_match en scoring full)
    extras: ExtrasInput = ExtrasInput()
    special_situation: SpecialSituationInput = SpecialSituationInput()
    cadastre_ref: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    building: Optional[dict] = None

class ComparableInput(BaseModel):
    id: str = ""
    title: str = ""
    address: str = ""
    price: float = 0
    priceByArea: float = 0
    size: float = 0
    rooms: int = 0
    bathrooms: int = 0
    distance: Optional[float] = None
    source: str = "idealista"
    url: str = ""
    thumbnail: str = ""
    selected: bool = False
    in_range: bool = False

class PropertyInput(BaseModel):
    address: str = ""
    total_area: float = 0
    rooms: int = 0
    bathrooms: int = 0

class ValuationInput(BaseModel):
    mean: float = 0
    min: float = 0
    max: float = 0

class GenerateDossierRequest(BaseModel):
    property: PropertyInput
    coordinates: dict
    valuation: ValuationInput
    comparables: list[ComparableInput]

class IdealistaService:
    def __init__(self):
        self.hetzner_url = COOKIE_SERVER_URL
    
    def search_address(self, query: str) -> Dict:
        try:
            response = httpx.get(f"{self.hetzner_url}/api/idealista/search", params={"query": query}, timeout=30)
            data = response.json()
            if not data.get("success"):
                return data
            results = []
            for item in data.get("data", []):
                town = item.get("town", {})
                results.append({
                    "id": item.get("id"),
                    "display": f"{item.get('type', '')} {item.get('name', '')}, {item.get('number', '')} - {town.get('name', '')}",
                    "street_name": item.get("name"),
                    "number": item.get("number"),
                    "city_slug": town.get("slug"),
                    "street_slug": item.get("slug"),
                    "city_name": town.get("name"),
                })
            return {"success": True, "results": results}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_building(self, city_slug: str, street_slug: str, number: str) -> Dict:
        try:
            response = httpx.get(f"{self.hetzner_url}/api/idealista/location/{city_slug}/{street_slug}/{number}", timeout=30)
            data = response.json()

            print("🔍 RAW DATA FROM HETZNER:")
            import json
            print(json.dumps(data, indent=2, default=str)[:3000])  # Primeros 3000 chars
            
            if not data.get("success"):
                return data
            location_data = data.get("data", {})
            parcels = location_data.get("parcels", []) or location_data.get("cadastre", {}).get("items", [])
            if not parcels:
                return {"success": False, "error": "No building data found"}
            parcel = parcels[0]
            attrs = parcel.get("attributes", {})
            units = []
            for prop in parcel.get("properties", []):
                if not prop.get("is_residential"):
                    continue
                structures = prop.get("structures", [])
                units.append({
                    "cadastre_ref": prop.get("reference"),
                    "name": prop.get("name"),
                    "floor": structures[0].get("floor") if structures else None,
                    "door": structures[0].get("door") if structures else None,
                    "area": prop.get("area"),
                })
            units.sort(key=lambda x: (x.get("floor") or "", x.get("door") or ""))
            # Extraer coordenadas del centroid del catastro
            centroid = parcel.get("centroid", {})
            coordinates = None
            if centroid.get("latitude") and centroid.get("longitude"):
                coordinates = {
                    "lat": centroid.get("latitude"),
                    "lng": centroid.get("longitude")
                }
            
            return {
                "success": True,
                "address": location_data.get("address"),
                "postal_code": location_data.get("postal_code"),
                "coordinates": coordinates,  # ← NUEVO
                "building": {
                    "year": attrs.get("constructed_year"),
                    "total_floors": attrs.get("total_floors"),
                    "has_lift": attrs.get("has_lift"),
                    "has_parking": attrs.get("has_parking"),
                    "has_pool": attrs.get("has_swimming_pool"),
                    "has_garden": attrs.get("has_garden"),
                    "has_doorman": attrs.get("has_doorman"),
                    "has_storage": attrs.get("has_storage"),
                },
                "units": units,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_property(self, city_slug: str, street_slug: str, number: str, floor: str, door: str) -> Dict:
        try:
            response = httpx.get(f"{self.hetzner_url}/api/idealista/location/{city_slug}/{street_slug}/{number}", timeout=30)
            data = response.json()
            if not data.get("success"):
                return data
            location_data = data.get("data", {})
            parcels = location_data.get("parcels", []) or location_data.get("cadastre", {}).get("items", [])
            if not parcels:
                return {"success": False, "error": "No building data found"}
            parcel = parcels[0]
            attrs = parcel.get("attributes", {})
            # Extraer coordenadas
            centroid = parcel.get("centroid", {})
            coordinates = None
            if centroid.get("latitude") and centroid.get("longitude"):
                coordinates = {
                    "lat": centroid.get("latitude"),
                    "lng": centroid.get("longitude")
                }
            floor_norm = self._normalize_floor(floor)
            door_norm = door.upper().strip() if door else ""
            for prop in parcel.get("properties", []):
                structures = prop.get("structures", [])
                if not structures:
                    continue
                prop_floor = self._normalize_floor(structures[0].get("floor", ""))
                prop_door = (structures[0].get("door") or "").upper().strip()
                if prop_floor == floor_norm and (not door_norm or prop_door == door_norm):
                    living_area = sum(s["area"] for s in structures if s.get("typology") == "V")
                    common_area = sum(s["area"] for s in structures if s.get("typology") == "COMMON")
                    return {
                        "success": True,
                        "cadastre_ref": prop.get("reference"),
                        "name": prop.get("name"),
                        "address": location_data.get("address"),
                        "postal_code": location_data.get("postal_code"),
                        "coordinates": coordinates,
                        "rooms": prop.get("room_number"),
                        "bathrooms": prop.get("bathroom_number"),
                        "total_area": prop.get("area"),
                        "living_area": living_area,
                        "common_area": common_area,
                        "floor": structures[0].get("floor"),
                        "door": structures[0].get("door"),
                        "energy_certificate": prop.get("energy_certificate"),
                        "building": {
                            "year": attrs.get("constructed_year"),
                            "total_floors": attrs.get("total_floors"),
                            "has_lift": attrs.get("has_lift"),
                            "has_parking": attrs.get("has_parking"),
                            "has_pool": attrs.get("has_swimming_pool"),
                            "has_garden": attrs.get("has_garden"),
                            "has_doorman": attrs.get("has_doorman"),
                            "has_storage": attrs.get("has_storage"),
                        },
                    }
            return {"success": False, "error": f"Property not found at {floor} {door}"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _normalize_floor(self, floor: str) -> str:
        if not floor:
            return ""
        floor = str(floor).upper().strip().replace("º", "").replace("ª", "")
        mappings = {"BAJO": "00", "BJ": "00", "ATICO": "AT"}
        if floor in mappings:
            return mappings[floor]
        try:
            return str(int(floor)).zfill(2)
        except:
            return floor

    def search_by_cadastre(self, cadastre_ref: str) -> Dict:
        """
        Busca por referencia catastral (parcela ~14 chars o unidad ~20 chars).

        Devuelve uno de estos formatos:
        - type=unit:       vivienda concreta resuelta (formato similar a get_property)
        - type=building:   edificio resuelto (formato similar a get_building)
        - type=candidates: varios candidatos, el frontend debe elegir
        - {success: false, error}: no encontrada o error
        """
        try:
            ref = (cadastre_ref or "").strip().upper()
            if not ref:
                return {"success": False, "error": "Referencia catastral vacía"}

            # 1) Reverse lookup: ref → slugs
            response = httpx.get(
                f"{self.hetzner_url}/api/idealista/cadastre/{ref}",
                timeout=30,
            )
            data = response.json()
            if not data.get("success"):
                return data

            results = data.get("data", []) or []
            if not results:
                return {"success": False, "error": "Referencia catastral no encontrada"}

            # 2) ¿Hay match exacto con `reference` llena? → ref de unidad
            exact_match = next(
                (r for r in results
                 if (r.get("reference") or "").strip().upper() == ref),
                None,
            )

            if exact_match:
                street = exact_match.get("street", {}) or {}
                town = street.get("town", {}) or {}
                city_slug = town.get("slug")
                street_slug = street.get("slug")
                number = street.get("number")
                if not (city_slug and street_slug and number):
                    return {"success": False, "error": "Datos de calle incompletos en respuesta del servidor"}
                return self._fetch_unit_or_building(city_slug, street_slug, number, ref)

            # 3) Sin match exacto: filtrar resultados con slugs válidos
            valid = [
                r for r in results
                if ((r.get("street") or {}).get("slug")
                    and (r.get("street") or {}).get("number")
                    and ((r.get("street") or {}).get("town") or {}).get("slug"))
            ]
            if not valid:
                return {"success": False, "error": "Referencia catastral no encontrada"}

            if len(valid) == 1:
                # Una sola parcela: devolver el building (y por si acaso buscamos la ref dentro)
                street = valid[0].get("street", {}) or {}
                town = street.get("town", {}) or {}
                return self._fetch_unit_or_building(
                    town.get("slug"),
                    street.get("slug"),
                    street.get("number"),
                    ref,
                )

            # Varios candidatos: el frontend debe elegir
            candidates = []
            for r in valid:
                street = r.get("street", {}) or {}
                town = street.get("town", {}) or {}
                candidates.append({
                    "display": r.get("name") or f"{street.get('type', '')} {street.get('name', '')}, {street.get('number', '')}".strip(),
                    "city_slug": town.get("slug"),
                    "street_slug": street.get("slug"),
                    "number": street.get("number"),
                    "city_name": town.get("name"),
                })
            return {
                "success": True,
                "type": "candidates",
                "candidates": candidates,
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _fetch_unit_or_building(self, city_slug: str, street_slug: str, number: str, target_ref: str) -> Dict:
        """
        Llama a /api/idealista/location/ y decide:
        - Si target_ref aparece en properties[] → devuelve type=unit completa
        - Si no → devuelve type=building con la lista de units
        """
        try:
            response = httpx.get(
                f"{self.hetzner_url}/api/idealista/location/{city_slug}/{street_slug}/{number}",
                timeout=30,
            )
            data = response.json()
            if not data.get("success"):
                return data

            location_data = data.get("data", {}) or {}
            parcels = location_data.get("parcels", []) or location_data.get("cadastre", {}).get("items", [])
            if not parcels:
                return {"success": False, "error": "No se encontraron datos del edificio"}

            parcel = parcels[0]
            attrs = parcel.get("attributes", {}) or {}

            centroid = parcel.get("centroid", {}) or {}
            coordinates = None
            if centroid.get("latitude") and centroid.get("longitude"):
                coordinates = {"lat": centroid.get("latitude"), "lng": centroid.get("longitude")}

            building_info = {
                "year": attrs.get("constructed_year"),
                "total_floors": attrs.get("total_floors"),
                "has_lift": attrs.get("has_lift"),
                "has_parking": attrs.get("has_parking"),
                "has_pool": attrs.get("has_swimming_pool"),
                "has_garden": attrs.get("has_garden"),
                "has_doorman": attrs.get("has_doorman"),
                "has_storage": attrs.get("has_storage"),
            }

            target = (target_ref or "").strip().upper()
            properties = parcel.get("properties", []) or []

            matched = next(
                (p for p in properties
                 if (p.get("reference") or "").strip().upper() == target),
                None,
            )

            if matched:
                # type = unit
                structures = matched.get("structures", []) or []
                living_area = sum((s.get("area") or 0) for s in structures if s.get("typology") == "V")
                common_area = sum((s.get("area") or 0) for s in structures if s.get("typology") == "COMMON")
                return {
                    "success": True,
                    "type": "unit",
                    "cadastre_ref": matched.get("reference"),
                    "name": matched.get("name"),
                    "address": location_data.get("address"),
                    "postal_code": location_data.get("postal_code"),
                    "coordinates": coordinates,
                    "rooms": matched.get("room_number"),
                    "bathrooms": matched.get("bathroom_number"),
                    "total_area": matched.get("area"),
                    "living_area": living_area,
                    "common_area": common_area,
                    "floor": structures[0].get("floor") if structures else None,
                    "door": structures[0].get("door") if structures else None,
                    "energy_certificate": matched.get("energy_certificate"),
                    "is_residential": matched.get("is_residential"),
                    "typology": matched.get("typology"),
                    "building": building_info,
                    "city_slug": city_slug,
                    "street_slug": street_slug,
                    "number": number,
                }

            # type = building
            units = []
            for prop in properties:
                if not prop.get("is_residential"):
                    continue
                structures = prop.get("structures", []) or []
                units.append({
                    "cadastre_ref": prop.get("reference"),
                    "name": prop.get("name"),
                    "floor": structures[0].get("floor") if structures else None,
                    "door": structures[0].get("door") if structures else None,
                    "area": prop.get("area"),
                })
            units.sort(key=lambda x: (x.get("floor") or "", x.get("door") or ""))

            return {
                "success": True,
                "type": "building",
                "address": location_data.get("address"),
                "postal_code": location_data.get("postal_code"),
                "coordinates": coordinates,
                "building": building_info,
                "units": units,
                "city_slug": city_slug,
                "street_slug": street_slug,
                "number": number,
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

idealista_service = IdealistaService()

@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.0.0"}

@app.post("/api/search-address")
async def search_address(request: SearchAddressRequest):
    return idealista_service.search_address(request.query)

@app.post("/api/get-building")
async def get_building(request: GetBuildingRequest):
    return idealista_service.get_building(request.city_slug, request.street_slug, request.number)

@app.post("/api/get-property")
async def get_property(request: GetPropertyRequest):
    return idealista_service.get_property(request.city_slug, request.street_slug, request.number, request.floor, request.door)

@app.post("/api/search-by-cadastre")
async def search_by_cadastre(request: CadastreRequest):
    return idealista_service.search_by_cadastre(request.cadastre_ref)

@app.post("/api/search-comparables")
async def search_comparables(request: SearchComparablesRequest):
    """
    Busca comparables en 3 niveles progresivos.
    Llama a los scrapers de Idealista, Fotocasa y Habitaclia.
    """
    RENDER_URL = os.environ.get("RENDER_SERVER_URL", "https://idealista-async-scraper.onrender.com")
    SEARCH_RADII = [1250, 2000, 3000]
    TOLERANCES = {
        1: {"size_pct": 0.15, "rooms_delta": 0, "bathrooms_delta": 0},
        2: {"size_pct": 0.20, "rooms_delta": 1, "bathrooms_delta": 1},
        3: {"size_pct": 0.30, "rooms_delta": 1, "bathrooms_delta": 1},
    }
    MIN_PER_PLATFORM = 5
    MIN_TOTAL = 20
    
    def create_circle_polygon(lat, lng, radius_m, points=32):
        R = 6371000
        result = []
        for i in range(points):
            angle = (2 * math.pi * i) / points
            dx = radius_m * math.cos(angle)
            dy = radius_m * math.sin(angle)
            d_lat = dy / R * (180 / math.pi)
            d_lng = dx / (R * math.cos(lat * math.pi / 180)) * (180 / math.pi)
            result.append([lng + d_lng, lat + d_lat])  # ← CAMBIADO: [lng, lat] en lugar de [lat, lng]
        result.append(result[0])
        return result
    
    def map_state(state):
        # Acepta vocabulario frontend (canonical) Y legacy español, normaliza a Idealista
        s = (state or "").strip()
        return {
            # Frontend canonical
            "newDevelopment": "newdevelopment",
            "good": "good",
            "renew": "renew",
            # Legacy español
            "obra_nueva": "newdevelopment",
            "buen_estado": "good",
            "a_reformar": "renew",
        }.get(s, "good")
    
    def normalize_state_canonical(state):
        # Devuelve el state en formato canonical (frontend) para usar en _norm.status match
        s = (state or "").strip()
        if s in ("newDevelopment", "obra_nueva"):
            return "newdevelopment"
        if s in ("good", "buen_estado"):
            return "good"
        if s in ("renew", "a_reformar"):
            return "renew"
        return "good"
    
    def normalize_propertytype(pt):
        # Vocabulario común. Acepta frontend (flat/penthouse/etc.) y legacy español (piso/atico/etc.)
        s = (pt or "").lower().strip()
        return {
            # Frontend canonical (passthrough)
            "flat": "flat", "penthouse": "penthouse", "duplex": "duplex",
            "studio": "studio", "chalet": "chalet", "house": "house",
            "countryhouse": "countryhouse",
            # Legacy español
            "piso": "flat", "apartamento": "flat",
            "atico": "penthouse", "ático": "penthouse",
            "loft": "loft",
            "chalet_independiente": "chalet", "chalet_pareado": "chalet", "chalet_adosado": "chalet",
            "casa_rustica": "countryhouse", "villa": "villa",
            # Idealista detail / Fotocasa subtype values
            "homes": "flat",  # default genérico
            "single-family-house": "house", "house": "house",
        }.get(s, s)  # fallback: passthrough lowercased
    
    def floor_tier(floor_str):
        # Devuelve "low" | "mid" | "high" | None. Robusto a varios formatos.
        if floor_str is None:
            return None
        f = str(floor_str).upper().strip().replace("º", "").replace("ª", "")
        if not f:
            return None
        # Bajos / sótanos / semis / planta baja / entresuelo / principal
        if f in ("BAJO", "BJ", "B", "SS", "ST", "SOTANO", "SÓTANO", "PB", "PR", "PRINCIPAL", "ENTLO", "ENTRESUELO", "EN"):
            return "low"
        # Áticos = high
        if f in ("AT", "ATICO", "ÁTICO", "PENTHOUSE"):
            return "high"
        # Fotocasa floorType strings
        ft_map = {
            "GROUND": "low", "BASEMENT": "low", "SUBBASEMENT": "low",
            "FIRST": "mid", "SECOND": "mid", "THIRD": "mid",
            "FOURTH": "high", "FIFTH": "high", "SIXTH": "high", "SEVENTH": "high",
            "EIGHTH": "high", "NINTH": "high", "TENTH": "high", "PENTHOUSE": "high",
        }
        for key, tier in ft_map.items():
            if key in f:
                return tier
        # Numérico (incluye "3rd floor", "1", "01", etc.)
        m = re.search(r"\b(\d+)\b", f)
        if m:
            try:
                n = int(m.group(1))
                if n == 0: return "low"
                if n <= 3: return "mid"
                return "high"
            except (ValueError, TypeError):
                pass
        return None
    
    def parse_haslift_from_description(features_arr):
        # HAB DescriptionFeatures parser. Devuelve True/False/None.
        if not features_arr or not isinstance(features_arr, list):
            return None
        joined = " ".join(str(f).lower() for f in features_arr)
        if "sin ascensor" in joined:
            return False
        if "ascensor" in joined:
            return True
        return None
    
    def parse_exterior_from_description(features_arr):
        # HAB DescriptionFeatures parser. Devuelve True/False/None.
        if not features_arr or not isinstance(features_arr, list):
            return None
        joined = " ".join(str(f).lower() for f in features_arr)
        if "exterior" in joined:
            return True
        if "interior" in joined:
            return False
        return None
    
    def normalize_comparable(comp):
        # Normaliza campos de cualquier source a un dict `_norm` canónico.
        # Idempotente: se puede llamar dos veces (post-enrichment HAB) sin efectos secundarios.
        src = comp.get("_source", "idealista")
        n = comp.get("_norm") or {}
        
        if src == "idealista":
            n["price"]        = comp.get("price")
            n["size"]         = comp.get("size")
            n["rooms"]        = comp.get("rooms")
            n["bathrooms"]    = comp.get("bathrooms")
            n["lat"]          = comp.get("latitude")
            n["lng"]          = comp.get("longitude")
            n["propertytype"] = normalize_propertytype(comp.get("propertyType"))
            n["newdev"]       = bool(comp.get("newDevelopment"))
            n["status"]       = comp.get("status")  # "good" | "renew" | "newdevelopment"
            n["exterior"]     = comp.get("exterior")
            n["haslift"]      = comp.get("hasLift")
            n["hasplan"]      = bool(comp.get("hasPlan"))
            n["numphotos"]    = comp.get("numPhotos") or 0
            n["floor_tier"]   = floor_tier(comp.get("floor"))
        
        elif src == "fotocasa":
            n["price"]        = comp.get("price")
            n["size"]         = comp.get("surface")
            n["rooms"]        = comp.get("rooms")
            n["bathrooms"]    = comp.get("bathrooms")
            n["lat"]          = comp.get("latitude")
            n["lng"]          = comp.get("longitude")
            n["propertytype"] = normalize_propertytype(comp.get("propertySubtype"))
            isdev = comp.get("isDevelopment")
            n["newdev"]       = isdev is True or isdev == "true"
            n["status"]       = None  # solo en detail FC; no consultamos
            n["exterior"]     = any(f.get("feature") == "IS_EXTERIOR" for f in (comp.get("dynamicFeatures") or []))
            extras_str        = (comp.get("extraList") or "")
            extras_set        = set(extras_str.split())
            n["haslift"]      = "13" in extras_set
            n["hasplan"]      = bool(comp.get("hasDescriptionPlan"))
            n["numphotos"]    = len(comp.get("mediaList") or [])
            floor_obj         = comp.get("propertyFloor") or {}
            n["floor_tier"]   = floor_tier(floor_obj.get("floorType") or floor_obj.get("description"))
        
        elif src == "habitaclia":
            n["price"]        = comp.get("PvpInm")
            n["size"]         = comp.get("M2Inm")
            n["rooms"]        = comp.get("NumHab")
            n["bathrooms"]    = comp.get("NumBany")
            # Coordenadas: prioridad detail (Map.VGPSLat/VGPSLon) > scrape (Point.Lat/Lon)
            map_obj           = comp.get("Map") or {}
            point_obj         = comp.get("Point") or {}
            n["lat"]          = map_obj.get("VGPSLat") or point_obj.get("Lat")
            n["lng"]          = map_obj.get("VGPSLon") or point_obj.get("Lon")
            n["propertytype"] = normalize_propertytype(comp.get("DescSubtipoBuscador"))
            n["newdev"]       = bool(comp.get("ObraNueva"))
            n["status"]       = None  # no en scrape ni detail
            # exterior y haslift solo después de enrichment (DescriptionFeatures)
            desc_features     = comp.get("DescriptionFeatures")
            if desc_features is not None:
                n["exterior"] = parse_exterior_from_description(desc_features)
                n["haslift"]  = parse_haslift_from_description(desc_features)
            else:
                n["exterior"] = n.get("exterior")  # preservar si ya estaba
                n["haslift"]  = n.get("haslift")
            n["hasplan"]      = None  # HAB no tiene equivalente
            # numphotos: scrape no aporta info fiable (NumImg1=0 basura). Detail: ImageCount o len(Images).
            n["numphotos"]    = comp.get("ImageCount") or len(comp.get("Images") or [])
            n["floor_tier"]   = None  # HAB no expone floor en scrape ni detail
        
        comp["_norm"] = n
        return n
    
    def calc_distance(lat1, lng1, lat2, lng2):
        R = 6371000
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = math.radians(lat2-lat1), math.radians(lng2-lng1)
        a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    all_comparables = []
    comparables_by_source = {"idealista": [], "fotocasa": [], "habitaclia": []}
    search_summary = []
    
    lat = request.coordinates.get("lat")
    lng = request.coordinates.get("lng")
    
    if not lat or not lng:
        return {"success": False, "error": "Coordenadas inválidas"}
    
    if not request.total_area or request.total_area <= 0:
        return {"success": False, "error": "Superficie inválida"}
    
    async with httpx.AsyncClient(timeout=60) as client:
        for call_level in [1, 2, 3]:
            radius = SEARCH_RADII[call_level - 1]
            tol = TOLERANCES[call_level]
            
            polygon = create_circle_polygon(lat, lng, radius)
            
            min_size = int(request.total_area * (1 - tol["size_pct"]))
            max_size = int(request.total_area * (1 + tol["size_pct"]))
            
            # Mapear habitaciones (Idealista: 1,2,3,4 donde 4 = "4 o más")
            def map_rooms(r):
                if r <= 0: return []
                if r >= 4: return ["4"]  # 4+ habitaciones
                return [str(r)]
            
            # Mapear baños (Idealista: 1,2,3 donde 3 = "3 o más")
            def map_baths(b):
                if b <= 0: return []
                if b >= 3: return ["3"]  # 3+ baños
                return [str(b)]
            
            if call_level == 1:
                bedrooms = map_rooms(request.rooms)
                bathrooms_list = map_baths(request.bathrooms)
            else:
                # Con tolerancia, incluir rangos
                room_min = max(1, request.rooms - tol["rooms_delta"])
                room_max = min(4, request.rooms + tol["rooms_delta"])  # Max 4 en Idealista
                bedrooms = [str(r) for r in range(room_min, room_max + 1)]
                if request.rooms >= 4:
                    bedrooms = ["4"]  # Si busca 4+, solo "4"
                
                bath_min = max(1, request.bathrooms - tol["bathrooms_delta"])
                bath_max = min(3, request.bathrooms + tol["bathrooms_delta"])  # Max 3 en Idealista
                bathrooms_list = [str(b) for b in range(bath_min, bath_max + 1)]
                if request.bathrooms >= 3:
                    bathrooms_list = ["3"]  # Si busca 3+, solo "3"
            
            # Parámetros para Idealista
            idealista_params = {
                "coordinates": json.dumps(polygon),
                "operation": "sale",
                "propertyType": "homes",
                "maxItems": 50,
                "minSize": min_size,
                "maxSize": max_size,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms_list,
                "preservations": map_state(request.state),
                "order": "publicationDate",
            }
            
            # Añadir filtros de tipo. Acepta vocabulario frontend canonical Y legacy español.
            type_map = {
                # Legacy español
                "atico": "penthouse", "duplex": "duplex", "apartamento": "apartamentoType",
                "loft": "loftType", "chalet_independiente": "independentHouse",
                "chalet_pareado": "semidetachedHouse", "chalet_adosado": "terracedHouse",
                "casa_rustica": "countryHouse", "villa": "villaType", "piso": "flat",
                # Frontend canonical
                "flat": "flat", "penthouse": "penthouse",
                "studio": "studioType", "countryHouse": "countryHouse",
                # "chalet" y "house" sin filtro específico (ambiguos: independiente/pareado/adosado)
            }
            if request.property_type in type_map:
                idealista_params[type_map[request.property_type]] = True
            
            # Añadir extras en nivel 1 y 2
            if call_level <= 2:
                if request.extras.elevator: idealista_params["elevator"] = True
                if request.extras.garage: idealista_params["garage"] = True
                if request.extras.swimming_pool: idealista_params["swimmingPool"] = True
                if request.extras.terrace: idealista_params["terrace"] = True
                if request.extras.garden: idealista_params["garden"] = True
                if request.extras.air_conditioning: idealista_params["airConditioning"] = True
                if request.extras.storage: idealista_params["storeRoom"] = True
            
            level_results = {"level": call_level, "radius": radius, "idealista": 0, "fotocasa": 0, "habitaclia": 0}
            
            # Llamar a Idealista
            try:
                print(f"🔍 Llamando Idealista nivel {call_level}, radio {radius}m...")
                print(f"📤 Params: {json.dumps(idealista_params, indent=2)}")
                resp = await client.post(f"{RENDER_URL}/scrape", json=idealista_params)
                if resp.status_code == 200:
                    data = resp.json()
                    properties = data.get("elementList", [])
                    if properties:
                        for prop in properties:
                            prop["_source"] = "idealista"
                            prop["_call_level"] = call_level
                            all_comparables.append(prop)
                            comparables_by_source["idealista"].append(prop)
                        level_results["idealista"] = len(properties)
                        print(f"✅ Idealista nivel {call_level}: {len(properties)} resultados")
                    else:
                        print(f"⚠️ Idealista nivel {call_level}: sin resultados")
                else:
                    print(f"❌ Idealista nivel {call_level}: HTTP {resp.status_code}")
            except Exception as e:
                print(f"❌ Error Idealista nivel {call_level}: {e}")
            
            # Llamar a Fotocasa
            try:
                print(f"🔍 Llamando Fotocasa nivel {call_level}, radio {radius}m...")
                # Fotocasa necesita: latitude, longitude (centro) + polygon en formato "lng,lat;lng,lat;..."
                fotocasa_polygon = ";".join([f"{p[0]},{p[1]}" for p in polygon])
                fotocasa_params = {
                    "latitude": lat,
                    "longitude": lng,
                    "polygon": fotocasa_polygon,
                    "transactionType": "SALE",
                    "propertyType": "HOME",
                    "pageSize": 36,
                    "surfaceFrom": min_size,
                    "surfaceTo": max_size,
                }
                resp = await client.post(f"{RENDER_URL}/fotocasa/scrape", json=fotocasa_params)
                if resp.status_code == 200:
                    data = resp.json()
                    # Fotocasa devuelve placeholders con estructura anidada
                    properties = []
                    if "placeholders" in data:
                        for placeholder in data["placeholders"]:
                            if placeholder.get("type") == "PROPERTY" and placeholder.get("property"):
                                prop = placeholder["property"]
                                properties.append(prop)
                    else:
                        properties = data.get("elementList", data.get("properties", []))
                    if properties:
                        for prop in properties:
                            prop["_source"] = "fotocasa"
                            prop["_call_level"] = call_level
                            all_comparables.append(prop)
                            comparables_by_source["fotocasa"].append(prop)
                        level_results["fotocasa"] = len(properties)
                        print(f"✅ Fotocasa nivel {call_level}: {len(properties)} resultados")
                    else:
                        print(f"⚠️ Fotocasa nivel {call_level}: sin resultados")
            except Exception as e:
                print(f"❌ Error Fotocasa nivel {call_level}: {e}")
            
            # Llamar a Habitaclia
            try:
                print(f"🔍 Llamando Habitaclia nivel {call_level}, radio {radius}m...")
                # Habitaclia necesita bounding box: minLat, minLon, maxLat, maxLon
                lats = [p[1] for p in polygon]
                lngs = [p[0] for p in polygon]
                habitaclia_params = {
                    "minLat": min(lats),
                    "minLon": min(lngs),
                    "maxLat": max(lats),
                    "maxLon": max(lngs),
                    "operation": "V",  # V = Venta
                    "surfaceMin": min_size,
                }
                resp = await client.post(f"{RENDER_URL}/habitaclia/scrape", json=habitaclia_params)
                if resp.status_code == 200:
                    data = resp.json()
                    # Habitaclia devuelve clusters_raw con estructura anidada
                    properties = []
                    if "clusters_raw" in data:
                        clusters = data["clusters_raw"].get("InmueblesClusterContainer", {}).get("InmueblesClusterModel", [])
                        for cluster in clusters:
                            for inmueble in cluster.get("inmuebles", []):
                                # Extraer coordenadas de Point
                                point = inmueble.get("Point", {})
                                inmueble["latitude"] = point.get("Lat")
                                inmueble["longitude"] = point.get("Lon")
                                properties.append(inmueble)
                    else:
                        properties = data.get("elementList", data.get("properties", []))
                    if properties:
                        for prop in properties:
                            prop["_source"] = "habitaclia"
                            prop["_call_level"] = call_level
                            all_comparables.append(prop)
                            comparables_by_source["habitaclia"].append(prop)
                        level_results["habitaclia"] = len(properties)
                        print(f"✅ Habitaclia nivel {call_level}: {len(properties)} resultados")
                    else:
                        print(f"⚠️ Habitaclia nivel {call_level}: sin resultados")
            except Exception as e:
                print(f"❌ Error Habitaclia nivel {call_level}: {e}")
            
            search_summary.append(level_results)
            
            # Verificar si tenemos suficientes
            total = len(all_comparables)
            if total >= MIN_TOTAL:
                print(f"✅ Suficientes comparables ({total}), parando búsqueda")
                break
    
    # ═══════════════════════════════════════════════════════════════════
    # FASE 1.5 — NORMALIZACIÓN: escribir comp["_norm"] canonical para todos
    # ═══════════════════════════════════════════════════════════════════
    for comp in all_comparables:
        normalize_comparable(comp)
    
    # ═══════════════════════════════════════════════════════════════════
    # FASE 2 — SCORE LIGHT: solo campos disponibles en scrape de las 3 fuentes
    #          → top 30 candidatos para enrichment
    # ═══════════════════════════════════════════════════════════════════
    target_proptype_norm    = normalize_propertytype(request.property_type)
    target_state_canonical  = normalize_state_canonical(request.state)  # "good"/"renew"/"newdevelopment"
    target_is_newdev        = (target_state_canonical == "newdevelopment")
    
    for comp in all_comparables:
        n = comp["_norm"]
        
        # Distancia (usando _norm.lat/lng que ya prioriza detail si existe)
        if n.get("lat") and n.get("lng"):
            comp["_distance"] = calc_distance(lat, lng, float(n["lat"]), float(n["lng"]))
        else:
            comp["_distance"] = 99999
        
        # Score LIGHT
        score = 0
        level_weight = {1: 1000, 2: 500, 3: 100}
        score += level_weight.get(comp.get("_call_level", 3), 0)
        score += max(0, 200 - comp["_distance"] / 15)
        if n.get("size"):
            score += max(0, 150 - abs(n["size"] - request.total_area) * 5)
        if n.get("rooms"):
            score += max(0, 100 - abs(n["rooms"] - request.rooms) * 30)
        if n.get("bathrooms"):
            score += max(0, 50 - abs(n["bathrooms"] - request.bathrooms) * 20)
        # Propertytype match
        if n.get("propertytype") and target_proptype_norm and n["propertytype"] == target_proptype_norm:
            score += 50
        # newdev match (alineado con state target)
        if n.get("newdev") is not None and bool(n["newdev"]) == target_is_newdev:
            score += 30
        
        comp["_score_light"] = score
    
    all_comparables.sort(key=lambda x: x.get("_score_light", 0), reverse=True)
    top30 = all_comparables[:30]
    src_counts_top30 = {"idealista": 0, "fotocasa": 0, "habitaclia": 0}
    for c in top30:
        src_counts_top30[c.get("_source", "idealista")] += 1
    print(f"🎯 Score light → top 30: {src_counts_top30}")
    
    # ═══════════════════════════════════════════════════════════════════
    # FASE 3 — ENRICHMENT HAB del top 30 (paralelo)
    #          Idealista y Fotocasa ya tienen fotos en scrape, no se enriquecen
    # ═══════════════════════════════════════════════════════════════════
    def _fix_hab_url(u):
        # Habitaclia images llegan como "//images.habimg.com/..."; añadir https:
        if not u:
            return None
        return ("https:" + u) if u.startswith("//") else u
    
    habitaclia_indices = [i for i, c in enumerate(top30) if c.get("_source") == "habitaclia"]
    if habitaclia_indices:
        print(f"🔍 Enriqueciendo {len(habitaclia_indices)} propiedades de Habitaclia (top 30)...")
        
        async with httpx.AsyncClient(timeout=60) as client:
            async def fetch_habitaclia_detail(idx):
                c = top30[idx]
                cod_emp = c.get("CodEmp", "")
                cod_inm = c.get("CodInm", "")
                if not cod_emp or not cod_inm:
                    return idx, None
                try:
                    resp = await client.post(
                        f"{RENDER_URL}/habitaclia/detail",
                        json={"codEmp": cod_emp, "codInm": cod_inm},
                        timeout=10.0
                    )
                    if resp.status_code == 200:
                        return idx, resp.json()
                except Exception as e:
                    print(f"⚠️ Error detail HAB {cod_emp}-{cod_inm}: {e}")
                return idx, None
            
            tasks = [fetch_habitaclia_detail(i) for i in habitaclia_indices]
            results = await asyncio.gather(*tasks)
            
            for idx, detail in results:
                if not detail:
                    continue
                comp = top30[idx]
                # Mergear campos del detail al comp para que normalize_comparable los lea
                comp["Images"]              = detail.get("Images", [])
                comp["ImageCount"]          = detail.get("ImageCount", 0)
                comp["DescriptionFeatures"] = detail.get("DescriptionFeatures", [])
                comp["Map"]                 = detail.get("Map") or {}
                comp["Description"]         = detail.get("Description", "")
                comp["Agency"]              = detail.get("Agency") or {}
                comp["Title"]               = detail.get("Title", "")
                comp["MainImage"]           = detail.get("MainImage", "")
                comp["DetailSegmentData"]   = detail.get("DetailSegmentData", "")
                
                # Extraer imágenes (URLXL > URLG > URL > URLNONE) con fix de scheme
                extracted_images = []
                for img in comp.get("Images") or []:
                    if isinstance(img, dict):
                        u = _fix_hab_url(img.get("URLXL") or img.get("URLG") or img.get("URL") or img.get("URLNONE"))
                        if u:
                            extracted_images.append(u)
                comp["_images"]    = extracted_images
                comp["_thumbnail"] = extracted_images[0] if extracted_images else _fix_hab_url(comp.get("MainImage"))
                
                # Re-normalizar para que _norm refleje los datos enriquecidos
                # (numphotos, exterior, haslift, lat/lng más precisos)
                normalize_comparable(comp)
                
                # Recalcular distance con coordenadas más precisas (Map.VGPSLat/Lon)
                n = comp["_norm"]
                if n.get("lat") and n.get("lng"):
                    comp["_distance"] = calc_distance(lat, lng, float(n["lat"]), float(n["lng"]))
                
                print(f"✅ HAB {comp.get('CodEmp')}-{comp.get('CodInm')}: {len(extracted_images)} fotos")
    
    # ═══════════════════════════════════════════════════════════════════
    # FASE 4 — SCORE FULL: añadir bonuses sobre score_light usando datos
    #          enriquecidos. Los campos ausentes en una source NO penalizan
    #          (no suman, pero tampoco restan): refleja la realidad de que
    #          hay menos información para juzgar similitud con la propiedad.
    # ═══════════════════════════════════════════════════════════════════
    target_haslift     = bool(request.extras.elevator)
    target_exterior    = bool(request.extras.exterior)
    target_floor_tier  = floor_tier(request.floor)  # None si no se proporciona
    target_status_idl  = map_state(request.state)   # "good"/"renew"/"newdevelopment" (formato Idealista)
    
    for comp in top30:
        n = comp["_norm"]
        score = comp.get("_score_light", 0)
        
        # 1) status match (campo Idealista-nativo, ausente en FC/HAB scrape)
        if n.get("status") is not None and n.get("status") == target_status_idl:
            score += 30
        
        # 2) exterior match (Idealista nativo + FC dynamicFeatures + HAB enrich DescriptionFeatures)
        if n.get("exterior") is not None and bool(n["exterior"]) == target_exterior:
            score += 25
        
        # 3) haslift match (Idealista nativo + FC extraList + HAB enrich DescriptionFeatures)
        if n.get("haslift") is not None and bool(n["haslift"]) == target_haslift:
            score += 25
        
        # 4) numphotos threshold (no premiar excesivamente, sí penalizar muy pocas)
        np = n.get("numphotos") or 0
        if np == 0:
            score += -10
        elif np <= 2:
            score += -5
        elif np <= 4:
            score += 0
        else:  # >= 5
            score += 10
        
        # 5) hasplan bonus (Idealista hasPlan + FC hasDescriptionPlan; HAB no tiene)
        if n.get("hasplan"):
            score += 5
        
        # 6) floor_tier match (solo si target_floor_tier se pudo derivar Y comp tiene floor_tier)
        if target_floor_tier and n.get("floor_tier") and n["floor_tier"] == target_floor_tier:
            score += 20
        
        comp["_score"] = score
    
    top30.sort(key=lambda x: x.get("_score", 0), reverse=True)
    top_comparables = top30[:15]
    src_counts_final = {"idealista": 0, "fotocasa": 0, "habitaclia": 0}
    for c in top_comparables:
        src_counts_final[c.get("_source", "idealista")] += 1
    print(f"🏆 Score full → top 15 final: {src_counts_final}")
    
    # Calcular valoración
    prices = [c["price"] for c in top_comparables if c.get("price") and c["price"] > 0]
    if prices:
        weights = [3 if c.get("_call_level") == 1 else 2 if c.get("_call_level") == 2 else 1 for c in top_comparables if c.get("price")]
        total_w = sum(weights)
        mean_price = sum(p * w for p, w in zip(prices, weights)) / total_w
        variance = sum(w * (p - mean_price)**2 for p, w in zip(prices, weights)) / total_w
        std_price = math.sqrt(variance)
        valuation = {
            "mean": round(mean_price),
            "std": round(std_price),
            "min": round(mean_price - std_price),
            "max": round(mean_price + std_price),
            "count": len(prices)
        }
    else:
        valuation = {"mean": 0, "std": 0, "min": 0, "max": 0, "count": 0}
    
    # Formatear comparables para el frontend
    formatted = []
    for c in top_comparables:
        source = c.get("_source", "idealista")
        
        # Normalizar campos según fuente
        if source == "habitaclia":
            # Habitaclia usa: PvpInm, M2Inm, NumHab, NumBany, NomZona
            # Coordenadas vienen en Point.Lat/Lon (scrape) y se refinan en Map.VGPSLat/Lon
            # (post-enrichment); el código usa `_norm.lat/lng` que ya hace prioridad correcta.
            price = c.get("PvpInm") or c.get("basic_info", {}).get("price", 0)
            size = c.get("M2Inm") or c.get("basic_info", {}).get("surface", 0)
            rooms = c.get("NumHab") or c.get("basic_info", {}).get("rooms", 0)
            bathrooms = c.get("NumBany") or c.get("basic_info", {}).get("bathrooms", 0)
            # Corregir valores negativos o inválidos
            if rooms and rooms < 0: rooms = 0
            if bathrooms and bathrooms < 0: bathrooms = 0
            address = c.get("NomZona") or c.get("basic_info", {}).get("location", "")
            title = f"{rooms} hab, {size}m² - {address}" if rooms and size else address
            prop_id = f"{c.get('CodEmp', '')}-{c.get('CodInm', '')}"
            url = f"https://www.habitaclia.com/vivienda-{prop_id}.htm"
            thumbnail = c.get("_thumbnail", "")
            images = c.get("_images", [])
            # Coordenadas: usar _norm que prioriza VGPSLat/Lon (detail) > Point.Lat/Lon (scrape)
            n_hab = c.get("_norm") or {}
            lat = n_hab.get("lat") or c.get("latitude")
            lng = n_hab.get("lng") or c.get("longitude")
        elif source == "fotocasa":
            # Fotocasa: placeholders[].property con pageSize=36 incluye todos los datos
            price = c.get("price", 0)
            size = c.get("surface", 0)
            rooms = c.get("rooms", 0)
            bathrooms = c.get("bathrooms", 0)
            address = c.get("locationDescription", "")
            subtype = c.get("propertySubtype", "Vivienda")
            title = f"{rooms} hab, {size}m² - {address}" if rooms and size else subtype.capitalize()
            prop_id = c.get("propertyId") or c.get("id", "").replace("1_", "")
            url = c.get("urlMarketplace") or f"https://www.fotocasa.es/es/comprar/vivienda/{prop_id}"
            # Imágenes desde mediaList. Tech debt: extraer LARGE (~800px) de mediaUrlDtos
            # en lugar de la URL top-level que es LARGE_LEGACY (~1200px), para tener
            # tamaño consistente con Idealista en la galería del frontend.
            media_list = c.get("mediaList", [])
            def _fc_pick_large(media_item):
                # Preferir LARGE de mediaUrlDtos; fallback a url top-level si no hay
                if not isinstance(media_item, dict):
                    return None
                dtos = media_item.get("mediaUrlDtos") or []
                for dto in dtos:
                    if isinstance(dto, dict) and dto.get("mediaSize") == "LARGE" and dto.get("url"):
                        return dto["url"]
                return media_item.get("url")
            images = [u for u in (_fc_pick_large(m) for m in media_list) if u]
            thumbnail = images[0] if images else ""
            lat = c.get("latitude")
            lng = c.get("longitude")
        else:
            # Idealista
            price = c.get("price", 0)
            size = c.get("size", 0)
            rooms = c.get("rooms", 0)
            bathrooms = c.get("bathrooms", 0)
            address = c.get("address") or c.get("neighborhood", "")
            title = c.get("suggestedTexts", {}).get("title") or f"{rooms} hab, {size}m²"
            prop_id = c.get("propertyCode") or c.get("id")
            url = c.get("url", "")
            thumbnail = c.get("thumbnail", "")
            # Idealista trae multimedia como dict {"images": [{"url":..., "tag":...}, ...]}
            # (verificado con curl directo al scraper). Defensa contra otras formas posibles.
            multimedia = c.get("multimedia", {})
            images = []
            if isinstance(multimedia, dict):
                img_list = multimedia.get("images") or []
                if isinstance(img_list, list):
                    images = [img.get("url") for img in img_list if isinstance(img, dict) and img.get("url")]
            elif isinstance(multimedia, list):
                # Fallback defensivo: lista directa
                for item in multimedia:
                    if isinstance(item, str):
                        images.append(item)
                    elif isinstance(item, dict) and item.get("url"):
                        images.append(item["url"])
            lat = c.get("latitude")
            lng = c.get("longitude")
        
        # Recalcular distancia si tenemos coordenadas
        if lat and lng:
            distance = calc_distance(request.coordinates["lat"], request.coordinates["lng"], float(lat), float(lng))
        else:
            distance = c.get("_distance", 99999)
        
        formatted.append({
            "id": prop_id,
            "source": source,
            "title": title,
            "address": address,
            "price": price or 0,
            "priceByArea": round(price / size) if price and size else 0,
            "size": size or 0,
            "rooms": rooms or 0,
            "bathrooms": bathrooms or 0,
            "floor": c.get("floor"),
            "status": c.get("status"),
            "url": url,
            "thumbnail": thumbnail,
            "images": images,
            "distance": round(distance) if distance < 90000 else None,
            "call_level": c.get("_call_level", 0),
            "score": round(c.get("_score", 0)),
            "in_range": valuation["min"] <= (price or 0) <= valuation["max"] if price else False,
            "latitude": lat,
            "longitude": lng,
        })
    
    return {
        "success": True,
        "coordinates": request.coordinates,
        "address": request.address,
        "search_summary": search_summary,
        "valuation": valuation,
        "comparables": formatted,
        "total_found": len(all_comparables),
        "search_radius": SEARCH_RADII[len(search_summary) - 1] if search_summary else 0,
    }

@app.post("/api/generate-dossier")
async def generate_dossier(request: GenerateDossierRequest):
    try:
        data = {
            "property": request.property.model_dump(),
            "coordinates": request.coordinates,
            "valuation": request.valuation.model_dump(),
            "comparables": [c.model_dump() for c in request.comparables],
        }
        pdf_bytes = await generate_dossier_pdf(data)
        filename = generate_dossier_filename(request.property.address)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        print(f"[dossier] Error: {e}")
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)