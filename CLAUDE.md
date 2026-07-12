# CLAUDE.md — property-search-backend (tasación legacy — posiblemente superseded)

> **Protocolo común AURA:** vive en el CLAUDE.md de la carpeta padre (canónico:
> `aura-contracts/protocol/CLAUDE-master.md`). Este fichero solo añade lo específico del repo.
> Núcleo mínimo si abres este repo suelto: (1) push a `main` probablemente auto-despliega en
> Render (config en su dashboard), sin CI ni tests; (2) rama por sesión vía worktree + PR;
> (3) OK de Pablo al plan antes de construir; (4) jamás secretos en commits.

## ⚠️ Antes de construir NADA aquí
El repo está dormido desde 2026-04-28 y el ecosistema ha consolidado la tasación en v2 +
`aura-report-service` (`REPORT_PROVIDER=aura` vivo en prod). **Verifica con Pablo si este
servicio sigue en el path de algún cliente antes de invertir trabajo aquí** — puede estar
superseded.

## Qué es
FastAPI stateless para el flujo de valoración: resuelve direcciones/edificios contra Catastro
(vía cookie server de Hetzner), busca comparables por los 3 portales (vía scraper Render) y
genera dossiers PDF (reportlab). Dos ficheros: `server.py` (~1.250 líneas, 7 endpoints) y
`dossier.py`. Sin DB.

## Verificación local
```bash
python3 -m py_compile server.py dossier.py
PORT=8001 python3 server.py   # arranca sin secrets; curl http://localhost:8001/health
```

## Gotchas
- **El README documenta la env var equivocada**: el código lee `RENDER_SERVER_URL` (no
  `RENDER_URL`); poner el nombre del README en Render cae en silencio al default.
- Dependencias externas hardcodeadas: cookie server Hetzner `http://37.27.8.255:5001` (HTTP
  plano, IP cruda) y el scraper de Render — el comportamiento depende de que esos hosts vivan.
- `MAPBOX_TOKEN` ausente degrada el mapa del dossier (no crashea).
- CORS abierto (`allow_origins=['*']` + credentials) — deuda conocida.
- Hay un `server.py.bak` untracked (58KB, casi duplicado): **no editarlo por error, no
  commitearlo**.

## fixes.md
Este repo adopta `fixes.md` append-only (reglas dentro del fichero y §6 del protocolo común).

## Seguridad
Nunca tokens en commits. Este archivo va a git.
