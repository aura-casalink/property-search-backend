# Property Search Backend

Backend API para el sistema de tasación de propiedades de AURA. Integra datos del Catastro con búsquedas en los principales portales inmobiliarios españoles.

## 🏗️ Arquitectura
```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│   Frontend      │────▶│  Property Search     │────▶│  Scraper Service    │
│   (HTML/React)  │     │  Backend (este repo) │     │  (Render - scrapers)│
└─────────────────┘     └──────────────────────┘     └─────────────────────┘
                                   │
                                   ▼
                        ┌──────────────────────┐
                        │  Hetzner Server      │
                        │  (Catastro + Cookies)│
                        └──────────────────────┘
```

## 🚀 Endpoints

### Búsqueda de dirección
```
POST /api/search-address
Body: { "query": "Calle Example 123, Madrid" }
```

### Obtener edificio
```
POST /api/get-building
Body: { "city_slug": "madrid-madrid", "street_slug": "calle-example", "number": "123" }
```

### Obtener propiedad
```
POST /api/get-property
Body: { "city_slug": "...", "street_slug": "...", "number": "...", "floor": "2", "door": "A" }
```

### Buscar comparables
```
POST /api/search-comparables
Body: {
  "coordinates": { "lat": 40.43, "lng": -3.67 },
  "total_area": 100,
  "rooms": 3,
  "bathrooms": 2,
  "state": "buen_estado",
  "property_type": "piso",
  "extras": { "elevator": true, "garage": false, ... },
  "special_situation": { "nuda_propiedad": false, ... }
}
```

## 📊 Fuentes de datos

| Fuente | Datos | Método |
|--------|-------|--------|
| **Catastro** | Superficie, año, planta, coordenadas | API Hetzner |
| **Idealista** | Precio, habitaciones, baños, extras | Scraper Render |
| **Fotocasa** | Precio, habitaciones, baños, extras | Scraper Render |
| **Habitaclia** | Precio, habitaciones, baños, extras | Scraper Render |

## 🔍 Lógica de búsqueda de comparables

1. **Nivel 1** (1.25km): Filtros exactos (±15% superficie, mismas hab/baños)
2. **Nivel 2** (2km): Filtros relajados (±20% superficie, ±1 hab/baños)
3. **Nivel 3** (3km): Filtros amplios (±30% superficie, ±1 hab/baños)

Se detiene cuando encuentra ≥20 comparables.

## 💰 Cálculo de valoración

- **Media ponderada**: Nivel 1 (peso 3), Nivel 2 (peso 2), Nivel 3 (peso 1)
- **Rango**: Media ± 1 desviación típica

## 🛠️ Desarrollo local
```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar servidor
PORT=8001 python server.py

# Probar endpoint
curl http://localhost:8001/health
```

## 🌐 Variables de entorno

| Variable | Descripción | Default |
|----------|-------------|---------|
| `PORT` | Puerto del servidor | 8000 |
| `COOKIE_SERVER_URL` | URL del servidor Hetzner | http://37.27.8.255:5001 |
| `RENDER_URL` | URL del servicio de scrapers | https://idealista-async-scraper.onrender.com |

## 📦 Despliegue en Render

1. Conectar repo a Render
2. Build command: `pip install -r requirements.txt`
3. Start command: `uvicorn server:app --host 0.0.0.0 --port $PORT`

---

Desarrollado para **AURA** 🏠
