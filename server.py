"""
Backend para búsqueda de propiedades
Flujo Idealista-first + datos del Catastro
"""

import os
import json
import httpx
import math
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Optional

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
    state: str = "buen_estado"
    property_type: str = "piso"
    extras: ExtrasInput = ExtrasInput()
    special_situation: SpecialSituationInput = SpecialSituationInput()
    cadastre_ref: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    building: Optional[dict] = None

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
        return {"obra_nueva": "newdevelopment", "buen_estado": "good"}.get(state, "renew")
    
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
            
            # Añadir filtros de tipo
            type_map = {
                "atico": "penthouse", "duplex": "duplex", "apartamento": "apartamentoType",
                "loft": "loftType", "chalet_independiente": "independentHouse",
                "chalet_pareado": "semidetachedHouse", "chalet_adosado": "terracedHouse",
                "casa_rustica": "countryHouse", "villa": "villaType", "piso": "flat"
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
    
    # Calcular scores y rankear
    for comp in all_comparables:
        source = comp.get("_source", "idealista")
        # Obtener coordenadas según fuente
        if source == "habitaclia":
            comp_lat = comp.get("Lat") or comp.get("latitude")
            comp_lng = comp.get("Lon") or comp.get("longitude")
        else:
            comp_lat = comp.get("latitude")
            comp_lng = comp.get("longitude")
        
        if comp_lat and comp_lng:
            comp["_distance"] = calc_distance(lat, lng, float(comp_lat), float(comp_lng))
        else:
            comp["_distance"] = 99999
        
        score = 0
        level_weight = {1: 1000, 2: 500, 3: 100}
        score += level_weight.get(comp.get("_call_level", 3), 0)
        score += max(0, 200 - comp["_distance"] / 15)
        if comp.get("size"):
            score += max(0, 150 - abs(comp["size"] - request.total_area) * 5)
        if comp.get("rooms"):
            score += max(0, 100 - abs(comp["rooms"] - request.rooms) * 30)
        if comp.get("bathrooms"):
            score += max(0, 50 - abs(comp["bathrooms"] - request.bathrooms) * 20)
        if comp.get("numPhotos", 0) > 0:
            score += min(30, comp["numPhotos"] * 2)
        if comp.get("hasPlan"):
            score += 20
        comp["_score"] = score
    
    all_comparables.sort(key=lambda x: x.get("_score", 0), reverse=True)
    top_comparables = all_comparables[:15]
    
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
            # NOTA: Las coordenadas solo vienen en enrichment (detail API), no en scrape
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
            thumbnail = ""
            # Coordenadas ya normalizadas desde Point.Lat/Point.Lon
            lat = c.get("latitude")
            lng = c.get("longitude")
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
            # Thumbnail desde mediaList
            media_list = c.get("mediaList", [])
            thumbnail = media_list[0].get("url", "") if media_list else ""
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)