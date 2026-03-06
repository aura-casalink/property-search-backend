"""
dossier.py
Genera el PDF del dossier de valoración de propiedad.
v4: Rectángulos normales, alineación correcta, nombre mejorado.
"""

import io
import os
import re
import httpx
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white, black
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

# Colores AURA
AURA_YELLOW = HexColor("#F5B800")
AURA_YELLOW_LIGHT = HexColor("#fef9e7")
AURA_DARK = HexColor("#1a1a1a")
AURA_GRAY = HexColor("#6b7280")
AURA_LIGHT_GRAY = HexColor("#f5f5f5")

# URLs de logos de portales
PORTAL_LOGOS = {
    "idealista": "https://cdn.aura-app.es/img/portal_idealista.png",
    "fotocasa": "https://cdn.aura-app.es/img/portal_fotocasa.png",
    "habitaclia": "https://cdn.aura-app.es/img/portal_habitaclia.png",
}

def format_price(price):
    """Formatea precio en formato español: 1.234.567 €"""
    if not price:
        return "-"
    return f"{int(price):,}".replace(",", ".") + " €"

def format_price_m2(price, area):
    """Calcula y formatea precio por m²"""
    if not price or not area:
        return "-"
    return f"{int(price / area):,}".replace(",", ".") + " €/m²"

async def fetch_image(url: str, timeout: int = 10) -> bytes:
    """Descarga una imagen desde URL"""
    if not url:
        return None
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            response = await client.get(url, headers=headers)
            if response.status_code == 200:
                return response.content
    except Exception as e:
        print(f"[dossier] Error fetching {url[:60]}...: {e}")
    return None

async def fetch_map_image(lat: float, lng: float, width: int = 400, height: int = 250) -> bytes:
    """Descarga imagen de mapa estático"""
    
    print(f"[dossier] Fetching map for: {lat}, {lng}")
    
    if not lat or not lng or lat == 0 or lng == 0:
        print("[dossier] Invalid coordinates")
        return None
    
    # Opción 1: Mapbox con token de variable de entorno
    mapbox_token = os.getenv("MAPBOX_TOKEN", "")
    if mapbox_token and len(mapbox_token) > 50:
        try:
            url = f"https://api.mapbox.com/styles/v1/mapbox/streets-v12/static/pin-l+F5B800({lng},{lat})/{lng},{lat},15,0/{width}x{height}@2x?access_token={mapbox_token}"
            print(f"[dossier] Trying Mapbox...")
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                response = await client.get(url)
                print(f"[dossier] Mapbox response: {response.status_code}")
                if response.status_code == 200 and len(response.content) > 5000:
                    print(f"[dossier] ✅ Mapbox success")
                    return response.content
        except Exception as e:
            print(f"[dossier] Mapbox error: {e}")
    
    # Opción 2: Thunderforest (OSM tiles con estilo limpio)
    try:
        # Usando tiles de CartoCDN (gratis, sin auth)
        import math
        zoom = 15
        n = 2 ** zoom
        x_tile = int((lng + 180) / 360 * n)
        lat_rad = math.radians(lat)
        y_tile = int((1 - math.asinh(math.tan(lat_rad)) / math.pi) / 2 * n)
        
        # Cargar múltiples tiles para formar una imagen más grande
        url = f"https://a.basemaps.cartocdn.com/rastertiles/voyager/{zoom}/{x_tile}/{y_tile}.png"
        print(f"[dossier] Trying CartoCDN tile...")
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            headers = {"User-Agent": "Mozilla/5.0 AURA-Valuations/1.0"}
            response = await client.get(url, headers=headers)
            print(f"[dossier] CartoCDN response: {response.status_code}, size: {len(response.content)}")
            if response.status_code == 200 and len(response.content) > 1000:
                print(f"[dossier] ✅ CartoCDN success")
                return response.content
    except Exception as e:
        print(f"[dossier] CartoCDN error: {e}")
    
    # Opción 3: OpenStreetMap tiles directos
    try:
        import math
        zoom = 15
        n = 2 ** zoom
        x_tile = int((lng + 180) / 360 * n)
        lat_rad = math.radians(lat)
        y_tile = int((1 - math.asinh(math.tan(lat_rad)) / math.pi) / 2 * n)
        
        url = f"https://tile.openstreetmap.org/{zoom}/{x_tile}/{y_tile}.png"
        print(f"[dossier] Trying OSM tile: {url}")
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            headers = {"User-Agent": "Mozilla/5.0 AURA-Valuations/1.0 (contact@aura-app.es)"}
            response = await client.get(url, headers=headers)
            print(f"[dossier] OSM tile response: {response.status_code}")
            if response.status_code == 200 and len(response.content) > 1000:
                print(f"[dossier] ✅ OSM tile success")
                return response.content
    except Exception as e:
        print(f"[dossier] OSM tile error: {e}")
    
    print("[dossier] ❌ All map providers failed")
    return None

async def generate_dossier_pdf(data: dict) -> bytes:
    """Genera el PDF del dossier de valoración."""
    
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    
    margin = 15 * mm
    content_width = width - (2 * margin)
    
    # Datos
    prop = data.get("property", {})
    coords = data.get("coordinates", {})
    valuation = data.get("valuation", {})
    comparables = data.get("comparables", [])
    
    print(f"[dossier] === GENERATING PDF ===")
    print(f"[dossier] Address: {prop.get('address')}")
    print(f"[dossier] Coordinates: {coords}")
    print(f"[dossier] Comparables: {len(comparables)}")
    
    # Filtrar seleccionados
    selected_comparables = [comp for comp in comparables if comp.get("selected", False) or comp.get("in_range", False)]
    if not selected_comparables:
        selected_comparables = comparables[:10]
    
    # Calcular precios min/max
    sorted_by_price = sorted([comp for comp in selected_comparables if comp.get("price", 0) > 0], key=lambda x: x["price"])
    lowest_3 = sorted_by_price[:3]
    highest_3 = sorted_by_price[-3:] if len(sorted_by_price) >= 3 else sorted_by_price
    
    price_min_avg = sum(comp["price"] for comp in lowest_3) / len(lowest_3) if lowest_3 else 0
    price_max_avg = sum(comp["price"] for comp in highest_3) / len(highest_3) if highest_3 else 0
    
    total_area = prop.get("total_area", 0)
    main_price = valuation.get("mean", 0)
    
    # ========== HEADER ==========
    header_height = 18 * mm
    c.setFillColor(AURA_DARK)
    c.rect(0, height - header_height, width, header_height, fill=1, stroke=0)
    
    c.setFillColor(AURA_YELLOW)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin, height - 12 * mm, "AURA Valuations")
    
    c.setFillColor(HexColor("#999999"))
    c.setFont("Helvetica", 8)
    c.drawRightString(width - margin, height - 12 * mm, "Informe de Valoración")
    
    # ========== TÍTULO ==========
    y = height - header_height - 10 * mm
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 13)
    address = prop.get("address", "Dirección no disponible")
    if len(address) > 55:
        address = address[:52] + "..."
    c.drawString(margin, y, address)
    
    y -= 5 * mm
    
    # ========== LAYOUT: MAPA (izq) + VALORACIÓN (der) ==========
    map_width = 88 * mm
    map_height = 55 * mm
    map_x = margin
    map_y = y - map_height
    
    # === MAPA ===
    map_image = None
    lat = coords.get("lat", 0)
    lng = coords.get("lng", 0)
    
    if lat and lng and lat != 0 and lng != 0:
        map_bytes = await fetch_map_image(lat, lng)
        if map_bytes:
            try:
                map_image = ImageReader(io.BytesIO(map_bytes))
            except Exception as e:
                print(f"[dossier] Error creating ImageReader: {e}")
    
    if map_image:
        try:
            c.drawImage(map_image, map_x, map_y, width=map_width, height=map_height, preserveAspectRatio=True, mask='auto')
        except Exception as e:
            print(f"[dossier] Error drawing map: {e}")
            map_image = None
    
    if not map_image:
        # Rectángulo normal para placeholder
        c.setFillColor(AURA_LIGHT_GRAY)
        c.rect(map_x, map_y, map_width, map_height, fill=1, stroke=0)
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 9)
        c.drawCentredString(map_x + map_width/2, map_y + map_height/2, "Mapa no disponible")
    
    # Borde del mapa
    c.setStrokeColor(HexColor("#e0e0e0"))
    c.setLineWidth(0.5)
    c.rect(map_x, map_y, map_width, map_height, fill=0, stroke=1)
    
    # ========== VALORACIÓN (derecha) ==========
    val_x = map_x + map_width + 8 * mm
    val_width = content_width - map_width - 8 * mm
    
    # PRECIO PRINCIPAL - Centrado, negro
    price_y = y - 12 * mm
    c.setFillColor(black)
    c.setFont("Helvetica-Bold", 26)
    c.drawCentredString(val_x + val_width/2, price_y, format_price(main_price))
    
    # Precio por m²
    price_m2_y = price_y - 8 * mm
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 11)
    c.drawCentredString(val_x + val_width/2, price_m2_y, format_price_m2(main_price, total_area))
    
    # === TARJETAS MIN/MAX - Rectángulos normales ===
    cards_top = price_m2_y - 10 * mm
    card_width = (val_width - 4 * mm) / 2
    card_height = 24 * mm
    card_y = cards_top - card_height
    
    # Tarjeta Mínimo
    c.setFillColor(AURA_YELLOW_LIGHT)
    c.rect(val_x, card_y, card_width, card_height, fill=1, stroke=0)
    c.setStrokeColor(AURA_YELLOW)
    c.setLineWidth(1)
    c.rect(val_x, card_y, card_width, card_height, fill=0, stroke=1)
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 7)
    c.drawCentredString(val_x + card_width/2, card_y + card_height - 6*mm, "Precio Mínimo")
    
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(val_x + card_width/2, card_y + card_height - 14*mm, format_price(price_min_avg))
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 6)
    c.drawCentredString(val_x + card_width/2, card_y + card_height - 20*mm, format_price_m2(price_min_avg, total_area))
    
    # Tarjeta Máximo
    max_card_x = val_x + card_width + 4 * mm
    c.setFillColor(AURA_YELLOW_LIGHT)
    c.rect(max_card_x, card_y, card_width, card_height, fill=1, stroke=0)
    c.setStrokeColor(AURA_YELLOW)
    c.setLineWidth(1)
    c.rect(max_card_x, card_y, card_width, card_height, fill=0, stroke=1)
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 7)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 6*mm, "Precio Máximo")
    
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 14*mm, format_price(price_max_avg))
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 6)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 20*mm, format_price_m2(price_max_avg, total_area))
    
    # ========== CARACTERÍSTICAS (debajo del mapa) ==========
    chars_y = map_y - 6 * mm
    chars_height = 16 * mm
    chars_width = map_width
    
    # Rectángulo normal
    c.setFillColor(AURA_YELLOW_LIGHT)
    c.rect(margin, chars_y - chars_height, chars_width, chars_height, fill=1, stroke=0)
    c.setStrokeColor(AURA_YELLOW)
    c.setLineWidth(1)
    c.rect(margin, chars_y - chars_height, chars_width, chars_height, fill=0, stroke=1)
    
    items = [
        (f"{prop.get('total_area', '-')} m²", "Superficie"),
        (f"{prop.get('rooms', '-')} hab.", "Habitaciones"),
        (f"{prop.get('bathrooms', '-')} baños", "Baños"),
    ]
    
    col_width = chars_width / 3
    for i, (value, label) in enumerate(items):
        cx = margin + (i * col_width) + col_width / 2
        
        c.setFillColor(AURA_DARK)
        c.setFont("Helvetica-Bold", 11)
        c.drawCentredString(cx, chars_y - 6*mm, value)
        
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 7)
        c.drawCentredString(cx, chars_y - 12*mm, label)
    
    # ========== COMPARABLES (2x2) ==========
    comps_section_y = chars_y - chars_height - 12 * mm
    
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin, comps_section_y, "Propiedades Comparables")
    
    comps_y = comps_section_y - 5 * mm
    
    top_comparables = selected_comparables[:4]
    if len(top_comparables) < 4 and len(comparables) >= 4:
        top_comparables = comparables[:4]
    
    # Grid 2x2
    comp_card_width = (content_width - 6*mm) / 2
    comp_card_height = 48 * mm
    gap_x = 6 * mm
    gap_y = 5 * mm
    
    # Tamaño del thumbnail (referencia para alineación)
    thumb_width = 42 * mm
    thumb_height = comp_card_height - 8 * mm
    
    # Pre-cargar logos
    portal_images = {}
    for portal, url in PORTAL_LOGOS.items():
        img_bytes = await fetch_image(url)
        if img_bytes:
            try:
                portal_images[portal] = ImageReader(io.BytesIO(img_bytes))
            except:
                pass
    
    # Dibujar comparables
    for i, comp in enumerate(top_comparables):
        col = i % 2
        row = i // 2
        
        cx = margin + col * (comp_card_width + gap_x)
        cy = comps_y - (row + 1) * comp_card_height - row * gap_y
        
        # Fondo tarjeta - rectángulo normal
        c.setFillColor(white)
        c.rect(cx, cy, comp_card_width, comp_card_height, fill=1, stroke=0)
        c.setStrokeColor(HexColor("#e0e0e0"))
        c.setLineWidth(0.5)
        c.rect(cx, cy, comp_card_width, comp_card_height, fill=0, stroke=1)
        
        # === FOTO (izquierda) ===
        img_x = cx + 3 * mm
        img_y = cy + 4 * mm
        
        thumbnail_url = comp.get("thumbnail", "")
        comp_image = None
        
        if thumbnail_url:
            img_bytes = await fetch_image(thumbnail_url)
            if img_bytes:
                try:
                    comp_image = ImageReader(io.BytesIO(img_bytes))
                except:
                    pass
        
        if comp_image:
            try:
                c.drawImage(comp_image, img_x, img_y, width=thumb_width, height=thumb_height, 
                           preserveAspectRatio=True, mask='auto')
            except:
                comp_image = None
        
        if not comp_image:
            c.setFillColor(HexColor("#f0f0f0"))
            c.rect(img_x, img_y, thumb_width, thumb_height, fill=1, stroke=0)
            c.setFillColor(HexColor("#cccccc"))
            c.setFont("Helvetica", 20)
            c.drawCentredString(img_x + thumb_width/2, img_y + thumb_height/2 - 4*mm, "🏠")
        
        # === INFO (derecha, alineada al thumbnail) ===
        info_x = img_x + thumb_width + 4 * mm
        
        # Precio - alineado arriba del thumbnail
        text_y = img_y + thumb_height - 4 * mm
        c.setFillColor(AURA_DARK)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(info_x, text_y, format_price(comp.get("price", 0)))
        
        # €/m²
        text_y -= 5 * mm
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 8)
        c.drawString(info_x, text_y, format_price_m2(comp.get("price", 0), comp.get("size", 0)))
        
        # Características
        text_y -= 8 * mm
        c.setFont("Helvetica", 8)
        rooms = comp.get('rooms', '-')
        baths = comp.get('bathrooms', '-')
        size = comp.get('size', '-')
        c.drawString(info_x, text_y, f"{rooms} hab. · {baths} baños")
        
        text_y -= 5 * mm
        c.drawString(info_x, text_y, f"{size} m²")
        
        # "Ver propiedad →" - alineado abajo del thumbnail
        c.setFillColor(HexColor("#3b82f6"))
        c.setFont("Helvetica", 7)
        c.drawString(info_x, img_y + 2*mm, "Ver propiedad →")
        
        url = comp.get("url", "")
        if url:
            c.linkURL(url, (info_x, img_y, info_x + 30*mm, img_y + 8*mm))
        
        # === LOGO PORTAL (esquina inferior derecha de la tarjeta) ===
        source = comp.get("source", "idealista").lower()
        if source in portal_images:
            logo_width = 16 * mm
            logo_height = 5.5 * mm
            logo_x = cx + comp_card_width - logo_width - 4*mm
            logo_y = cy + 4*mm
            try:
                c.drawImage(portal_images[source], logo_x, logo_y, 
                           width=logo_width, height=logo_height, 
                           preserveAspectRatio=True, mask='auto')
            except:
                pass
    
    # ========== FOOTER ==========
    footer_height = 16 * mm
    c.setFillColor(AURA_DARK)
    c.rect(0, 0, width, footer_height, fill=1, stroke=0)
    
    c.setFillColor(AURA_YELLOW)
    c.setFont("Helvetica-Bold", 9)
    c.drawCentredString(width/2, 9*mm, "Powered by AURA")
    
    c.setFillColor(HexColor("#888888"))
    c.setFont("Helvetica", 6)
    c.drawCentredString(width/2, 4*mm, "Herramientas inteligentes para agencias inmobiliarias")
    
    # Fecha y comparables
    c.setFillColor(HexColor("#666666"))
    c.setFont("Helvetica", 6)
    today = datetime.now().strftime("%d/%m/%Y")
    c.drawString(margin, footer_height + 2*mm, f"Generado: {today}")
    c.drawRightString(width - margin, footer_height + 2*mm, f"{len(selected_comparables)} comparables analizados")
    
    c.save()
    print(f"[dossier] ✅ PDF generated")
    
    return buffer.getvalue()


def generate_dossier_filename(address: str) -> str:
    """
    Genera nombre de archivo para el PDF.
    Formato: AAMMDD_HHMM_[calle]_[número]_valoración_by_AURA.pdf
    """
    now = datetime.now()
    date_str = now.strftime("%y%m%d_%H%M")
    
    # Extraer calle y número de la dirección
    # Ejemplo: "Calle Irun, 23" -> "Irun_23"
    # Ejemplo: "Calle González Rubio, 6" -> "Gonzalez_Rubio_6"
    
    # Limpiar caracteres especiales
    clean_address = address.replace("Calle ", "").replace("Avenida ", "").replace("Plaza ", "")
    clean_address = clean_address.replace("C/ ", "").replace("Av. ", "").replace("Pz. ", "")
    
    # Quitar acentos
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
        'ñ': 'n', 'Ñ': 'N', 'ü': 'u', 'Ü': 'U'
    }
    for old, new in replacements.items():
        clean_address = clean_address.replace(old, new)
    
    # Solo alphanumeric y espacios/comas
    clean_address = re.sub(r'[^a-zA-Z0-9\s,]', '', clean_address)
    
    # Separar por coma (calle, número)
    parts = clean_address.split(',')
    if len(parts) >= 2:
        street = parts[0].strip().replace(' ', '_')[:30]
        number = parts[1].strip().replace(' ', '')[:10]
        location = f"{street}_{number}"
    else:
        location = clean_address.replace(' ', '_').replace(',', '_')[:40]
    
    return f"{date_str}_{location}_valoracion_by_AURA.pdf"
