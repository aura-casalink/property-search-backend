"""
dossier.py
Genera el PDF del dossier de valoración de propiedad.
v3: Layout 2x2 comparables, colores corregidos, mapa mejorado.
"""

import io
import os
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
    
    # Opción 1: Mapbox
    mapbox_token = os.getenv("MAPBOX_TOKEN", "")
    if mapbox_token:
        try:
            url = f"https://api.mapbox.com/styles/v1/mapbox/streets-v12/static/pin-l+F5B800({lng},{lat})/{lng},{lat},15,0/{width}x{height}@2x?access_token={mapbox_token}"
            print(f"[dossier] Trying Mapbox with token: {mapbox_token[:20]}...")
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                response = await client.get(url)
                print(f"[dossier] Mapbox response: {response.status_code}")
                if response.status_code == 200:
                    print(f"[dossier] ✅ Mapbox success, size: {len(response.content)} bytes")
                    return response.content
                else:
                    print(f"[dossier] Mapbox error body: {response.text[:200]}")
        except Exception as e:
            print(f"[dossier] Mapbox exception: {e}")
    else:
        print("[dossier] No MAPBOX_TOKEN in environment")
    
    # Opción 2: Geoapify (gratis con API key demo)
    try:
        geoapify_key = os.getenv("GEOAPIFY_KEY", "")
        if geoapify_key:
            url = f"https://maps.geoapify.com/v1/staticmap?style=osm-bright&width={width}&height={height}&center=lonlat:{lng},{lat}&zoom=15&marker=lonlat:{lng},{lat};color:%23F5B800;size:large&apiKey={geoapify_key}"
        else:
            # Sin API key, usar demo limitado
            url = f"https://maps.geoapify.com/v1/staticmap?style=osm-bright&width={width}&height={height}&center=lonlat:{lng},{lat}&zoom=15"
        
        print(f"[dossier] Trying Geoapify...")
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            response = await client.get(url)
            print(f"[dossier] Geoapify response: {response.status_code}")
            if response.status_code == 200 and len(response.content) > 1000:
                print(f"[dossier] ✅ Geoapify success")
                return response.content
    except Exception as e:
        print(f"[dossier] Geoapify error: {e}")
    
    # Opción 3: OpenStreetMap Static Map API
    try:
        url = f"https://staticmap.openstreetmap.de/staticmap.php?center={lat},{lng}&zoom=15&size={width}x{height}&maptype=mapnik&markers={lat},{lng},red-pushpin"
        print(f"[dossier] Trying OSM static map...")
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            headers = {"User-Agent": "Mozilla/5.0 AURA-Valuations/1.0"}
            response = await client.get(url, headers=headers)
            print(f"[dossier] OSM response: {response.status_code}, size: {len(response.content)}")
            if response.status_code == 200 and len(response.content) > 5000:
                print(f"[dossier] ✅ OSM success")
                return response.content
    except Exception as e:
        print(f"[dossier] OSM error: {e}")
    
    print("[dossier] ❌ All map providers failed")
    return None

def draw_rounded_rect(c, x, y, width, height, radius, fill_color=None, stroke_color=None, stroke_width=0.5):
    """Dibuja un rectángulo con esquinas redondeadas"""
    p = c.beginPath()
    p.moveTo(x + radius, y)
    p.lineTo(x + width - radius, y)
    p.arcTo(x + width - radius, y, x + width, y + radius, radius)
    p.lineTo(x + width, y + height - radius)
    p.arcTo(x + width, y + height - radius, x + width - radius, y + height, radius)
    p.lineTo(x + radius, y + height)
    p.arcTo(x + radius, y + height, x, y + height - radius, radius)
    p.lineTo(x, y + radius)
    p.arcTo(x, y + radius, x + radius, y, radius)
    p.close()
    
    if fill_color:
        c.setFillColor(fill_color)
        c.drawPath(p, fill=1, stroke=0)
    if stroke_color:
        c.setStrokeColor(stroke_color)
        c.setLineWidth(stroke_width)
        c.drawPath(p, fill=0, stroke=1)

async def generate_dossier_pdf(data: dict) -> bytes:
    """Genera el PDF del dossier de valoración."""
    
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4  # 210mm x 297mm
    
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
        draw_rounded_rect(c, map_x, map_y, map_width, map_height, 3*mm, fill_color=AURA_LIGHT_GRAY)
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 9)
        c.drawCentredString(map_x + map_width/2, map_y + map_height/2, "Mapa no disponible")
    
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
    price_text = format_price(main_price)
    c.drawCentredString(val_x + val_width/2, price_y, price_text)
    
    # Precio por m²
    price_m2_y = price_y - 8 * mm
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 11)
    c.drawCentredString(val_x + val_width/2, price_m2_y, format_price_m2(main_price, total_area))
    
    # === TARJETAS MIN/MAX - Fondo amarillo ===
    cards_top = price_m2_y - 10 * mm
    card_width = (val_width - 4 * mm) / 2
    card_height = 24 * mm
    card_y = cards_top - card_height
    
    # Tarjeta Mínimo
    draw_rounded_rect(c, val_x, card_y, card_width, card_height, 2*mm, 
                      fill_color=AURA_YELLOW_LIGHT, stroke_color=AURA_YELLOW)
    
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
    draw_rounded_rect(c, max_card_x, card_y, card_width, card_height, 2*mm, 
                      fill_color=AURA_YELLOW_LIGHT, stroke_color=AURA_YELLOW)
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 7)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 6*mm, "Precio Máximo")
    
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 14*mm, format_price(price_max_avg))
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 6)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 20*mm, format_price_m2(price_max_avg, total_area))
    
    # ========== CARACTERÍSTICAS (debajo del mapa, mismo ancho que mapa) ==========
    chars_y = map_y - 6 * mm
    chars_height = 16 * mm
    chars_width = map_width  # Mismo ancho que el mapa
    
    draw_rounded_rect(c, margin, chars_y - chars_height, chars_width, chars_height, 3*mm, 
                      fill_color=AURA_YELLOW_LIGHT, stroke_color=AURA_YELLOW)
    
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
    
    # ========== COMPARABLES (2 columnas x 2 filas) ==========
    comps_section_y = chars_y - chars_height - 12 * mm
    
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin, comps_section_y, "Propiedades Comparables")
    
    comps_y = comps_section_y - 5 * mm
    
    # 4 comparables en grid 2x2
    top_comparables = selected_comparables[:4]
    if len(top_comparables) < 4 and len(comparables) >= 4:
        top_comparables = comparables[:4]
    
    # Grid 2x2
    comp_card_width = (content_width - 6*mm) / 2
    comp_card_height = 50 * mm
    gap_x = 6 * mm
    gap_y = 5 * mm
    
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
        
        # Fondo tarjeta
        draw_rounded_rect(c, cx, cy, comp_card_width, comp_card_height, 3*mm, 
                          fill_color=white, stroke_color=HexColor("#e0e0e0"))
        
        # === FOTO (izquierda de la tarjeta) ===
        img_width = 38 * mm
        img_height = comp_card_height - 6 * mm
        img_x = cx + 3 * mm
        img_y = cy + 3 * mm
        
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
                c.drawImage(comp_image, img_x, img_y, width=img_width, height=img_height, 
                           preserveAspectRatio=True, mask='auto')
            except:
                comp_image = None
        
        if not comp_image:
            c.setFillColor(HexColor("#f0f0f0"))
            c.rect(img_x, img_y, img_width, img_height, fill=1, stroke=0)
            c.setFillColor(HexColor("#cccccc"))
            c.setFont("Helvetica", 20)
            c.drawCentredString(img_x + img_width/2, img_y + img_height/2 - 4*mm, "🏠")
        
        # === INFO (derecha de la foto) ===
        info_x = img_x + img_width + 4 * mm
        info_width = comp_card_width - img_width - 10 * mm
        
        # Precio
        text_y = cy + comp_card_height - 10 * mm
        c.setFillColor(AURA_DARK)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(info_x, text_y, format_price(comp.get("price", 0)))
        
        # €/m²
        text_y -= 5 * mm
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 8)
        c.drawString(info_x, text_y, format_price_m2(comp.get("price", 0), comp.get("size", 0)))
        
        # Características
        text_y -= 7 * mm
        c.setFont("Helvetica", 8)
        rooms = comp.get('rooms', '-')
        baths = comp.get('bathrooms', '-')
        size = comp.get('size', '-')
        c.drawString(info_x, text_y, f"{rooms} hab. · {baths} baños")
        
        text_y -= 5 * mm
        c.drawString(info_x, text_y, f"{size} m²")
        
        # === LOGO PORTAL (esquina inferior derecha) ===
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
        
        # "Ver propiedad →"
        c.setFillColor(HexColor("#3b82f6"))
        c.setFont("Helvetica", 7)
        c.drawString(info_x, cy + 5*mm, "Ver propiedad →")
        
        url = comp.get("url", "")
        if url:
            c.linkURL(url, (info_x, cy + 2*mm, info_x + 30*mm, cy + 10*mm))
    
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
    """Genera nombre de archivo para el PDF"""
    clean = "".join(c if c.isalnum() or c in " -_" else "_" for c in address)
    clean = clean.replace(" ", "_")[:40]
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"AURA_Valoracion_{clean}_{timestamp}.pdf"
