"""
dossier.py v7
Alineación perfecta al thumbnail, nombre de archivo corregido.
"""

import io
import os
import re
import math
import httpx
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white, black
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

AURA_YELLOW = HexColor("#F5B800")
AURA_YELLOW_LIGHT = HexColor("#fef9e7")
AURA_DARK = HexColor("#1a1a1a")
AURA_GRAY = HexColor("#6b7280")
AURA_LIGHT_GRAY = HexColor("#f5f5f5")

PORTAL_LOGOS = {
    "idealista": "https://cdn.aura-app.es/img/portal_idealista.png",
    "fotocasa": "https://cdn.aura-app.es/img/portal_fotocasa.png",
    "habitaclia": "https://cdn.aura-app.es/img/portal_habitaclia.png",
}

def format_price(price):
    if not price:
        return "-"
    return f"{int(price):,}".replace(",", ".") + " €"

def format_price_m2(price, area):
    if not price or not area:
        return "-"
    return f"{int(price / area):,}".replace(",", ".") + " €/m²"

async def fetch_image(url: str, timeout: int = 10) -> bytes:
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
    if not lat or not lng or lat == 0 or lng == 0:
        return None
    
    mapbox_token = os.getenv("MAPBOX_TOKEN", "")
    if mapbox_token and len(mapbox_token) > 50:
        try:
            url = f"https://api.mapbox.com/styles/v1/mapbox/streets-v12/static/pin-l+F5B800({lng},{lat})/{lng},{lat},15,0/{width}x{height}@2x?access_token={mapbox_token}"
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                response = await client.get(url)
                if response.status_code == 200 and len(response.content) > 5000:
                    return response.content
        except Exception as e:
            print(f"[dossier] Mapbox error: {e}")
    
    try:
        zoom = 15
        n = 2 ** zoom
        x_tile = int((lng + 180) / 360 * n)
        lat_rad = math.radians(lat)
        y_tile = int((1 - math.asinh(math.tan(lat_rad)) / math.pi) / 2 * n)
        url = f"https://a.basemaps.cartocdn.com/rastertiles/voyager/{zoom}/{x_tile}/{y_tile}.png"
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code == 200:
                return response.content
    except:
        pass
    
    return None

async def generate_dossier_pdf(data: dict) -> bytes:
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    
    margin = 15 * mm
    content_width = width - (2 * margin)
    
    prop = data.get("property", {})
    coords = data.get("coordinates", {})
    valuation = data.get("valuation", {})
    comparables = data.get("comparables", [])
    
    print(f"[dossier] === GENERATING PDF v7 ===")
    print(f"[dossier] Address: {prop.get('address')}")
    
    selected_comparables = [comp for comp in comparables if comp.get("selected") or comp.get("in_range")]
    if not selected_comparables:
        selected_comparables = comparables[:10]
    
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
    
    # ========== MAPA + VALORACIÓN ==========
    map_width = 88 * mm
    map_height = 55 * mm
    map_x = margin
    map_y = y - map_height
    
    map_image = None
    lat = coords.get("lat", 0)
    lng = coords.get("lng", 0)
    
    if lat and lng:
        map_bytes = await fetch_map_image(lat, lng)
        if map_bytes:
            try:
                map_image = ImageReader(io.BytesIO(map_bytes))
            except:
                pass
    
    if map_image:
        try:
            c.drawImage(map_image, map_x, map_y, width=map_width, height=map_height, preserveAspectRatio=True, mask='auto')
        except:
            map_image = None
    
    if not map_image:
        c.setFillColor(AURA_LIGHT_GRAY)
        c.rect(map_x, map_y, map_width, map_height, fill=1, stroke=0)
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 9)
        c.drawCentredString(map_x + map_width/2, map_y + map_height/2, "Mapa no disponible")
    
    c.setStrokeColor(HexColor("#e0e0e0"))
    c.setLineWidth(0.5)
    c.rect(map_x, map_y, map_width, map_height, fill=0, stroke=1)
    
    # === VALORACIÓN ===
    val_x = map_x + map_width + 8 * mm
    val_width = content_width - map_width - 8 * mm
    
    price_y = y - 12 * mm
    c.setFillColor(black)
    c.setFont("Helvetica-Bold", 26)
    c.drawCentredString(val_x + val_width/2, price_y, format_price(main_price))
    
    price_m2_y = price_y - 8 * mm
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 11)
    c.drawCentredString(val_x + val_width/2, price_m2_y, format_price_m2(main_price, total_area))
    
    cards_top = price_m2_y - 10 * mm
    card_width = (val_width - 4 * mm) / 2
    card_height = 24 * mm
    card_y = cards_top - card_height
    
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
    
    # ========== CARACTERÍSTICAS ==========
    chars_y = map_y - 6 * mm
    chars_height = 16 * mm
    chars_width = map_width
    
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
    
    comp_card_width = (content_width - 6*mm) / 2
    comp_card_height = 46 * mm
    gap_x = 6 * mm
    gap_y = 4 * mm
    
    # Thumbnail ocupa casi toda la altura de la tarjeta
    thumb_padding = 3 * mm
    thumb_width = 40 * mm
    thumb_height = comp_card_height - (thumb_padding * 2)  # ~40mm
    
    portal_images = {}
    for portal, url in PORTAL_LOGOS.items():
        img_bytes = await fetch_image(url)
        if img_bytes:
            try:
                portal_images[portal] = ImageReader(io.BytesIO(img_bytes))
            except:
                pass
    
    for i, comp in enumerate(top_comparables):
        col = i % 2
        row = i // 2
        
        cx = margin + col * (comp_card_width + gap_x)
        cy = comps_y - (row + 1) * comp_card_height - row * gap_y
        
        c.setFillColor(white)
        c.rect(cx, cy, comp_card_width, comp_card_height, fill=1, stroke=0)
        c.setStrokeColor(HexColor("#e0e0e0"))
        c.setLineWidth(0.5)
        c.rect(cx, cy, comp_card_width, comp_card_height, fill=0, stroke=1)
        
        # THUMBNAIL - posicionado con padding desde los bordes de la tarjeta
        thumb_x = cx + thumb_padding
        thumb_y = cy + thumb_padding
        thumb_top_y = thumb_y + thumb_height  # Borde superior del thumbnail
        thumb_bottom_y = thumb_y              # Borde inferior del thumbnail
        
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
                c.drawImage(comp_image, thumb_x, thumb_y, width=thumb_width, height=thumb_height, 
                           preserveAspectRatio=True, mask='auto')
            except:
                comp_image = None
        
        if not comp_image:
            c.setFillColor(HexColor("#f0f0f0"))
            c.rect(thumb_x, thumb_y, thumb_width, thumb_height, fill=1, stroke=0)
            c.setFillColor(HexColor("#cccccc"))
            c.setFont("Helvetica", 20)
            c.drawCentredString(thumb_x + thumb_width/2, thumb_y + thumb_height/2 - 3*mm, "🏠")
        
        # INFO - alineada EXACTAMENTE con los bordes del thumbnail
        info_x = thumb_x + thumb_width + 4 * mm
        
        # El texto debe ir desde thumb_top_y hasta thumb_bottom_y
        # Distribución: Precio arriba, Ver propiedad abajo, resto en medio
        
        # PRECIO - exactamente en el borde superior del thumbnail
        c.setFillColor(AURA_DARK)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(info_x, thumb_top_y - 3*mm, format_price(comp.get("price", 0)))
        
        # €/m² - justo debajo del precio
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 7)
        c.drawString(info_x, thumb_top_y - 9*mm, format_price_m2(comp.get("price", 0), comp.get("size", 0)))
        
        # Habitaciones y baños - en el medio
        c.drawString(info_x, thumb_top_y - 17*mm, f"{comp.get('rooms', '-')} hab. · {comp.get('bathrooms', '-')} baños")
        
        # Metros - debajo de hab/baños
        c.drawString(info_x, thumb_top_y - 23*mm, f"{comp.get('size', '-')} m²")
        
        # VER PROPIEDAD - exactamente en el borde inferior del thumbnail
        c.setFillColor(HexColor("#3b82f6"))
        c.setFont("Helvetica", 7)
        c.drawString(info_x, thumb_bottom_y, "Ver propiedad →")
        
        url = comp.get("url", "")
        if url:
            c.linkURL(url, (info_x, thumb_bottom_y - 2*mm, info_x + 28*mm, thumb_bottom_y + 5*mm))
        
        # LOGO - en el borde inferior derecho, alineado con "Ver propiedad"
        source = comp.get("source", "idealista").lower()
        if source in portal_images:
            logo_width = 14 * mm
            logo_height = 5 * mm
            logo_x = cx + comp_card_width - logo_width - thumb_padding
            logo_y = thumb_bottom_y
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
    
    c.setFillColor(HexColor("#666666"))
    c.setFont("Helvetica", 6)
    today = datetime.now().strftime("%d/%m/%Y")
    c.drawString(margin, footer_height + 2*mm, f"Generado: {today}")
    c.drawRightString(width - margin, footer_height + 2*mm, f"{len(selected_comparables)} comparables analizados")
    
    c.save()
    print(f"[dossier] ✅ PDF v7 generated")
    
    return buffer.getvalue()


def generate_dossier_filename(address: str) -> str:
    """AAMMDD_HHMM_[calle]_[número]_valoracion_by_AURA.pdf"""
    now = datetime.now()
    date_str = now.strftime("%y%m%d_%H%M")
    
    clean = address
    for prefix in ["Calle ", "C/ ", "C. ", "Avenida ", "Av. ", "Av ", "Plaza ", "Pz. ", "Paseo ", "Pº "]:
        if clean.startswith(prefix):
            clean = clean[len(prefix):]
            break
    
    for old, new in {'á':'a', 'é':'e', 'í':'i', 'ó':'o', 'ú':'u', 'Á':'A', 'É':'E', 'Í':'I', 'Ó':'O', 'Ú':'U', 'ñ':'n', 'Ñ':'N', 'ü':'u', 'Ü':'U'}.items():
        clean = clean.replace(old, new)
    
    clean = re.sub(r'[^a-zA-Z0-9\s,]', '', clean)
    
    parts = clean.split(',')
    if len(parts) >= 2:
        street = parts[0].strip().replace(' ', '_')[:30]
        number = parts[1].strip().replace(' ', '')[:10]
        location = f"{street}_{number}"
    else:
        location = clean.replace(' ', '_')[:35]
    
    return f"{date_str}_{location}_valoracion_by_AURA.pdf"
