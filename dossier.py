"""
dossier.py
Genera el PDF del dossier de valoración de propiedad.

Flujo:
  1. Recibe datos de valoración (propiedad, comparables, precios)
  2. Descarga mapa estático de OpenStreetMap/Mapbox
  3. Genera PDF con reportlab
  4. Devuelve bytes del PDF
"""

import io
import os
import math
import httpx
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white, black
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Colores AURA
AURA_YELLOW = HexColor("#F5B800")
AURA_DARK = HexColor("#1a1a1a")
AURA_GRAY = HexColor("#6b7280")
AURA_LIGHT_GRAY = HexColor("#f5f5f5")

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

async def fetch_map_image(lat, lng, zoom=16, width=400, height=200):
    """Descarga imagen de mapa estático de OpenStreetMap via Mapbox o alternativa"""
    
    # Intentar con un tile server gratuito
    # Usamos el estilo de OpenStreetMap directamente
    try:
        # Opción 1: Mapbox Static API (requiere token, pero tiene uno público de demo)
        mapbox_token = os.getenv("MAPBOX_TOKEN", "")
        url = f"https://api.mapbox.com/styles/v1/mapbox/streets-v12/static/pin-l+F5B800({lng},{lat})/{lng},{lat},{zoom},0/{width}x{height}@2x?access_token={mapbox_token}"
        
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(url)
            if response.status_code == 200:
                return response.content
    except Exception as e:
        print(f"[dossier] Error fetching Mapbox map: {e}")
    
    # Opción 2: OpenStreetMap Static Map (stadiamaps)
    try:
        url = f"https://tiles.stadiamaps.com/static/osm_bright?center={lat},{lng}&zoom={zoom}&size={width}x{height}&markers={lat},{lng},red"
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(url)
            if response.status_code == 200:
                return response.content
    except Exception as e:
        print(f"[dossier] Error fetching Stadia map: {e}")
    
    return None

def draw_rounded_rect(c, x, y, width, height, radius, fill_color=None, stroke_color=None):
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
        c.drawPath(p, fill=0, stroke=1)

async def generate_dossier_pdf(data: dict) -> bytes:
    """
    Genera el PDF del dossier de valoración.
    
    Args:
        data: {
            "property": {
                "address": str,
                "cadastre_ref": str,
                "postal_code": str,
                "total_area": float,
                "rooms": int,
                "bathrooms": int,
                "floor": str,
                "building_year": int,
                "state": str,
                "property_type": str
            },
            "coordinates": {"lat": float, "lng": float},
            "valuation": {
                "mean": float,
                "min": float,
                "max": float,
                "std": float,
                "count": int
            },
            "comparables": [
                {
                    "id": str,
                    "title": str,
                    "address": str,
                    "price": float,
                    "priceByArea": float,
                    "size": float,
                    "rooms": int,
                    "bathrooms": int,
                    "distance": float,
                    "source": str,
                    "url": str,
                    "thumbnail": str,
                    "selected": bool
                }
            ],
            "price_min_avg": float,  # Media de 3 más baratos
            "price_max_avg": float,  # Media de 3 más caros
        }
    
    Returns:
        bytes: Contenido del PDF
    """
    
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4  # 210mm x 297mm
    
    # Márgenes
    margin = 15 * mm
    content_width = width - (2 * margin)
    
    # Datos
    prop = data.get("property", {})
    coords = data.get("coordinates", {})
    valuation = data.get("valuation", {})
    comparables = data.get("comparables", [])
    selected_comparables = [c for c in comparables if c.get("selected", False)]
    
    # Si no hay seleccionados, usar los que están in_range
    if not selected_comparables:
        selected_comparables = [c for c in comparables if c.get("in_range", False)]
    
    # Calcular precios min/max (media de 3 más baratos/caros)
    sorted_by_price = sorted([c for c in selected_comparables if c.get("price", 0) > 0], key=lambda x: x["price"])
    lowest_3 = sorted_by_price[:3]
    highest_3 = sorted_by_price[-3:] if len(sorted_by_price) >= 3 else sorted_by_price
    
    price_min_avg = data.get("price_min_avg") or (sum(c["price"] for c in lowest_3) / len(lowest_3) if lowest_3 else 0)
    price_max_avg = data.get("price_max_avg") or (sum(c["price"] for c in highest_3) / len(highest_3) if highest_3 else 0)
    
    total_area = prop.get("total_area", 0)
    main_price = valuation.get("mean", 0)
    
    # ========== HEADER ==========
    header_height = 20 * mm
    c.setFillColor(AURA_DARK)
    c.rect(0, height - header_height, width, header_height, fill=1, stroke=0)
    
    # Logo AURA
    c.setFillColor(AURA_YELLOW)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin, height - 14 * mm, "AURA Valuations")
    
    # Subtítulo
    c.setFillColor(HexColor("#999999"))
    c.setFont("Helvetica", 9)
    c.drawRightString(width - margin, height - 14 * mm, "Informe de Valoración")
    
    # ========== TÍTULO (Dirección) ==========
    y = height - header_height - 12 * mm
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 14)
    address = prop.get("address", "Dirección no disponible")
    # Truncar si es muy largo
    if len(address) > 60:
        address = address[:57] + "..."
    c.drawString(margin, y, address)
    
    y -= 8 * mm
    
    # ========== LAYOUT PRINCIPAL ==========
    # Izquierda: Mapa (85mm ancho)
    # Derecha: Precio y tarjetas
    
    map_width = 85 * mm
    map_height = 50 * mm
    map_x = margin
    map_y = y - map_height
    
    # Intentar cargar imagen del mapa
    map_image = None
    if coords.get("lat") and coords.get("lng"):
        try:
            map_bytes = await fetch_map_image(coords["lat"], coords["lng"])
            if map_bytes:
                map_image = ImageReader(io.BytesIO(map_bytes))
        except Exception as e:
            print(f"[dossier] Error loading map: {e}")
    
    if map_image:
        c.drawImage(map_image, map_x, map_y, width=map_width, height=map_height, preserveAspectRatio=True, mask='auto')
    else:
        # Placeholder del mapa
        c.setFillColor(AURA_LIGHT_GRAY)
        c.rect(map_x, map_y, map_width, map_height, fill=1, stroke=0)
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 10)
        c.drawCentredString(map_x + map_width/2, map_y + map_height/2, "Mapa no disponible")
    
    # Borde del mapa
    c.setStrokeColor(HexColor("#e5e5e5"))
    c.rect(map_x, map_y, map_width, map_height, fill=0, stroke=1)
    
    # ========== PRECIO (lado derecho del mapa) ==========
    price_x = margin + map_width + 8 * mm
    price_y = y - 5 * mm
    
    # Precio principal
    c.setFillColor(AURA_YELLOW)
    c.setFont("Helvetica-Bold", 24)
    c.drawString(price_x, price_y, format_price(main_price))
    
    # Precio por m²
    price_y -= 8 * mm
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 12)
    c.drawString(price_x, price_y, format_price_m2(main_price, total_area))
    
    # ========== TARJETAS MIN/MAX ==========
    card_y = price_y - 18 * mm
    card_width = 38 * mm
    card_height = 28 * mm
    card_gap = 5 * mm
    
    # Tarjeta Precio Mínimo
    draw_rounded_rect(c, price_x, card_y, card_width, card_height, 3*mm, fill_color=AURA_LIGHT_GRAY)
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 7)
    c.drawCentredString(price_x + card_width/2, card_y + card_height - 7*mm, "Precio Mínimo")
    
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(price_x + card_width/2, card_y + card_height - 14*mm, format_price(price_min_avg))
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 7)
    c.drawCentredString(price_x + card_width/2, card_y + card_height - 20*mm, format_price_m2(price_min_avg, total_area))
    
    # Tarjeta Precio Máximo
    max_card_x = price_x + card_width + card_gap
    draw_rounded_rect(c, max_card_x, card_y, card_width, card_height, 3*mm, fill_color=AURA_LIGHT_GRAY)
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 7)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 7*mm, "Precio Máximo")
    
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 14*mm, format_price(price_max_avg))
    
    c.setFillColor(AURA_GRAY)
    c.setFont("Helvetica", 7)
    c.drawCentredString(max_card_x + card_width/2, card_y + card_height - 20*mm, format_price_m2(price_max_avg, total_area))
    
    # ========== CARACTERÍSTICAS DEL INMUEBLE ==========
    chars_y = map_y - 8 * mm
    chars_height = 18 * mm
    
    draw_rounded_rect(c, margin, chars_y - chars_height, map_width, chars_height, 3*mm, fill_color=AURA_LIGHT_GRAY)
    
    # Superficie, Habitaciones, Baños
    char_items = [
        f"{prop.get('total_area', '-')} m²",
        f"{prop.get('rooms', '-')} hab.",
        f"{prop.get('bathrooms', '-')} baños"
    ]
    
    char_width = map_width / 3
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 10)
    
    for i, item in enumerate(char_items):
        x = margin + (i * char_width) + char_width/2
        c.drawCentredString(x, chars_y - chars_height/2 - 2*mm, item)
    
    # ========== SECCIÓN COMPARABLES ==========
    comps_y = chars_y - chars_height - 12 * mm
    
    c.setFillColor(AURA_DARK)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin, comps_y, "Propiedades Comparables")
    
    comps_y -= 5 * mm
    
    # Mostrar hasta 4 comparables
    top_comparables = selected_comparables[:4]
    if len(top_comparables) < 4:
        top_comparables = comparables[:4]
    
    comp_card_width = (content_width - 15*mm) / 4
    comp_card_height = 65 * mm
    
    for i, comp in enumerate(top_comparables):
        cx = margin + (i * (comp_card_width + 5*mm))
        cy = comps_y - comp_card_height
        
        # Fondo de tarjeta
        c.setFillColor(white)
        c.setStrokeColor(HexColor("#e0e0e0"))
        c.roundRect(cx, cy, comp_card_width, comp_card_height, 3*mm, fill=1, stroke=1)
        
        # Placeholder de imagen
        img_height = 22 * mm
        c.setFillColor(HexColor("#f0f0f0"))
        c.rect(cx + 2*mm, cy + comp_card_height - img_height - 2*mm, comp_card_width - 4*mm, img_height, fill=1, stroke=0)
        c.setFillColor(HexColor("#cccccc"))
        c.setFont("Helvetica", 14)
        c.drawCentredString(cx + comp_card_width/2, cy + comp_card_height - img_height/2 - 2*mm, "🏠")
        
        # Precio
        text_y = cy + comp_card_height - img_height - 10*mm
        c.setFillColor(AURA_DARK)
        c.setFont("Helvetica-Bold", 9)
        price_text = format_price(comp.get("price", 0))
        if len(price_text) > 12:
            c.setFont("Helvetica-Bold", 8)
        c.drawString(cx + 3*mm, text_y, price_text)
        
        # Precio por m²
        text_y -= 5*mm
        c.setFillColor(AURA_GRAY)
        c.setFont("Helvetica", 6)
        c.drawString(cx + 3*mm, text_y, format_price_m2(comp.get("price", 0), comp.get("size", 0)))
        
        # Características
        text_y -= 6*mm
        c.setFont("Helvetica", 6)
        details = f"{comp.get('rooms', '-')} hab. | {comp.get('bathrooms', '-')} baños | {comp.get('size', '-')} m²"
        c.drawString(cx + 3*mm, text_y, details)
        
        # Indicador de fuente (círculo de color)
        source_colors = {
            "idealista": HexColor("#22c55e"),  # Verde
            "fotocasa": HexColor("#3b82f6"),   # Azul
            "habitaclia": HexColor("#f97316"), # Naranja
        }
        source = comp.get("source", "idealista")
        source_color = source_colors.get(source, AURA_GRAY)
        
        c.setFillColor(source_color)
        c.circle(cx + comp_card_width - 5*mm, cy + 5*mm, 2*mm, fill=1, stroke=0)
        
        # Link "Ver →"
        c.setFillColor(HexColor("#3b82f6"))
        c.setFont("Helvetica", 6)
        link_text = "Ver propiedad →"
        c.drawString(cx + 3*mm, cy + 3*mm, link_text)
        
        # Añadir link clickeable
        url = comp.get("url", "")
        if url:
            c.linkURL(url, (cx + 3*mm, cy + 1*mm, cx + 30*mm, cy + 6*mm))
    
    # ========== FOOTER ==========
    footer_height = 18 * mm
    c.setFillColor(AURA_DARK)
    c.rect(0, 0, width, footer_height, fill=1, stroke=0)
    
    # Powered by AURA
    c.setFillColor(AURA_YELLOW)
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(width/2, 10*mm, "Powered by AURA")
    
    c.setFillColor(HexColor("#999999"))
    c.setFont("Helvetica", 7)
    c.drawCentredString(width/2, 5*mm, "Herramientas inteligentes para agencias inmobiliarias")
    
    # Fecha de generación
    c.setFillColor(HexColor("#666666"))
    c.setFont("Helvetica", 6)
    today = datetime.now().strftime("%d/%m/%Y")
    c.drawString(margin, footer_height + 3*mm, f"Generado: {today}")
    
    # Número de comparables
    c.drawRightString(width - margin, footer_height + 3*mm, f"{len(selected_comparables)} comparables analizados")
    
    # Guardar
    c.save()
    
    return buffer.getvalue()


def generate_dossier_filename(address: str) -> str:
    """Genera nombre de archivo para el PDF"""
    # Limpiar caracteres especiales
    clean = "".join(c if c.isalnum() or c in " -_" else "_" for c in address)
    clean = clean.replace(" ", "_")[:40]
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"AURA_Valoracion_{clean}_{timestamp}.pdf"
