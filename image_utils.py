from io import BytesIO
from PIL import Image
import requests
import time
from urllib.parse import urlparse
import uuid

def create_and_upload_thumbnail(img_url: str, supabase_client, bucket_name: str, max_width: int = 300, quality: int = 70) -> str:
    """
    Baixa imagem, cria miniatura, faz upload para Supabase e retorna URL pública.
    
    Args:
        img_url: URL da imagem original
        supabase_client: cliente Supabase já inicializado
        bucket_name: nome do bucket (ex: "noticias-storage")
        max_width: largura máxima da miniatura em pixels (altura é proporcional)
        quality: qualidade JPEG (0-100, padrão 70)
    
    Returns:
        URL pública da miniatura ou None se falhar
    """
    try:
        # 1. Baixar imagem original com headers e retry
        if not img_url:
            return None

        if img_url.lower().endswith('.svg'):
            return None

        parsed = urlparse(img_url)
        origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else None
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        }
        if origin:
            headers["Referer"] = origin

        r = None
        for attempt in range(3):
            try:
                r = requests.get(img_url, headers=headers, timeout=10)
                if r.status_code == 403:
                    break
                r.raise_for_status()
                break
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(1 * (2 ** attempt))
                    continue
                raise

        if r is None:
            return None

        # pular svg pelo content-type
        ctype = r.headers.get('Content-Type', '')
        if 'svg' in ctype:
            return None
        
        # 2. Abrir e processar com PIL
        img = Image.open(BytesIO(r.content))

        # Tratar paleta e transparência
        if img.mode == "P":
            if "transparency" in img.info:
                img = img.convert("RGBA")
            else:
                img = img.convert("RGB")
        elif img.mode in ("RGBA", "LA"):
            img = img.convert("RGBA")
        else:
            img = img.convert("RGB")

        # 3. Redimensionar (mantendo proporção)
        img.thumbnail((max_width, max_width), Image.Resampling.LANCZOS)

        # 4. Se tiver alpha, mesclar sobre fundo branco antes de salvar JPEG
        if img.mode == "RGBA":
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3])
            image = background
        else:
            image = img

        # 5. Salvar em memória como JPEG comprimido
        thumb_buf = BytesIO()
        image.save(thumb_buf, format="JPEG", quality=quality)
        thumb_buf.seek(0)
        
        # 5. Upload para Supabase Storage (pasta 'thumbnails')
        thumb_name = f"thumbnails/{uuid.uuid4()}.jpg"
        supabase_client.storage.from_(bucket_name).upload(thumb_name, thumb_buf.read())

        # 6. Obter URL pública
        public_url = supabase_client.storage.from_(bucket_name).get_public_url(thumb_name)

        # Normalizar retorno: aceitar dict ou string
        if isinstance(public_url, dict):
            for k in ("publicUrl", "public_url", "publicurl", "url"):
                if k in public_url:
                    return public_url[k]
            return str(public_url)

        return public_url
    
    except Exception as e:
        return None