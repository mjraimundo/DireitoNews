import feedparser
import requests
import time
from urllib.parse import urljoin, urlparse
import re
from datetime import datetime, timedelta, timezone
from date_utils import parse_rss_date_to_dt
from bs4 import BeautifulSoup
from image_utils import create_and_upload_thumbnail

# Supabase + imagem helpers
import os
from io import BytesIO
from PIL import Image
import uuid
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BUCKET = os.getenv("SUPABASE_BUCKET")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def upload_image_to_supabase(img_url: str) -> str:
    """Baixa imagem, converte para JPEG e envia para Supabase Storage.

    Retorna a URL pública da imagem ou None em caso de erro.
    """
    try:
        # Pular SVGs (não vamos rasterizar SVGs aqui)
        if not img_url:
            return None
        if img_url.lower().endswith('.svg'):
            return None

        # Preparar headers básicos (User-Agent + Referer com origem da imagem)
        parsed = urlparse(img_url)
        origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else None
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        }
        if origin:
            headers["Referer"] = origin

        # Tentativas com backoff (reduz chance de bloqueio temporário)
        r = None
        for attempt in range(3):
            try:
                r = requests.get(img_url, headers=headers, timeout=10)
                # Se 403, pode ser bloqueio por hotlink; não retryar indefinidamente
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

        # Se o servidor retornou SVG via content-type, pular
        ctype = r.headers.get('Content-Type', '')
        if 'svg' in ctype:
            return None

        img = Image.open(BytesIO(r.content))

        # Tratar imagens paletizadas e com alpha (transparência)
        if img.mode == "P":
            # paleta pode ter 'transparency' em info
            if "transparency" in img.info:
                img = img.convert("RGBA")
            else:
                img = img.convert("RGB")
        elif img.mode in ("RGBA", "LA"):
            img = img.convert("RGBA")
        else:
            img = img.convert("RGB")

        # Se houver canal alpha, mesclar sobre fundo branco antes de salvar JPEG
        if img.mode == "RGBA":
            background = Image.new("RGB", img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3])  # 3 = canal alpha
            image = background
        else:
            image = img

        buf = BytesIO()
        image.save(buf, format="JPEG", quality=80)
        buf.seek(0)

        image_name = f"images/{uuid.uuid4()}.jpg"

        supabase.storage.from_(BUCKET).upload(image_name, buf.read())

        public_url = supabase.storage.from_(BUCKET).get_public_url(image_name)

        # Normalizar retorno: aceitar dict ou string
        if isinstance(public_url, dict):
            for k in ("publicUrl", "public_url", "publicurl", "url"):
                if k in public_url:
                    return public_url[k]
            return str(public_url)

        return public_url
    except Exception:
        return None


def test_uploads(sample_url: str):
    """Testa upload de imagem e miniatura; imprime as URLs retornadas.

    Uso (no terminal do projeto):
        python -c "from scraper import test_uploads; test_uploads('https://example.com/image.jpg')"
    """
    print("Testing upload_image_to_supabase...")
    print(upload_image_to_supabase(sample_url))
    print("Testing create_and_upload_thumbnail...")
    print(create_and_upload_thumbnail(sample_url, supabase, BUCKET))

# ============================================================
# 1. EXTRATORES DE IMAGEM
# ============================================================


def extract_image_by_extension(description_html: str):
    if not description_html:
        return None

    # Busca URLs terminando em extensões comuns
    pattern = r'(https?://[^\s\'\"<>]+\.(?:jpg|jpeg|png|gif|webp|bmp|svg))'
    match = re.search(pattern, description_html, re.IGNORECASE)
    return match.group(1) if match else None


def extract_image_from_page(url: str):
    """Busca imagem diretamente no HTML da notícia."""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # OG:image
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            return og["content"]

        # Twitter:image
        tw = soup.find("meta", attrs={"name": "twitter:image"})
        if tw and tw.get("content"):
            return tw["content"]

        # image_src
        link_img = soup.find("link", rel="image_src")
        if link_img and link_img.get("href"):
            return link_img["href"]

        # Primeira img como fallback (resolver URLs relativas)
        img = soup.find("img")
        if img and img.get("src"):
            src = img.get("src")
            # resolver relativo
            return urljoin(url, src)

    except Exception:
        return None

    return None


def extract_image(entry):
    """Fusão de todos os métodos para achar a imagem principal."""
    # 1) media:content
    if "media_content" in entry and entry.media_content:
        url = entry.media_content[0].get("url")
        if url: return url

    # 2) enclosures
    if "enclosures" in entry and entry.enclosures:
        url = entry.enclosures[0].get("href")
        if url: return url

    # 3) media_thumbnail
    if "media_thumbnail" in entry:
        url = entry.media_thumbnail[0].get("url")
        if url: return url

    # 4) Description regex
    desc = entry.get("summary") or entry.get("description")
    url = extract_image_by_extension(desc)
    if url: return url

    # 5) Scraping da página (entry.link)
    url = extract_image_from_page(entry.get("link"))
    if url: return url

    return None


# ============================================================
# 2. SCRAPER PRINCIPAL
# ============================================================


def scrape_rss(rss_url: str):
    # Ler RSS (sem prints excessivos aqui)

    feed = feedparser.parse(rss_url)

    # -------- RSS HEADER (canal / jornal) --------

    channel_name = feed.feed.get("title", "Sem Nome")
    channel_icon = None

    # tenta extrair o ícone do feed (favicon ou og:image do canal)
    if "image" in feed.feed and feed.feed.image:
        channel_icon = feed.feed.image.get("href")

    # salva jornal
    jornal = {
        "rss_url": rss_url,
        "name": channel_name,
        "icon_url": channel_icon
    }

    # -------- LISTA DE NOTÍCIAS --------

    noticias = []

    for entry in feed.entries:
        try:
            # Data de publicação
            published_raw = entry.get("published") or entry.get("updated")
            published_date = parse_rss_date_to_dt(published_raw)

            if published_date:
                # Normalizar para UTC (se já estiver em outro fuso, converte; se UTC, mantém)
                pub_date = published_date.astimezone(timezone.utc)
            else:
                continue

            # Imagem principal
            imagem = extract_image(entry)

            noticia = {
                "rss_url": rss_url,
                "title": entry.get("title"),
                "link": entry.get("link"),
                "published": pub_date,
                "image_url": imagem
            }

            noticias.append(noticia)

        except Exception as e:
            print("Erro ao processar item:", e)

    return jornal, noticias


# ============================================================
# 3. FUNCTIONS TO FETCH RSS FEEDS FROM EXTERNAL SOURCES
# ============================================================


def get_rss_feeds_from_supabase():
    """Fetch all active RSS feeds from the 'news' table in Supabase."""
    try:
        response = supabase.table("news").select("rss_url").execute()
        if isinstance(response, dict):
            data = response.get("data", [])
        else:
            data = getattr(response, "data", [])

        if data:
            return [row["rss_url"] for row in data if row.get("rss_url")]
        return []
    except Exception as e:
        print(f"⚠ Erro ao buscar RSS feeds do Supabase: {e}")
        return []


def get_rss_feeds_from_config(config_path: str = "config.json"):
    """Fetch RSS feeds from a local config.json file."""
    try:
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
                return config.get("rss_list", [])
    except Exception as e:
        print(f"⚠ Erro ao ler config.json: {e}")
    return []


def get_rss_feeds():
    """
    Get RSS feeds in priority order:
    1. From Supabase 'news' table (preferred)
    2. From config.json (fallback)

    Returns list of RSS URLs.
    """
    feeds = get_rss_feeds_from_supabase()
    if feeds:
        return feeds

    # Fallback to config.json
    feeds = get_rss_feeds_from_config()
    if feeds:
        print("ℹ Usando feeds do config.json (Supabase vazio ou indisponível)")
        return feeds

    # If both sources are empty, use minimal default
    print("⚠ Nenhum feed encontrado no Supabase ou config.json; usando lista padrão")
    return ["https://revistadireitohoje.com.br/feed/"]


if __name__ == "__main__":
    import sys
    import os
    import json

    # Fetch RSS feeds from Supabase or config.json
    RSS_LIST = get_rss_feeds()

    total_inserted = 0
    total_dup_skipped = 0
    total_failures = 0

    # Process each feed independently so we can use/maintain news.last_article per feed
    for rss in RSS_LIST:
        jornal, noticias = scrape_rss(rss)

        # Ensure news row exists and fetch current last_article
        news_id = None
        last_article_str = None
        try:
            resp = supabase.table("news").select("id,last_article").eq("rss_url", rss).limit(1).execute()
            if isinstance(resp, dict):
                rows = resp.get("data") or []
            else:
                rows = getattr(resp, "data", []) or []

            if rows:
                news_id = rows[0].get("id")
                last_article_str = rows[0].get("last_article")
            else:
                ins = supabase.table("news").insert({
                    "rss_url": rss,
                    "name": jornal.get("name"),
                    "icon_url": jornal.get("icon_url"),
                }).execute()
                if isinstance(ins, dict):
                    ins_rows = ins.get("data") or []
                else:
                    ins_rows = getattr(ins, "data", []) or []
                if ins_rows:
                    news_id = ins_rows[0].get("id")

        except Exception as e:
            print(f"Erro ao obter/insere jornal {rss}: {e}")

        # Parse stored last_article into datetime (UTC-aware)
        last_article_dt = None
        if last_article_str:
            try:
                s = last_article_str
                if isinstance(s, str) and s.endswith("Z"):
                    s = s.replace("Z", "+00:00")
                last_article_dt = datetime.fromisoformat(s).astimezone(timezone.utc)
            except Exception:
                last_article_dt = None

        # Determine max published date among scraped entries and filter those newer than last_article
        max_published = None
        to_insert = []
        for n in noticias:
            pub = n.get("published")
            if not isinstance(pub, datetime):
                continue
            if max_published is None or pub > max_published:
                max_published = pub
            if last_article_dt is None or pub > last_article_dt:
                to_insert.append(n)

        # Insert filtered articles
        inserted = 0
        dup_skipped = 0
        failures = 0

        for noticia in to_insert:
            if not news_id:
                failures += 1
                continue

            image_url = noticia.get("image_url")
            thumbnail_url = None
            if image_url:
                thumbnail_url = create_and_upload_thumbnail(
                    image_url,
                    supabase,
                    BUCKET,
                    max_width=300,
                    quality=70,
                )
                if thumbnail_url is None:
                    failures += 1
                    # still try to insert the article without thumbnail

            published = noticia.get("published")
            published_str = published.isoformat() if published else None

            try:
                link_val = noticia.get("link")
                if link_val:
                    exists = supabase.table("article").select("id").eq("link", link_val).limit(1).execute()
                    has = False
                    if isinstance(exists, dict):
                        has = bool(exists.get("data"))
                    else:
                        has = bool(getattr(exists, "data", None))
                    if has:
                        dup_skipped += 1
                        continue

                article_insert = supabase.table("article").insert({
                    "news_id": news_id,
                    "rss_url": rss,
                    "title": noticia.get("title"),
                    "link": link_val,
                    "thumbnail_url": thumbnail_url,
                    "published": published_str,
                }).execute()

                ok = False
                if isinstance(article_insert, dict):
                    ok = bool(article_insert.get("data"))
                else:
                    ok = bool(getattr(article_insert, "data", None))

                if ok:
                    inserted += 1
                else:
                    failures += 1

            except Exception:
                failures += 1
                continue

        # Update news.last_article if we have a newer max_published
        if max_published:
            try:
                if last_article_dt is None or max_published > last_article_dt:
                    supabase.table("news").update({"last_article": max_published.isoformat()}).eq("rss_url", rss).execute()
            except Exception as e:
                print(f"Erro ao atualizar last_article para {rss}: {e}")

        # Print per-feed summary and accumulate totals
        print(f"{rss}: {inserted} notícias inseridas")
        if dup_skipped:
            print(f"Duplicatas puladas neste feed: {dup_skipped}")

        total_inserted += inserted
        total_dup_skipped += dup_skipped
        total_failures += failures

    # Global brief summary
    print(f"Total inserido: {total_inserted}")
    if total_dup_skipped:
        print(f"Total duplicatas puladas: {total_dup_skipped}")
    if total_failures:
        print(f"Total falhas: {total_failures}")
