import sys
from pathlib import Path
import json
import argparse

# Add project root to path
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from scraper import supabase, scrape_rss, create_and_upload_thumbnail
from datetime import datetime, timezone


def reprocess_feed(rss_url: str, count: int = 10, bucket: str = None):
    jornal, noticias = scrape_rss(rss_url)
    noticias_sorted = sorted([
        n for n in noticias if isinstance(n.get('published'), datetime)
    ], key=lambda x: x['published'], reverse=True)

    to_process = noticias_sorted[:count]

    # Ensure news row exists
    news_id = None
    last_article_str = None
    try:
        resp = supabase.table('news').select('id,last_article').eq('rss_url', rss_url).limit(1).execute()
        if isinstance(resp, dict):
            rows = resp.get('data') or []
        else:
            rows = getattr(resp, 'data', []) or []
        if rows:
            news_id = rows[0].get('id')
            last_article_str = rows[0].get('last_article')
        else:
            ins = supabase.table('news').insert({'rss_url': rss_url, 'name': jornal.get('name'), 'icon_url': jornal.get('icon_url')}).execute()
            if isinstance(ins, dict):
                ins_rows = ins.get('data') or []
            else:
                ins_rows = getattr(ins, 'data', []) or []
            if ins_rows:
                news_id = ins_rows[0].get('id')
    except Exception as e:
        print('Erro ao obter/insere jornal:', e)
        return

    inserted = 0
    dup = 0
    failures = 0
    max_published = None

    for n in to_process:
        pub = n.get('published')
        if max_published is None or (isinstance(pub, datetime) and pub > max_published):
            max_published = pub

        link_val = n.get('link')
        try:
            if link_val:
                exists = supabase.table('article').select('id').eq('link', link_val).limit(1).execute()
                has = False
                if isinstance(exists, dict):
                    has = bool(exists.get('data'))
                else:
                    has = bool(getattr(exists, 'data', None))
                if has:
                    dup += 1
                    continue

            thumbnail_url = None
            image_url = n.get('image_url')
            if image_url:
                thumbnail_url = create_and_upload_thumbnail(image_url, supabase, bucket or (root.joinpath('.env').exists() and None))

            published = n.get('published')
            published_str = published.isoformat() if published else None

            ins = supabase.table('article').insert({
                'news_id': news_id,
                'rss_url': rss_url,
                'title': n.get('title'),
                'link': link_val,
                'thumbnail_url': thumbnail_url,
                'published': published_str,
            }).execute()

            ok = False
            if isinstance(ins, dict):
                ok = bool(ins.get('data'))
            else:
                ok = bool(getattr(ins, 'data', None))

            if ok:
                inserted += 1
            else:
                failures += 1

        except Exception as e:
            failures += 1
            print('Erro inserindo artigo:', e)

    # Update last_article if needed
    try:
        if max_published:
            # parse existing
            last_dt = None
            if last_article_str:
                try:
                    s = last_article_str
                    if isinstance(s, str) and s.endswith('Z'):
                        s = s.replace('Z', '+00:00')
                    last_dt = datetime.fromisoformat(s).astimezone(timezone.utc)
                except Exception:
                    last_dt = None
            if last_dt is None or max_published > last_dt:
                supabase.table('news').update({'last_article': max_published.isoformat()}).eq('rss_url', rss_url).execute()
    except Exception as e:
        print('Erro ao atualizar last_article:', e)

    print(json.dumps({'rss': rss_url, 'requested': count, 'processed': len(to_process), 'inserted': inserted, 'duplicates': dup, 'failures': failures, 'max_published': max_published.isoformat() if max_published else None}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('rss')
    p.add_argument('--count', '-n', type=int, default=10)
    args = p.parse_args()
    reprocess_feed(args.rss, args.count)
