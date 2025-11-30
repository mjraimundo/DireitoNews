from pathlib import Path
import sys
import json

# adicionar raiz do projeto para importar scraper
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))

from scraper import get_rss_feeds, scrape_rss

feeds = get_rss_feeds()

result = []
for rss in feeds:
    jornal, noticias = scrape_rss(rss)
    items = []
    for n in noticias[:10]:
        pub = n.get('published')
        pub_s = pub.isoformat() if hasattr(pub, 'isoformat') else str(pub)
        items.append({
            'title': n.get('title'),
            'link': n.get('link'),
            'published': pub_s,
        })
    result.append({'rss': rss, 'sample_count': len(noticias), 'sample': items})

print(json.dumps(result, ensure_ascii=False, indent=2))
