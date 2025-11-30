"""Helpers para inspecionar Storage e tabela `article` no Supabase.

Use estes utilitários localmente (com seu `.env` configurado) para listar
arquivos do bucket e consultar registros da tabela `article`.
"""
from __future__ import annotations

import os
from dotenv import load_dotenv
from supabase import create_client
from typing import Any, Dict, List, Optional

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
BUCKET = os.getenv("SUPABASE_BUCKET")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def list_bucket_files(prefix: Optional[str] = None, limit: int = 100) -> Any:
    """Lista arquivos no bucket. Retorna até `limit` itens.

    Observação: a API do SDK de Storage pode variar entre versões; aqui
    chamamos `.list(path)` sem passar kwargs e cortamos o resultado localmente.
    """
    if not BUCKET:
        raise RuntimeError("SUPABASE_BUCKET não configurado no .env")

    path = (prefix or "").rstrip("/")
    try:
        res = supabase.storage.from_(BUCKET).list(path)
        # SDK pode retornar uma lista diretamente ou um dict com 'data'
        items = None
        if isinstance(res, dict) and "data" in res:
            items = res["data"]
        else:
            items = res

        if isinstance(items, list):
            return items[:limit]
        return items
    except Exception as e:
        return {"error": str(e)}


def get_news_samples(table_name: str = "article", limit: int = 10) -> List[Dict[str, Any]]:
    """Retorna alguns registros da tabela especificada.

    Args:
        table_name: nome da tabela no Supabase (ex: 'article' ou 'news').
    """
    try:
        resp = supabase.table(table_name).select("*").limit(limit).execute()
        if isinstance(resp, dict):
            return resp.get("data") or []
        return getattr(resp, "data", []) or []
    except Exception as e:
        return [{"error": str(e)}]


def count_news_with_thumbnails(table_name: str = "article") -> int:
    """Conta registros com `thumbnail_url` não nulo na tabela 'article' (retorna -1 em erro).

    Nota: thumbnails existem apenas na tabela 'article', não em 'news'.
    Para contar jornais/sources, use a tabela 'news' sem este filtro.
    """
    try:
        try:
            resp = supabase.table(table_name).select("id", count="exact").neq("thumbnail_url", None).execute()
        except Exception:
            resp = None

        if resp is not None:
            if isinstance(resp, dict):
                if "count" in resp:
                    return int(resp.get("count", -1))
                data = resp.get("data")
                if isinstance(data, list):
                    return len(data)
            else:
                cnt = getattr(resp, "count", None)
                if cnt is not None:
                    return int(cnt)
                data = getattr(resp, "data", None)
                if isinstance(data, list):
                    return len(data)

        resp2 = supabase.table(table_name).select("id").neq("thumbnail_url", None).execute()
        if isinstance(resp2, dict):
            return len(resp2.get("data") or [])
        return len(getattr(resp2, "data", []) or [])
    except Exception as e:
        print(f"Erro ao contar thumbnails em '{table_name}':", e)
        return -1


def count_news_with_images(table_name: str = "article") -> int:
    """Conta registros com `image_url` não nulo na tabela 'article' (retorna -1 em erro).

    Implementa a mesma estratégia robusta usada em `count_news_with_thumbnails`:
    tenta `count='exact'` e em seguida faz fallback para medir o tamanho de
    `data` quando necessário.
    """
    try:
        try:
            resp = supabase.table(table_name).select("id", count="exact").neq("image_url", None).execute()
        except Exception:
            resp = None

        if resp is not None:
            if isinstance(resp, dict):
                if "count" in resp:
                    return int(resp.get("count", -1))
                data = resp.get("data")
                if isinstance(data, list):
                    return len(data)
            else:
                cnt = getattr(resp, "count", None)
                if cnt is not None:
                    return int(cnt)
                data = getattr(resp, "data", None)
                if isinstance(data, list):
                    return len(data)

        resp2 = supabase.table(table_name).select("id").neq("image_url", None).execute()
        if isinstance(resp2, dict):
            return len(resp2.get("data") or [])
        return len(getattr(resp2, "data", []) or [])
    except Exception as e:
        print(f"Erro ao contar imagens em '{table_name}':", e)
        return -1


def get_table_sample(table_name: str = "article") -> Optional[Dict[str, Any]]:
    """Retorna uma amostra (primeira linha) da tabela ou None em caso de erro."""
    try:
        resp = supabase.table(table_name).select("*").limit(1).execute()
        if isinstance(resp, dict):
            data = resp.get("data") or []
        else:
            data = getattr(resp, "data", []) or []

        return data[0] if data else None
    except Exception:
        return None


if __name__ == "__main__":
    import argparse
    import json

    p = argparse.ArgumentParser()
    p.add_argument("--list", action="store_true", help="List files in bucket (images/ and thumbnails/)")
    p.add_argument("--samples", action="store_true", help="Show sample news records")
    p.add_argument("--count-thumbs", action="store_true", help="Count news with thumbnail_url")
    p.add_argument("--compare-imagens-thumbs", action="store_true", help="Compare registros com image_url e thumbnail_url preenchidos")
    p.add_argument("--auto-compare", action="store_true", help="Auto-detect table among common names and compare image/thumbnail counts")
    p.add_argument("--table", default="article", help="Table name to query for samples/counts (default: article)")
    args = p.parse_args()

    if args.list:
        print("Listing images/ (first 100):")
        print(json.dumps(list_bucket_files("images/", limit=100), indent=2, ensure_ascii=False))
        print("\nListing thumbnails/ (first 100):")
        print(json.dumps(list_bucket_files("thumbnails/", limit=100), indent=2, ensure_ascii=False))

    if args.samples:
        print(json.dumps(get_news_samples(args.table, 10), indent=2, default=str, ensure_ascii=False))

    if args.count_thumbs:
        print("News with thumbnails:", count_news_with_thumbnails(args.table))

    # Novo comando: compara quantos registros têm image_url e quantos têm thumbnail_url
    if args.compare_imagens_thumbs:
        n_img = count_news_with_images(args.table)
        n_thumb = count_news_with_thumbnails(args.table)
        print(f"Registros com image_url: {n_img}")
        print(f"Registros com thumbnail_url: {n_thumb}")

    if args.auto_compare:
        candidates = [args.table, "news", "article"]
        seen = set()
        for t in candidates:
            if not t or t in seen:
                continue
            seen.add(t)
            sample = get_table_sample(t)
            if sample is None:
                print(f"Tabela '{t}': não encontrada ou sem permissões / sem registros.")
                continue

            cols = set(sample.keys())
            has_img = "image_url" in cols
            has_thumb = "thumbnail_url" in cols
            print(f"Tabela '{t}': colunas detectadas: {', '.join(sorted(cols))}")
            if has_img:
                print(f"  -> Registros com image_url: {count_news_with_images(t)}")
            else:
                print("  -> Coluna 'image_url' não encontrada nesta tabela.")

            if has_thumb:
                print(f"  -> Registros com thumbnail_url: {count_news_with_thumbnails(t)}")
            else:
                print("  -> Coluna 'thumbnail_url' não encontrada nesta tabela.")
