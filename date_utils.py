"""Utilities para normalizar e parsear datas de feeds RSS.

Fornece a função `parse_rss_date_to_dt` que aceita strings RFC-2822 em
inglês (ex: 'Wed, 26 Nov 2025 11:01:00 GMT') e formatos com meses/dias
abreviados em português (ex: 'Qua, nov 26 2025 08:25:00'), retornando um
`datetime` timezone-aware (UTC) ou `None` se não for possível parsear.

Uso:
    from date_utils import parse_rss_date_to_dt

    dt = parse_rss_date_to_dt('Qua, nov 26 2025 08:25:00')
    if dt:
        iso = dt.isoformat()  # '2025-11-26T08:25:00+00:00'

Obs: para melhores resultados instale `python-dateutil` (opcional, usado
como fallback): `pip install python-dateutil`.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

try:
    # dateutil é um fallback mais permissivo para formatos variados
    from dateutil import parser as _dateutil_parser  # type: ignore
except Exception:
    _dateutil_parser = None

# Mapa de nomes/abreviações em português -> abreviação em inglês
PT_TO_EN_MONTH = {
    "jan": "Jan", "janeiro": "Jan",
    "fev": "Feb", "fevereiro": "Feb",
    "mar": "Mar", "marco": "Mar", "março": "Mar",
    "abr": "Apr", "abril": "Apr",
    "mai": "May", "maio": "May",
    "jun": "Jun", "junho": "Jun",
    "jul": "Jul", "julho": "Jul",
    "ago": "Aug", "agosto": "Aug",
    "set": "Sep", "setembro": "Sep",
    "out": "Oct", "outubro": "Oct",
    "nov": "Nov", "novembro": "Nov",
    "dez": "Dec", "dezembro": "Dec",
}


def _strip_accents(s: str) -> str:
    """Remove acentos/diacríticos de uma string."""
    return ''.join(
        c for c in unicodedata.normalize('NFKD', s)
        if not unicodedata.combining(c)
    )


def pt_to_en_date_string(s: str) -> str:
    """Converte uma string de data em português para uma forma com meses em inglês.

    Exemplos:
        'Qua, nov 26 2025 08:25:00' -> 'nov 26 2025 08:25:00' -> 'Nov 26 2025 08:25:00'
    """
    if not s:
        return s

    s0 = s.strip()
    # remover acentos e normalizar para minúsculas para o processamento
    s0 = _strip_accents(s0).lower()

    # remove weekday no início (ex: 'qua,', 'quarta,') se existir
    s0 = re.sub(r'^[a-z]{2,9},?\s*', '', s0)

    # regex com as chaves do dicionário (ordenar por tamanho para evitar colisões)
    months_pattern = r'\b(' + '|'.join(sorted(PT_TO_EN_MONTH.keys(), key=len, reverse=True)) + r')\b'

    def repl(m: re.Match) -> str:
        key = m.group(0)
        return PT_TO_EN_MONTH.get(key, key)

    s1 = re.sub(months_pattern, repl, s0, flags=re.IGNORECASE)

    # devolver com casing 'Jan' etc — parsedate_to_datetime aceita variações, mas
    # manter capitalização ajuda legibilidade
    # capitalize apenas as ocorrências de meses (é suficiente usar o resultado)
    return s1


def parse_rss_date_to_dt(s: str) -> Optional[datetime]:
    """Converte uma string de data de RSS para `datetime` timezone-aware (UTC).

    Aceita tanto formatos RFC-2822 em inglês quanto versões com nomes/abreviações
    em português (faz normalização). Retorna `None` se não conseguir parsear.
    """
    if not s:
        return None

    # 1) tentar parse direto (funciona para RFC-2822 em inglês)
    try:
        dt = parsedate_to_datetime(s)
        if dt is not None:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
    except Exception:
        pass

    # 2) normalizar PT -> EN e tentar novamente
    try:
        s_en = pt_to_en_date_string(s)
        dt = parsedate_to_datetime(s_en)
        if dt is not None:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
    except Exception:
        pass

    # 3) fallback com dateutil, se disponível
    if _dateutil_parser is not None:
        try:
            s_en = pt_to_en_date_string(s)
            dt = _dateutil_parser.parse(s_en)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    return None


__all__ = ["parse_rss_date_to_dt", "pt_to_en_date_string"]
