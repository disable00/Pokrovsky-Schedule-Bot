import html
import re
from datetime import datetime, timezone
from typing import Optional

from .config import MSK

NBSPS = {"\u00A0", "\u202F", "\u2007"}
HYPHENS = "-\u2010\u2011\u2012\u2013\u2014\u2212"


def norm(s: str) -> str:
    if not s:
        return ""
    for ch in NBSPS:
        s = s.replace(ch, " ")
    return " ".join(s.split()).strip()


def norm_soft(s: str) -> str:
    if not s:
        return ""
    for ch in NBSPS:
        s = s.replace(ch, " ")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return "\n".join(" ".join(line.split()).strip() for line in s.split("\n")).strip()


def normalize_hyphens(s: str) -> str:
    for ch in HYPHENS[1:]:
        s = s.replace(ch, "-")
    return s


def fmt_msk(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        msk = dt.astimezone(MSK)
        return f"{msk:%d.%m.%Y} ({msk:%H:%M} MSK)"
    except Exception:
        return str(iso)


def bold(s: str) -> str:
    return f"<b>{html.escape(s)}</b>"
