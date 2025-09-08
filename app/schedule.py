import re, csv
from io import StringIO
from typing import Dict, Tuple, Optional, List, Set, Any
from dataclasses import dataclass
from app.parsers.site import get_links_from_site, SLink
from app.parsers.sheets import htmlview_url, csv_url, resolve_google_url, sheets_meta
from app.http import fetch_text

HYPHENS = "-\u2010\u2011\u2012\u2013\u2014\u2212"
HCLASS  = re.escape(HYPHENS)
NBSPS = {"\u00A0", "\u202F", "\u2007"}

def norm(s: str) -> str:
    if not s: return ""
    for ch in NBSPS: s = s.replace(ch, " ")
    return " ".join(s.split()).strip()

def norm_soft(s: str) -> str:
    if not s: return ""
    for ch in NBSPS: s = s.replace(ch, " ")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return "\n".join(" ".join(line.split()).strip() for line in s.split("\n")).strip()

def normalize_hyphens(s: str) -> str:
    for ch in HYPHENS[1:]: s = s.replace(ch, "-")
    return s

TIME_RX = re.compile(r"\d{1,2}[:.]\d{2}\s*[-–—]\s*\d{1,2}[:.]\d{2}")
CLASS_LABEL_RX = re.compile(r"(\d{1,2})\s*([^\d\s][^\d]*)", re.UNICODE)
CLASS_PURE_RX = re.compile(r'^\s*\d{1,2}\s*[A-Za-zА-Яа-яЁё]{1,6}\s*$')
CAB_CODE_RX = re.compile(
    rf"(?i)"
    rf"(?:\bкаб(?:инет)?\.?\s*[:\-]?\s*([A-Za-zА-Яа-я0-9_/ \t{HCLASS}]+))"
    rf"|(?:\b([А-ЯA-Z]\s*\d{{1,2}}\s*[{HCLASS}]\s*\d{{2}}(?:/\s*[А-ЯA-Z]\s*\d{{1,2}}\s*[{HCLASS}]\s*\d{{2}})*)\b)"
    rf"|(?:\b(спортзал(?:\s*\d*)?|актовый зал|спорт[ .-]?зал|ауд\.?\s*\d+)\b)"
)

def extract_cabinet(text: str) -> Optional[str]:
    if not text: return None
    for line in normalize_hyphens(norm_soft(text)).split("\n"):
        m = CAB_CODE_RX.search(line)
        if m:
            cab = next((g for g in m.groups() if g), None)
            if cab:
                import re as _re
                return _re.sub(r"\s+", "", normalize_hyphens(cab)).upper().replace("Ё","Е")
    return None

def parse_class_label(cell: str) -> Optional[str]:
    s = norm(cell).upper().replace("Ё","Е")
    m = CLASS_LABEL_RX.search(s)
    if not m: return None
    import re as _re
    suf = _re.sub(r"[ \-\d]+","", m.group(2))
    return f"{m.group(1)}{suf}" if suf else None

def grade_from_label(label: str) -> Optional[int]:
    m = re.match(r"(\d{1,2})", label)
    return int(m.group(1)) if m else None

LINKS: List[SLink] = []
DOC_URL: Dict[str, str] = {}
GID_BY_GRADE: Dict[str, Dict[int,str]] = {}
MATRIX: Dict[Tuple[str,str], Tuple[List[List[str]], Dict[str,Tuple[int,int,int]], List[int], Dict[str,Tuple[Optional[int],int]]]] = {}
STATE: Dict[int, Dict[str, Any]] = {}

async def ensure_links() -> List[SLink]:
    global LINKS
    if not LINKS:
        LINKS = await get_links_from_site()
    return LINKS

def _rows_from_csv_text(csv_text: str) -> List[List[str]]:
    return [list(r) for r in csv.reader(StringIO(csv_text))]

async def get_rows_from_csv(g_url: str, gid: str) -> List[List[str]]:
    return _rows_from_csv_text(await fetch_text(csv_url(g_url, gid)))

def parse_headers(rows: List[List[str]]) -> Tuple[Dict[str, Tuple[int,int,int]], List[int]]:
    labels, headers = {}, []
    for i, row in enumerate(rows[:400]):
        cells = [norm(c) for c in row]
        if not any("время" in (c or "").lower() for c in cells): continue
        time_cols = [j for j,c in enumerate(cells) if "время" in (c or "").lower()]
        if not time_cols: continue
        time_col = time_cols[0]; headers.append(i)
        for j, cell in enumerate(cells):
            label = parse_class_label(cell)
            if label: labels[label] = (i, time_col, j)
    return labels, headers

def next_header(headers: List[int], idx: int, total_rows: int) -> int:
    for h in headers:
        if h > idx: return h
    return total_rows

def right_boundary(labels: Dict[str, Tuple[int,int,int]], headers: List[int], label: str, total_cols: int) -> int:
    hdr, _, subj_col = labels[label]
    same = sorted(col for (h, _t, col) in labels.values() if h == hdr)
    next_cols = [c for c in same if c > subj_col]
    return min(next_cols[0] if next_cols else total_cols, total_cols)

def detect_cab_col(rows: List[List[str]], hdr: int, subj_col: int, end_row: int, right_bound: int) -> Optional[int]:
    for cand in range(subj_col+1, min(subj_col+4, right_bound)+1):
        if cand < len(rows[hdr]) and "каб" in norm(rows[hdr][cand]).lower(): return cand
    best, hits = None, -1
    for cand in range(subj_col+1, min(subj_col+4, right_bound)+1):
        cnt = sum(1 for r in rows[hdr+1: min(end_row, hdr+19)] if cand < len(r) and extract_cabinet(rows[r][cand]))
        if cnt > hits: best, hits = cand, cnt
    return best if hits>0 else None

def build_cab_map(rows, labels, headers):
    total_cols, total_rows = max((len(r) for r in rows), default=0), len(rows)
    ans = {}
    for lb,(hdr,_t,subj_col) in labels.items():
        end = next_header(headers, hdr, total_rows)
        right = right_boundary(labels, headers, lb, total_cols)
        ans[lb] = (detect_cab_col(rows, hdr, subj_col, end, right), right)
    return ans

def subject_nearby(rows, r, c, right_bound):
    def pick(cells, start, left, right):
        best, best_d = None, 10**9
        for j in range(max(0,left), min(right, len(cells))):
            val = norm_soft(cells[j])
            if not val or CAB_CODE_RX.search(val) or TIME_RX.search(val) or val.strip("—- ") == "" or CLASS_PURE_RX.match(val): continue
            d = abs(j-start)
            if d < best_d: best, best_d = val, d
        return best
    left, right = max(0,c-2), max(c+6,c+1)
    for rr in (r, r-1):
        if 0 <= rr < len(rows):
            got = pick([norm_soft(x) for x in rows[rr]], c, left, right)
            if got: return got
    return None

def find_cab(rows, r, c, cab_col, right_bound, prefer_above):
    def get(rr,cc):
        return rows[rr][cc] if 0<=rr<len(rows) and 0<=cc<len(rows[rr]) else None
    order = [r, r+1, r-1] if prefer_above else [r, r+1, r-1]
    if cab_col is not None:
        for rr in order:
            cab = extract_cabinet(get(rr, cab_col));  if cab: return cab
    for rr in order:
        cab = extract_cabinet(get(rr, c+1));         if cab: return cab
    limit = min(c+3, right_bound)
    for rr in order:
        for cc in range(c+2, limit+1):
            cab = extract_cabinet(get(rr, cc));      if cab: return cab
    for rr in order:
        cab = extract_cabinet(get(rr, c));           if cab: return cab
    for rr in order:
        cab = extract_cabinet(get(rr, c-1));         if cab: return cab
    return None

def extract_schedule(rows, labels, headers, label, cab_info):
    hdr, time_col, subj_col = labels[label]
    end = next_header(headers, hdr, len(rows))
    cab_col, right_bound = cab_info
    idxs = list(range(hdr+1, end))

    raw, last = [], None
    for r in idxs:
        t = rows[r][time_col] if time_col < len(rows[r]) else ""
        raw.append(t or None)
    filled=[]
    for t in raw:
        if t and t.strip(): last = t
        filled.append(last)
    if filled and filled[0] is None:
        nxt = next((x for x in filled if x is not None), None)
        if nxt is not None:
            i=0
            while i < len(filled) and filled[i] is None:
                filled[i] = nxt; i+=1

    out=[]
    for off, r in enumerate(idxs):
        row = [norm_soft(x) for x in rows[r]]
        time_range = (filled[off] or "").replace(".", ":")
        s = row[subj_col] if subj_col < len(row) else ""
        if not time_range and not s: continue
        subj_from_same = bool(s.strip())
        subj = s if subj_from_same else (subject_nearby(rows, r, subj_col, right_bound) or "—")
        if subj and CLASS_PURE_RX.match(subj): break
        cab = find_cab(rows, r, subj_col, cab_col, right_bound, prefer_above=not subj_from_same) if subj and subj!="—" else None
        out.append((time_range, subj, cab))
    return out

def _time_key(t: str) -> str:
    s = (t or "").replace(".", ":").strip()
    m = re.search(r'(\d{1,2}):(\d{2}).*?(\d{1,2}):(\d{2})', s)
    return f"{m.group(1).zfill(2)}:{m.group(2)}-{m.group(3).zfill(2)}:{m.group(4)}" if m else s

def collapse_by_time(items):
    order, subj_map, cab_map = [], {}, {}
    for t, subj, cab in items:
        k = _time_key(t)
        if k not in order:
            order.append(k); subj_map[k]=[]; cab_map[k]=[]
        s = (subj or "").strip()
        if s and s!="—" and s not in subj_map[k]: subj_map[k].append(s)
        if s and s!="—" and cab and cab.strip() and cab not in cab_map[k]: cab_map[k].append(cab.strip())
    out=[]
    for k in order:
        if not k: continue
        sj = " / ".join(subj_map.get(k, [])) or "—"
        cb = "/".join(cab_map.get(k, [])) or None
        if "-" in k and ":" in k:
            a,b = k.split("-",1); t = f"{a} - {b}"
        else:
            t = k
        out.append((t, sj, cb))
    return out

def pretty(date_label: str, klass: str, items) -> str:
    import html
    lines = [f"<b>РАСПИСАНИЕ НА {html.escape(date_label)}</b>", f"Класс: <b>{html.escape(klass)}</b>", ""]
    for i, (t, subj, cab) in enumerate(items, 1):
        subj_html = f"<b>{html.escape(subj)}</b>" if subj!="—" else "—"
        lines.append(f"{i} — ({t}) {subj_html}" if TIME_RX.search(t) else f"{i} — {subj_html}")
        if cab:
            lines.append(f"Кабинет: <b>{html.escape(cab)}</b>")
    return "\n".join(lines) if items else "Пусто."

async def ensure_sheet_for_grade(date: str, grade: int):
    g_url = DOC_URL.get(date)
    if not g_url:
        link = next((l for l in LINKS if l.date == date), None)
        if not link:
            LINKS[:] = await get_links_from_site()
            link = next((l for l in LINKS if l.date == date), None)
            if not link:
                raise RuntimeError("Дата не найдена.")
        g_url = await resolve_google_url(link.url); DOC_URL[date] = g_url

    if date in GID_BY_GRADE and grade in GID_BY_GRADE[date]:
        gid = GID_BY_GRADE[date][grade]
        if (date, gid) not in MATRIX:
            rows = await get_rows_from_csv(g_url, gid)
            labels, headers = parse_headers(rows)
            MATRIX[(date, gid)] = (rows, labels, headers, build_cab_map(rows, labels, headers))
        return g_url, gid, MATRIX[(date, gid)]

    gid2title, gids = await sheets_meta(g_url)
    quick = {grade_from_label(parse_class_label(t) or ""): gid for gid, t in gid2title.items()}
    if grade in quick and quick[grade]:
        gid = quick[grade]
        rows = await get_rows_from_csv(g_url, gid)
        labels, headers = parse_headers(rows)
        MATRIX[(date, gid)] = (rows, labels, headers, build_cab_map(rows, labels, headers))
        GID_BY_GRADE.setdefault(date, {})[grade] = gid
        return g_url, gid, MATRIX[(date, gid)]

    import aiohttp, asyncio
    sem = asyncio.Semaphore(6)
    async with aiohttp.ClientSession() as session:
        async def try_gid(gid):
            async with sem:
                try:
                    csv_text = await fetch_text(csv_url(g_url, gid), session=session)
                    rws = [list(r) for r in csv.reader(StringIO(csv_text))]
                    lm, hr = parse_headers(rws)
                    if grade in {grade_from_label(L) for L in lm}:
                        return gid, rws, lm, hr
                except:
                    return None
        tasks = [asyncio.create_task(try_gid(g)) for g in (list(gids) or ["0"])]
        for t in asyncio.as_completed(tasks):
            res = await t
            if res:
                gid, rows, labels, headers = res
                MATRIX[(date, gid)] = (rows, labels, headers, build_cab_map(rows, labels, headers))
                GID_BY_GRADE.setdefault(date, {})[grade] = gid
                return g_url, gid, MATRIX[(date, gid)]
    raise RuntimeError("Не нашёл вкладку для выбранного номера класса.")
