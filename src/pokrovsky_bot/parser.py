import html
import re
from typing import Dict, List, Optional, Tuple

from .state import CLASS_PURE_RX, CLASS_LABEL_RX, TIME_RX
from .utils import norm, norm_soft, normalize_hyphens

# --- паттерны для распознавания кабинетов ---
HYPHENS = "-\u2010\u2011\u2012\u2013\u2014\u2212"
HCLASS = re.escape(HYPHENS)

CAB_CODE_RX = re.compile(
    rf"(?i)"
    rf"(?:\bкаб(?:инет)?\.?\s*[:\-]?\s*([A-Za-zА-Яа-я0-9_/ \t{HCLASS}]+))"
    rf"|(?:\b([А-ЯA-Z]\s*\d{{1,2}}\s*[{HCLASS}]\s*\d{{2}}(?:/\s*[А-ЯA-Z]\s*\d{{1,2}}\s*[{HCLASS}]\s*\d{{2}})*)\b)"
    rf"|(?:\b(спортзал(?:\s*\d*)?|актовый зал|спорт[ .-]?зал|ауд\.?\s*\d+)\b)"
)

# ---------- утилиты ----------
def extract_cabinet(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    for line in normalize_hyphens(norm_soft(text)).split("\n"):
        m = CAB_CODE_RX.search(line)
        if m:
            cab = next((g for g in m.groups() if g), None)
            if cab:
                import re as _re
                return _re.sub(r"\s+", "", normalize_hyphens(cab)).upper().replace("Ё", "Е")
    return None

def parse_class_label(cell: str) -> Optional[str]:
    s = norm(cell).upper().replace("Ё", "Е")
    m = CLASS_LABEL_RX.search(s)
    if not m:
        return None
    suf = re.sub(r"[ \-\d]+", "", m.group(2))
    return f"{m.group(1)}{suf}" if suf else None

def grade_from_label(label: str) -> Optional[int]:
    m = re.match(r"(\d{1,2})", label)
    return int(m.group(1)) if m else None

# ---------- поиск заголовков блока ----------
def parse_headers(rows: List[List[str]]) -> Tuple[Dict[str, Tuple[int, int, int]], List[int]]:
    labels: Dict[str, Tuple[int, int, int]] = {}
    headers: List[int] = []
    for i, row in enumerate(rows[:400]):
        cells = [norm(c) for c in row]
        if not any("время" in (c or "").lower() for c in cells):
            continue
        time_cols = [j for j, c in enumerate(cells) if "время" in (c or "").lower()]
        if not time_cols:
            continue
        time_col = time_cols[0]
        headers.append(i)
        for j, cell in enumerate(cells):
            label = parse_class_label(cell)
            if label:
                labels[label] = (i, time_col, j)
    return labels, headers

def next_header(headers: List[int], idx: int, total_rows: int) -> int:
    for h in headers:
        if h > idx:
            return h
    return total_rows

def right_boundary(
    labels: Dict[str, Tuple[int, int, int]],
    headers: List[int],
    label: str,
    total_cols: int,
) -> int:
    hdr, _, subj_col = labels[label]
    same = sorted(col for (h, _t, col) in labels.values() if h == hdr)
    next_cols = [c for c in same if c > subj_col]
    return min(next_cols[0] if next_cols else total_cols, total_cols)

def detect_cab_col(rows: List[List[str]], hdr: int, subj_col: int, end_row: int, right_bound: int) -> Optional[int]:
    # явный заголовок "каб"
    for cand in range(subj_col + 1, min(subj_col + 4, right_bound) + 1):
        if cand < len(rows[hdr]) and "каб" in norm(rows[hdr][cand]).lower():
            return cand
    # статистика встречаемости кабинетов в первых ~18 строках блока
    best, hits = None, -1
    for cand in range(subj_col + 1, min(subj_col + 4, right_bound) + 1):
        cnt = 0
        for row in rows[hdr + 1 : min(end_row, hdr + 19)]:
            if cand < len(row) and extract_cabinet(row[cand]):
                cnt += 1
        if cnt > hits:
            best, hits = cand, cnt
    return best if hits > 0 else None

def build_cab_map(
    rows: List[List[str]],
    labels: Dict[str, Tuple[int, int, int]],
    headers: List[int]
) -> Dict[str, Tuple[Optional[int], int]]:
    total_cols, total_rows = max((len(r) for r in rows), default=0), len(rows)
    m: Dict[str, Tuple[Optional[int], int]] = {}
    for lb, (hdr, _t, subj_col) in labels.items():
        end = next_header(headers, hdr, total_rows)
        right = right_boundary(labels, headers, lb, total_cols)
        m[lb] = (detect_cab_col(rows, hdr, subj_col, end, right), right)
    return m

# ---------- извлечение по правилам «пара строк» ----------
def _normalize_time(s: str) -> str:
    s = (s or "").replace(".", ":").strip()
    s = re.sub(r"\s*[-–—]\s*", " - ", s)
    return s

def extract_schedule(
    rows: List[List[str]],
    labels: Dict[str, Tuple[int, int, int]],
    headers: List[int],
    label: str,
    cab_info: Tuple[Optional[int], int],  # cab_col не используем для поиска
):
    hdr, time_col, subj_col = labels[label]
    end = next_header(headers, hdr, len(rows))
    _cab_col, right_bound = cab_info
    out: List[Tuple[str, Optional[str], Optional[str]]] = []

    r = hdr + 1
    while r < end:
        row = rows[r]
        subj_cell = row[subj_col] if subj_col < len(row) else ""
        subj = norm_soft(subj_cell).strip() or None
        if subj and CLASS_PURE_RX.match(subj):
            break

        # где время: в этой строке или в нижней
        t_here = norm_soft(row[time_col]) if time_col < len(row) else ""
        t_next = norm_soft(rows[r + 1][time_col]) if (r + 1) < end and time_col < len(rows[r + 1]) else ""

        # если предмет есть и время в НИЖНЕЙ строке — это «пара» строк
        if subj and t_next and not t_here and (r + 1) < end:
            time_range = _normalize_time(t_next)
            # кабинет: правый нижний сосед (r+1, subj_col+1), не выходя за правую границу
            cab = None
            cc = subj_col + 1
            if cc <= right_bound and cc < len(rows[r + 1]):
                cab = extract_cabinet(rows[r + 1][cc])
            out.append((time_range, subj, cab))
            r += 2
            continue

        # одиночная строка (время здесь или вообще нет)
        time_range = _normalize_time(t_here)
        cab = None
        if subj:
            # по правилу — правый нижний; если нижней строки нет, пробуем правого соседа в этой строке
            if (r + 1) < end:
                cc = subj_col + 1
                if cc <= right_bound and cc < len(rows[r + 1]):
                    cab = extract_cabinet(rows[r + 1][cc])
            if not cab:
                cc0 = subj_col + 1
                if cc0 < len(row) and cc0 <= right_bound:
                    cab = extract_cabinet(row[cc0])

        # если и времени, и предмета нет — пропускаем
        if not time_range and not subj:
            r += 1
            continue

        out.append((time_range, subj, cab))
        r += 1

    return out

# ---------- группировка и вывод ----------
def _time_key(t: str) -> str:
    s = (t or "").replace(".", ":").strip()
    m = re.search(r'(\d{1,2}):(\d{2}).*?(\d{1,2}):(\d{2})', s)
    return f"{m.group(1).zfill(2)}:{m.group(2)}-{m.group(3).zfill(2)}:{m.group(4)}" if m else s

def collapse_by_time(items: List[tuple]) -> List[tuple]:
    order, subj_map, cab_map = [], {}, {}
    for t, subj, cab in items:
        k = _time_key(t)
        if k not in order:
            order.append(k); subj_map[k] = []; cab_map[k] = []
        s = (subj or "").strip()
        if s and s not in subj_map[k]:
            subj_map[k].append(s)
        if s and cab and cab.strip() and cab not in cab_map[k]:
            cab_map[k].append(cab.strip())
    out = []
    for k in order:
        if not k:
            continue
        sj = " / ".join(subj_map.get(k, [])) or "—"
        cb = "/".join(cab_map.get(k, [])) or None
        t = f"{k.split('-', 1)[0]} - {k.split('-', 1)[1]}" if "-" in k and ":" in k else k
        out.append((t, sj, cb))
    return out

def pretty(date_label: str, klass: str, items: List[tuple]) -> str:
    lines = [f"<b>РАСПИСАНИЕ НА {html.escape(date_label)}</b>", f"Класс: <b>{html.escape(klass)}</b>", ""]
    for i, (t, subj, cab) in enumerate(items, 1):
        subj_html = f"<b>{html.escape(subj)}</b>" if subj else "—"
        show_time = bool(re.search(r"\d{1,2}[:.]\d{2}.*\d{1,2}[:.]\d{2}", t))
        lines.append(f"{i} — ({html.escape(t)}) {subj_html}" if show_time else f"{i} — {subj_html}")
        lines.append(f"Кабинет: <b>{html.escape(cab)}</b>" if cab else "Кабинет: <b>—</b>")
    return "\n".join(lines) if items else "Пусто."
