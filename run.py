import os, sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
CANDIDATES = [
    BASE / "src",
    BASE / "pokrovsky-bot" / "src",
    BASE.parent / "src",
    BASE.parent / "pokrovsky-bot" / "src",
]
for p in CANDIDATES:
    if (p / "pokrovsky_bot").exists():
        sys.path.insert(0, str(p))
        break
else:
    raise SystemExit("Не нашёл пакет pokrovsky_bot. Убедись, что рядом есть папка src/pokrovsky_bot "
                     "или установи проект: pip install -e .")

from pokrovsky_bot.main import main

if __name__ == "__main__":
    main()
