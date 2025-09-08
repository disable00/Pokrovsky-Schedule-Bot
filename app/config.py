import os
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv(override=True)

TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
NEWS_CHANNEL_URL = os.getenv("NEWS_CHANNEL_URL", "")
PAGE_URL = "https://pokrovsky.gosuslugi.ru/glavnoe/raspisanie/"
HEADERS = {"User-Agent": "ScheduleBot/1.0"}
DB_PATH = os.getenv("DB_PATH", "bot_stats.sqlite3")
MSK = ZoneInfo("Europe/Moscow")
