import os
from dataclasses import dataclass
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))
    NEWS_CHANNEL_URL: str = os.getenv("NEWS_CHANNEL_URL", "https://t.me/YourNewsChannel")
    NEWS_CHANNEL_ID = os.getenv("NEWS_CHANNEL_ID", "")
    PAGE_URL: str = os.getenv("PAGE_URL", "https://pokrovsky.gosuslugi.ru/glavnoe/raspisanie/")
    DB_PATH: str = os.getenv("DB_PATH", "bot_stats.sqlite3")
    TZ: str = os.getenv("TZ", "Europe/Moscow")
    USER_AGENT: str = "ScheduleBot/1.0"


settings = Settings()
MSK = ZoneInfo(settings.TZ or "Europe/Moscow")
HEADERS = {"User-Agent": settings.USER_AGENT}
