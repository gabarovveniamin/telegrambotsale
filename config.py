import os
from dotenv import load_dotenv
from dataclasses import dataclass, field
from typing import List
load_dotenv()
@dataclass
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    BOT_USERNAME: str = os.getenv("BOT_USERNAME", "salesalesa1e_bot")
    CRYPTOPAY_TOKEN: str = os.getenv("CRYPTOPAY_TOKEN", "")
    ADMIN_IDS: List[int] = field(default_factory=lambda: [
        int(x.strip()) for x in os.getenv("ADMIN_IDS", "YOUR_TELEGRAM_ID_HERE").split(",")
        if x.strip().lstrip("-").isdigit()
    ])
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        ""
    )
    FETCH_INTERVAL_MINUTES: int = 5
    MAX_MESSAGES_PER_SECOND: int = 25
    MAX_MESSAGES_PER_MINUTE_GROUP: int = 20
config = Config()