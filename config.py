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

KASPI_CATEGORIES = {
    "kaspi_Smartphones": "📱 Смартфоны",
    "kaspi_Notebooks": "💻 Ноутбуки",
    "kaspi_Tablets": "📱 Планшеты",
    "kaspi_Headphones": "🎧 Наушники",
    "kaspi_tv_audio": "📺 ТВ и Аудио",
    "kaspi_Refrigerators": "❄️ Холодильники",
    "kaspi_Vacuum Cleaners": "🧹 Пылесосы",
    "kaspi_Monitors": "🖥 Мониторы",
    "kaspi_Computers": "🖳 Компьютеры",
    "kaspi_Desktop Computers": "🖥 Настольные ПК",
    "kaspi_Game consoles": "🎮 Игровые приставки",
    "kaspi_home equipment": "🏠 Бытовая техника",
    "kaspi_Furniture": "🪑 Мебель",
    "kaspi_beauty care": "💄 Красота и уход",
    "kaspi_Car Goods": "🚗 Автотовары",
    "kaspi_sports and outdoors": "⚽ Спорт и отдых",
    "kaspi_child goods": "🧸 Детские товары",
    "kaspi_pharmacy": "💊 Аптека",
    "kaspi_construction and repair": "🛠 Строительство и ремонт",
    "kaspi_fashion": "👗 Одежда",
    "kaspi_shoes": "👟 Обувь",
    "kaspi_fashion accessories": "👜 Аксессуары",
    "kaspi_jewelry and bijouterie": "💍 Ювелирные изделия",
    "kaspi_home": "🏡 Для дома",
    "kaspi_pet goods": "🐾 Зоотовары",
    "kaspi_leisure": "🏕 Досуг",
}