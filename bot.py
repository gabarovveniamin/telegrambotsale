import asyncio
import logging
from datetime import timezone

from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.filters import Command, CommandStart
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError
from aiogram.utils.chat_action import ChatActionSender
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery,
    ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from config import config
from database import db

logger = logging.getLogger(__name__)

router = Router()
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()

# ──────────────────────────────────────────────────────────────────────────────
# Конфиги подписки и рефералки
# ──────────────────────────────────────────────────────────────────────────────
PREMIUM_PRICE_STARS = 150          # Цена в Telegram Stars (в месяц)
PREMIUM_DAYS = 30                  # Длительность подписки (дни)
REFERRAL_BONUS_DAYS = 7           # Бонус рефереру за каждую конверсию (когда реферал купил Premium)
REFERRAL_INVITEES_FOR_FREE = 3    # Сколько друзей нужно пригласить для бесплатного Premium
REFERRAL_MILESTONE_WEEK = 3       # Рефералов для подарка: 1 неделя
REFERRAL_MILESTONE_MONTH = 10     # Рефералов для подарка: 1 месяц

# ──────────────────────────────────────────────────────────────────────────────
# FSM States
# ──────────────────────────────────────────────────────────────────────────────
class TrackState(StatesGroup):
    waiting_for_url = State()


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def build_main_menu(user_id: int = None) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu_settings"),
         InlineKeyboardButton(text="👑 Premium", callback_data="menu_premium")],
        [InlineKeyboardButton(text="🔗 Реферальная программа", callback_data="menu_referral")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="menu_stats")],
    ]
    
    # Добавляем кнопку админа, если пользователь в списке
    if user_id in config.ADMIN_IDS:
        buttons.append([InlineKeyboardButton(text="🛠 Админ-панель (Inline)", callback_data="admin_menu")])
        
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_admin_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⚙️ Админ панель", web_app=WebAppInfo(url="https://79.76.47.252.nip.io"))]
        ],
        resize_keyboard=True
    )


def build_premium_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"⭐️ Купить за {PREMIUM_PRICE_STARS} Stars ({PREMIUM_DAYS} дней)",
            callback_data="buy_premium"
        )],
        [InlineKeyboardButton(text="🔗 Получить бесплатно", callback_data="menu_referral")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
    ])


async def build_referral_kb(user_id: int) -> InlineKeyboardMarkup:
    bot_username = config.BOT_USERNAME
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    ref_count = await db.get_referral_count(user_id)
    reward_status = await db.get_ref_reward_status(user_id)

    buttons = [
        [InlineKeyboardButton(text="📤 Поделиться ссылкой", url=f"https://t.me/share/url?url={ref_link}&text=Получай%20скидки%20первым%20с%20этим%20ботом!")],
        [InlineKeyboardButton(text="📋 Скопировать ссылку", callback_data="copy_ref_link")],
    ]

    # Кнопка активации недели (3 рефералов)
    if ref_count >= REFERRAL_MILESTONE_WEEK:
        if reward_status["week_claimed"]:
            buttons.append([InlineKeyboardButton(text="✅ Неделя Premium — уже активирована", callback_data="ref_reward_noop")])
        else:
            buttons.append([InlineKeyboardButton(text="🎁 Активировать 7 дней Premium (за 3 друга)", callback_data="ref_claim_week")])

    # Кнопка активации месяца (10 рефералов)
    if ref_count >= REFERRAL_MILESTONE_MONTH:
        if reward_status["month_claimed"]:
            buttons.append([InlineKeyboardButton(text="✅ Месяц Premium — уже активирован", callback_data="ref_reward_noop")])
        else:
            buttons.append([InlineKeyboardButton(text="🎁 Активировать 30 дней Premium (за 10 друзей)", callback_data="ref_claim_month")])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ──────────────────────────────────────────────────────────────────────────────
# /start — с поддержкой реферальных ссылок
# ──────────────────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: types.Message):
    """Регистрация пользователя + обработка реферального кода."""
    user_id = message.from_user.id
    username = message.from_user.username

    # Регистрируем пользователя
    await db.add_user(user_id, username)

    # Проверяем реферальный код
    args = message.text.split(maxsplit=1)
    referral_bonus_text = ""

    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1].replace("ref_", ""))
            already_referred = await db.has_referred_before(user_id)

            if not already_referred and referrer_id != user_id:
                # Сначала убедимся что реферер существует в БД
                referrer_exists = await db.pool.fetchrow(
                    "SELECT 1 FROM users WHERE user_id = $1", referrer_id
                )
                if referrer_exists:
                    await db.register_referral(referrer_id, user_id)

                    # Уведомляем реферера
                    ref_count = await db.get_referral_count(referrer_id)
                    try:
                        await bot.send_message(
                            referrer_id,
                            f"🎉 <b>По вашей ссылке зарегистрировался новый пользователь!</b>\n\n"
                            f"👥 Ваших рефералов: <b>{ref_count}</b>\n\n"
                            f"💡 Когда он купит Premium, вы получите +{REFERRAL_BONUS_DAYS} дней бесплатно!",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

                    referral_bonus_text = (
                        f"\n\n🎁 <b>Вы зашли по реферальной ссылке!</b>\n"
                        f"Ваш друг получит бонус, когда вы активируете Premium."
                    )
        except (ValueError, IndexError):
            pass

    is_prem = await db.is_premium(user_id)
    premium_badge = "👑 <b>Premium</b>" if is_prem else "🆓 Бесплатный аккаунт"

    # Если пишет админ — присылаем и Reply-клавиатуру с WebApp
    reply_markup_to_send = build_main_menu(user_id)
    if user_id in config.ADMIN_IDS:
        # В aiogram 3.x мы можем отправить Reply-клавиатуру в сообщении. 
        # Но основное меню у нас Inline. Мы можем отправить сначала сообщение с Reply-кнопкой,
        # либо объединить (хотя Telegram не позволяет Inline и Reply в одном сообщении).
        # Сделаем как в запросе: для админа основное сообщение идет с Reply-клавиатурой (WebApp),
        # а инлайновое меню пришлем следующим сообщением или заменим.
        
        await message.answer(
            "🛠 <b>Доступ разрешен.</b>\nВаша секретная кнопка админ-панели добавлена в меню.",
            reply_markup=build_admin_reply_kb(),
            parse_mode="HTML"
        )

    await message.answer(
        f"👋 <b>Привет! Я бот для мониторинга скидок в Казахстане.</b>\n\n"
        f"🔔 Буду присылать уведомления о новых скидках!\n"
        f"📌 Статус: {premium_badge}"
        f"{referral_bonus_text}\n\n"
        f"Выбери раздел 👇",
        reply_markup=reply_markup_to_send,
        parse_mode="HTML"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Main menu callbacks
# ──────────────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "back_main")
async def cb_back_main(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    is_prem = await db.is_premium(user_id)
    premium_badge = "👑 <b>Premium</b>" if is_prem else "🆓 Бесплатный аккаунт"
    await callback.message.edit_text(
        f"🏠 <b>Главное меню</b>\n\nСтатус: {premium_badge}\n\nВыбери раздел 👇",
        reply_markup=build_main_menu(user_id),
        parse_mode="HTML"
    )
    await callback.answer()


# ──────────────────────────────────────────────────────────────────────────────
# Settings
# ──────────────────────────────────────────────────────────────────────────────

@router.message(Command("settings"))
async def cmd_settings(message: types.Message):
    await _show_settings(message, message.from_user.id)


@router.callback_query(F.data == "menu_settings")
async def cb_settings(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    threshold = await db.get_user_threshold(user_id)
    
    await callback.message.edit_text(
        "⚙️ <b>Настройки отслеживания</b>\n\n"
        f"🎯 Текущий порог уведомлений: <b>от {threshold}% скидки</b>\n"
        "Я буду присылать уведомления только если скидка больше или равна этому значению.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📉 Настроить порог скидки", callback_data="settings_threshold")],
            [InlineKeyboardButton(text="📂 Выбрать категории товаров", callback_data="settings_categories")],
            [InlineKeyboardButton(text="🎯 Моя следилка (Kaspi)", callback_data="settings_track")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(F.data == "settings_threshold")
async def cb_settings_threshold(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    current = await db.get_user_threshold(user_id)
    
    # Генерация кнопок для выбора порога
    buttons = []
    thresholds = [0, 5, 10, 20, 30, 40, 50, 70]
    
    row = []
    for t in thresholds:
        text = f"{t}%"
        if t == current:
            text = f"✅ {t}%"
        
        row.append(InlineKeyboardButton(text=text, callback_data=f"set_threshold_{t}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
        
    buttons.append([InlineKeyboardButton(text="◀️ Назад в настройки", callback_data="menu_settings")])
    
    await callback.message.edit_text(
        "🎯 <b>Настройка порога уведомлений</b>\n\n"
        "Выбери минимальный процент скидки, при котором я должен прислать тебе уведомление.\n"
        "<i>Например, если выберешь 20%, я не буду беспокоить тебя по скидкам 5-15%.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("set_threshold_"))
async def cb_set_threshold(callback: types.CallbackQuery):
    threshold = int(callback.data.split("_")[-1])
    user_id = callback.from_user.id
    
    await db.set_user_threshold(user_id, threshold)
    await callback.answer(f"✅ Порог установлен на {threshold}%", show_alert=True)
    await cb_settings_threshold(callback)


async def _show_settings(target, user_id: int):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📉 Порог скидки", callback_data="settings_threshold")],
        [InlineKeyboardButton(text="📂 Категории товаров", callback_data="settings_categories")],
        [InlineKeyboardButton(text="🎯 Добавить товар следилки", callback_data="settings_track")],
        [InlineKeyboardButton(text="💎 Premium подписка", callback_data="menu_premium")],
    ])
    await target.answer(
        "⚙️ <b>Настройки</b>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@router.callback_query(F.data == "settings_categories")
async def cb_settings_categories(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    enabled_cats = await db.get_user_categories(user_id)
    
    categories = {
        "tech":    "💻 Электроника и Техника",
        "fashion": "👟 Одежда и Обувь (Adidas, Intertop)",
        "other":   "📚 Другое (книги, игры)"
    }
    
    buttons = []
    for code, label in categories.items():
        status = "✅" if code in enabled_cats else "❌"
        buttons.append([InlineKeyboardButton(text=f"{status} {label}", callback_data=f"toggle_cat_{code}")])
    
    buttons.append([InlineKeyboardButton(text="◀️ Назад в настройки", callback_data="menu_settings")])
    
    await callback.message.edit_text(
        "📂 <b>Выбор категорий уведомлений</b>\n\n"
        "Выберите категории товаров, по которым вы хотите получать уведомления о скидках:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("toggle_cat_"))
async def cb_toggle_category(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    cat_to_toggle = callback.data.replace("toggle_cat_", "")
    
    enabled_cats = await db.get_user_categories(user_id)
    if cat_to_toggle in enabled_cats:
        enabled_cats.remove(cat_to_toggle)
    else:
        enabled_cats.append(cat_to_toggle)
    
    await db.set_user_categories(user_id, enabled_cats)
    await cb_settings_categories(callback)


@router.callback_query(F.data == "settings_track")
async def cb_track_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало добавления товара в следилку — только для Premium."""
    user_id = callback.from_user.id
    is_prem = await db.is_premium(user_id)

    if not is_prem:
        await callback.answer(
            "👑 Следилка доступна только для Premium-подписчиков!\n"
            "Купи подписку или пригласи друзей для бесплатного доступа.",
            show_alert=True
        )
        return

    await callback.message.answer(
        "🎯 <b>Точечная следилка</b>\n\n"
        "Пришли ссылку на товар:\n"
        "• <b>Kaspi.kz</b>\n• <b>Sulpak.kz</b>\n• <b>Mechta.kz</b>\n\n"
        "Я буду проверять цену каждый час и пришлю уведомление при изменении!",
        parse_mode="HTML"
    )
    await state.set_state(TrackState.waiting_for_url)
    await callback.answer()


@router.callback_query(F.data.startswith("settings_"))
async def cb_settings_other(callback: types.CallbackQuery):
    await callback.answer("⏳ Этот раздел в разработке!", show_alert=True)


@router.message(TrackState.waiting_for_url)
async def process_track_url(message: types.Message, state: FSMContext):
    url = message.text.strip()
    shop = None
    if "kaspi.kz" in url:    shop = "Kaspi"
    elif "sulpak.kz" in url: shop = "Sulpak"
    elif "mechta.kz" in url: shop = "Mechta"

    if not shop:
        await message.answer("❌ Поддерживаю только Kaspi, Sulpak или Mechta.")
        await state.clear()
        return

    from parser import parser
    await message.answer(f"🔍 Проверяю товар из {shop}...")
    current_price = await parser.get_single_product_price(url, shop)

    if current_price is None:
        await message.answer("❌ Не удалось получить цену. Проверьте правильность URL.")
    else:
        await db.add_tracked_item(message.from_user.id, shop, url, current_price)
        await message.answer(
            f"✅ <b>Товар добавлен в следилку!</b>\n\n"
            f"🏪 Магазин: {shop}\n"
            f"💰 Текущая цена: {current_price:,} ₸\n\n"
            "Уведомлю при изменении цены.",
            parse_mode="HTML"
        )

    await state.clear()


# ──────────────────────────────────────────────────────────────────────────────
# Premium menu
# ──────────────────────────────────────────────────────────────────────────────

@router.message(Command("premium"))
async def cmd_premium(message: types.Message):
    await _show_premium_menu(message, message.from_user.id)


@router.callback_query(F.data == "menu_premium")
async def cb_premium_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    text = await _build_premium_text(user_id)
    await callback.message.edit_text(text, reply_markup=build_premium_kb(), parse_mode="HTML")
    await callback.answer()


async def _show_premium_menu(target, user_id: int):
    text = await _build_premium_text(user_id)
    await target.answer(text, reply_markup=build_premium_kb(), parse_mode="HTML")


async def _build_premium_text(user_id: int) -> str:
    is_prem = await db.is_premium(user_id)
    sub_info = await db.get_subscription_info(user_id)
    ref_count = await db.get_referral_count(user_id)

    if is_prem and sub_info and sub_info.get("expires_at"):
        expires = sub_info["expires_at"].astimezone(timezone.utc).strftime("%d.%m.%Y")
        status_line = f"👑 <b>Активна</b> — до {expires}"
    elif is_prem:
        status_line = "👑 <b>Активна</b> (бессрочно)"
    else:
        status_line = "🚫 Не активна"

    return (
        f"💎 <b>Premium-подписка</b>\n\n"
        f"📌 Статус: {status_line}\n\n"
        f"<b>Что даёт Premium:</b>\n"
        f"• 🔔 Уведомления о новых скидках — <b>только для Premium!</b>\n"
        f"• 🎯 Точечная следилка товаров\n"
        f"• 📊 Расширенная статистика\n\n"
        f"<b>Цена:</b> {PREMIUM_PRICE_STARS} ⭐️ Stars / месяц\n\n"
        f"🔗 <b>Бесплатно:</b> Пригласи <b>{REFERRAL_INVITEES_FOR_FREE}</b> друзей!\n"
        f"Твоих рефералов: <b>{ref_count}</b> / {REFERRAL_INVITEES_FOR_FREE}"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Buy Premium (Telegram Stars Invoice)
# ──────────────────────────────────────────────────────────────────────────────

@router.message(Command("buy"))
async def cmd_buy(message: types.Message):
    await _send_invoice(message.chat.id, message.from_user.id)


@router.callback_query(F.data == "buy_premium")
async def cb_buy_premium(callback: types.CallbackQuery):
    await _send_invoice(callback.message.chat.id, callback.from_user.id)
    await callback.answer()


async def _send_invoice(chat_id: int, user_id: int):
    """Send a Stars payment invoice."""
    await bot.send_invoice(
        chat_id=chat_id,
        title="👑 Premium-подписка",
        description=(
            f"Ежемесячная подписка ({PREMIUM_DAYS} дней): уведомления о скидках "
            f"из Технодома, Алсера, Kaspi, Mechta, Sulpak и других магазинов Казахстана. "
            f"Следилка товаров и расширенная статистика."
        ),
        payload=f"premium_sub_{PREMIUM_DAYS}_days_{user_id}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=f"Premium на {PREMIUM_DAYS} дней", amount=PREMIUM_PRICE_STARS)],
    )


@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    """Подтверждаем готовность принять оплату."""
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@router.message(F.successful_payment)
async def successful_payment_handler(message: types.Message):
    """Успешная оплата — активируем Premium и начисляем бонус рефереру."""
    user_id = message.from_user.id
    payment = message.successful_payment
    stars = payment.total_amount  # Количество звезд

    # Активируем подписку в базе
    await db.activate_subscription(user_id, days=PREMIUM_DAYS, stars_paid=stars)

    # Проверяем, был ли реферал, и выдаём бонус
    referrer_id = await db.get_referrer_of(user_id)
    referrer_bonus_text = ""
    if referrer_id:
        rewarded = await db.reward_referral(referrer_id, user_id, bonus_days=REFERRAL_BONUS_DAYS)
        if rewarded:
            referrer_bonus_text = f"\n\n🎁 Ваш пригласивший получил <b>+{REFERRAL_BONUS_DAYS} дней Premium</b>!"
            try:
                ref_count = await db.get_referral_count(referrer_id)
                await bot.send_message(
                    referrer_id,
                    f"🎉 <b>Ваш реферал купил Premium!</b>\n\n"
                    f"Вам начислено <b>+{REFERRAL_BONUS_DAYS} дней</b> Premium-подписки!\n"
                    f"Всего рефералов: {ref_count}",
                    parse_mode="HTML"
                )
            except Exception:
                pass

    await message.answer(
        f"🎉 <b>Оплата прошла успешно!</b>\n\n"
        f"⭐️ Списано: {stars} Stars\n"
        f"📅 Premium активирован на <b>{PREMIUM_DAYS} дней</b>\n\n"
        f"Теперь тебе доступны все функции бота!"
        f"{referrer_bonus_text}",
        reply_markup=build_main_menu(user_id),
        parse_mode="HTML"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Referral system
# ──────────────────────────────────────────────────────────────────────────────

@router.message(Command("referral"))
async def cmd_referral(message: types.Message):
    await _show_referral_menu(message, message.from_user.id)


@router.callback_query(F.data == "menu_referral")
async def cb_referral_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    text = await _build_referral_text(user_id)
    kb = await build_referral_kb(user_id)
    await callback.message.edit_text(
        text,
        reply_markup=kb,
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    await callback.answer()


@router.callback_query(F.data == "copy_ref_link")
async def cb_copy_ref_link(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    bot_username = config.BOT_USERNAME
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    await callback.message.answer(
        f"🔗 <b>Ваша реферальная ссылка:</b>\n\n"
        f"<code>{ref_link}</code>\n\n"
        "Нажмите на ссылку, чтобы скопировать.",
        parse_mode="HTML"
    )
    await callback.answer()


async def _show_referral_menu(target, user_id: int):
    text = await _build_referral_text(user_id)
    kb = await build_referral_kb(user_id)
    await target.answer(
        text,
        reply_markup=kb,
        parse_mode="HTML",
        disable_web_page_preview=True
    )


async def _build_referral_text(user_id: int) -> str:
    bot_username = config.BOT_USERNAME
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    ref_count = await db.get_referral_count(user_id)
    referrals = await db.get_referrals(user_id)
    reward_status = await db.get_ref_reward_status(user_id)

    converted = sum(1 for r in referrals if r["rewarded"])

    # Прогресс бар к 10 (максимальный milestone)
    max_milestone = REFERRAL_MILESTONE_MONTH
    bar_count = min(ref_count, max_milestone)
    progress_bar = "🟢" * bar_count + "⚪️" * (max_milestone - bar_count)

    lines = [
        f"🔗 <b>Реферальная программа</b>\n\n",
        f"📊 Прогресс: {progress_bar} {ref_count}/{max_milestone}\n",
        f"👥 Приглашено: <b>{ref_count}</b> чел.\n",
        f"💰 Принесли бонус (купили Premium): <b>{converted}</b>\n\n",
        f"<b>🎁 Награды за рефералов:</b>\n",
    ]

    # Статус наград
    week_status = "✅ Получена" if reward_status["week_claimed"] else ("🔓 Доступна — нажми 'Активировать'" if ref_count >= REFERRAL_MILESTONE_WEEK else f"🔒 {REFERRAL_MILESTONE_WEEK - ref_count} до разблокировки")
    month_status = "✅ Получена" if reward_status["month_claimed"] else ("🔓 Доступна — нажми 'Активировать'" if ref_count >= REFERRAL_MILESTONE_MONTH else f"🔒 {REFERRAL_MILESTONE_MONTH - ref_count} до разблокировки")

    lines += [
        f"3 друга → 7 дней Premium: {week_status}\n",
        f"10 друзей → 30 дней Premium: {month_status}\n\n",
        f"<b>Как работает:</b>\n",
        f"1️⃣ Поделись своей ссылкой с друзьями\n",
        f"2️⃣ Когда они запустят бота — ты получишь +1 реферал\n",
        f"3️⃣ При достижении 3 или 10 рефералов — нажми <b>Активировать</b>\n\n",
        f"🔗 Твоя ссылка:\n<code>{ref_link}</code>",
    ]

    return "".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Claim referral milestone rewards
# ──────────────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "ref_reward_noop")
async def cb_ref_reward_noop(callback: types.CallbackQuery):
    await callback.answer("✅ Эта награда уже активирована!", show_alert=False)


@router.callback_query(F.data == "ref_claim_week")
async def cb_claim_week(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    ref_count = await db.get_referral_count(user_id)

    if ref_count < REFERRAL_MILESTONE_WEEK:
        await callback.answer(f"❌ Нужно {REFERRAL_MILESTONE_WEEK} рефералов. У тебя: {ref_count}", show_alert=True)
        return

    claimed = await db.claim_ref_reward(user_id, "week")
    if not claimed:
        await callback.answer("✅ Ты уже активировал эту награду!", show_alert=True)
        return

    await db.activate_subscription(user_id, days=7, stars_paid=0)
    await callback.answer("🎉 7 дней Premium активировано!", show_alert=True)
    await callback.message.answer(
        "🎁 <b>Поздравляем!</b>\n\n"
        "Ты пригласил 3 друга и получил <b>7 дней Premium</b> бесплатно!\n"
        "Наслаждайся уведомлениями о скидках 🔥",
        parse_mode="HTML"
    )
    # Обновляем меню
    text = await _build_referral_text(user_id)
    kb = await build_referral_kb(user_id)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)


@router.callback_query(F.data == "ref_claim_month")
async def cb_claim_month(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    ref_count = await db.get_referral_count(user_id)

    if ref_count < REFERRAL_MILESTONE_MONTH:
        await callback.answer(f"❌ Нужно {REFERRAL_MILESTONE_MONTH} рефералов. У тебя: {ref_count}", show_alert=True)
        return

    claimed = await db.claim_ref_reward(user_id, "month")
    if not claimed:
        await callback.answer("✅ Ты уже активировал эту награду!", show_alert=True)
        return

    await db.activate_subscription(user_id, days=30, stars_paid=0)
    await callback.answer("🎉 30 дней Premium активировано!", show_alert=True)
    await callback.message.answer(
        "🏆 <b>Невероятно!</b>\n\n"
        "Ты пригласил 10 друзей и получил <b>30 дней Premium</b> бесплатно!\n"
        "Это целый месяц уведомлений о самых жирных скидках 💎",
        parse_mode="HTML"
    )
    # Обновляем меню
    text = await _build_referral_text(user_id)
    kb = await build_referral_kb(user_id)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)


# ──────────────────────────────────────────────────────────────────────────────
# My stats
# ──────────────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "menu_stats")
async def cb_stats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    is_prem = await db.is_premium(user_id)
    sub_info = await db.get_subscription_info(user_id)
    ref_count = await db.get_referral_count(user_id)
    tracked = await db.get_user_tracked_items(user_id)
    referrer_id = await db.get_referrer_of(user_id)

    if is_prem and sub_info and sub_info.get("expires_at"):
        expires = sub_info["expires_at"].astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M")
        sub_line = f"👑 Premium до {expires}"
    elif is_prem:
        sub_line = "👑 Premium (бессрочно)"
    else:
        sub_line = "🆓 Бесплатный"

    stars_total = sub_info.get("stars_paid", 0) if sub_info else 0
    referrer_line = f"Вас пригласил: @{referrer_id}" if referrer_id else "Вы пришли сами"

    await callback.message.edit_text(
        f"📊 <b>Моя статистика</b>\n\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"📌 Подписка: {sub_line}\n"
        f"⭐️ Потрачено Stars: <b>{stars_total}</b>\n"
        f"🔗 Рефералов: <b>{ref_count}</b>\n"
        f"🎯 Товаров в следилке: <b>{len(tracked)}</b>\n"
        f"👋 {referrer_line}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()


# ──────────────────────────────────────────────────────────────────────────────
# Sale commands (broadcasts)
# ──────────────────────────────────────────────────────────────────────────────

@router.message(Command("sale"))
async def cmd_sale(message: types.Message):
    """Команда для вывода всех текущих скидок с Технодома."""
    from parser import parser
    from curl_cffi.requests import AsyncSession

    await message.answer(
        "🔄 <b>Начинаю собирать скидки с Технодома...</b>\nЭто может занять время!",
        parse_mode="HTML"
    )

    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                td_items = await parser.fetch_technodom(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")
            return

    if not td_items:
        await message.answer("Пока нет скидок в Технодоме.")
        return

    lines = [f"🟣 <b>Скидки Технодом (Найдено: {len(td_items)})</b>\n"]
    for item in td_items:
        price_line = f"<s>{item['old_price']}</s> → <b>{item['new_price']}</b>"
        title = item["title"][:57] + "..." if len(item["title"]) > 60 else item["title"]
        percent = ""
        try:
            old = float(item["old_price"].replace(" ₸", "").replace("₸", "").replace(" ", ""))
            new = float(item["new_price"].replace(" ₸", "").replace("₸", "").replace(" ", ""))
            percent = f" (-{round((old - new) / old * 100)}%)"
        except Exception:
            pass
        lines.append(f"• <a href='{item['link']}'>{title}</a>{percent}\n  {price_line}\n")

    chunk = ""
    for line in lines:
        if len(chunk) + len(line) > 4000:
            await message.answer(chunk, parse_mode="HTML", disable_web_page_preview=True)
            chunk = line
            await asyncio.sleep(0.5)
        else:
            chunk += line
    if chunk:
        await message.answer(chunk, parse_mode="HTML", disable_web_page_preview=True)
    await message.answer("✅ <b>Вывод завершён!</b>", parse_mode="HTML")


@router.message(Command("salekaspi", "kaspisale"))
async def cmd_salekaspi(message: types.Message):
    """Команда для вывода акций Каспи."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Kaspi...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_kaspi(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в Kaspi."); return

    header = f"🔵 <b>Скидки Kaspi (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


@router.message(Command("salealser"))
async def cmd_salealser(message: types.Message):
    """Команда для вывода акций Alser."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Alser...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_alser(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в Alser."); return

    header = f"🟢 <b>Скидки Alser (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


@router.message(Command("salemechta"))
async def cmd_salemechta(message: types.Message):
    """Команда для вывода акций Mechta."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Mechta...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_mechta(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в Mechta."); return

    header = f"🔵 <b>Скидки Mechta (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


@router.message(Command("freedomsale"))
async def cmd_salefreedom(message: types.Message):
    """Команда для вывода акций Freedom Mobile."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Freedom Mobile...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_freedom(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок во Freedom Mobile."); return

    header = f"🟢 <b>Скидки Freedom Mobile (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


@router.message(Command("adidassale"))
async def cmd_saleadidas(message: types.Message):
    """Команда для вывода акций Adidas KZ."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Adidas KZ...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_adidas(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в Adidas KZ."); return

    header = f"👟 <b>Скидки Adidas KZ (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


async def send_sale_chunks(message, items, header):
    """Вспомогательная функция для отправки товаров кусками."""
    # Сортируем по проценту скидки перед выводом
    items.sort(key=lambda x: x.get("discount", 0), reverse=True)
    
    lines = [header]
    for item in items[:40]: # Ограничим до 40 самых жирных скидок для скорости
        price_line = f"<s>{item['old_price']}</s> → <b>{item['new_price']}</b>"
        title = item["title"][:57] + "..." if len(item["title"]) > 60 else item["title"]
        lines.append(f"• <a href='{item['link']}'>{title}</a> (-{item['discount']}%)\n  {price_line}\n")

    chunk = ""
    for line in lines:
        if len(chunk) + len(line) > 4000:
            await message.answer(chunk, parse_mode="HTML", disable_web_page_preview=True)
            chunk = line
        else: chunk += line
    if chunk:
        await message.answer(chunk, parse_mode="HTML", disable_web_page_preview=True)
    await message.answer("✅ <b>Вывод завершён!</b>", parse_mode="HTML")


@router.message(Command("salesulpak"))
async def cmd_salesulpak(message: types.Message):
    """Команда для вывода акций Sulpak."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Sulpak...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_sulpak(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в Sulpak."); return

    header = f"🔴 <b>Скидки Sulpak (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


@router.message(Command("intertopsale"))
async def cmd_intertopsale(message: types.Message):
    """Команда для вывода акций Intertop."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Intertop...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_intertop(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в Intertop."); return

    header = f"👟 <b>Скидки Intertop (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


@router.message(Command("saleshopkz"))
async def cmd_saleshopkz(message: types.Message):
    """Команда для вывода акций Shop.kz (Белый Ветер)."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Shop.kz...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_shopkz(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в Shop.kz."); return

    header = f"🌪 <b>Скидки Shop.kz (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


@router.message(Command("salemeloman"))
async def cmd_salemeloman(message: types.Message):
    """Команда для вывода акций Meloman."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с Meloman...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_meloman(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в Meloman."); return

    header = f"📚 <b>Скидки Meloman (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


@router.message(Command("saledns"))
async def cmd_saledns(message: types.Message):
    """Команда для вывода акций DNS Shop KZ."""
    from parser import parser
    from curl_cffi.requests import AsyncSession
    await message.answer("🔄 <b>Начинаю собирать скидки с DNS Shop...</b>", parse_mode="HTML")
    async with ChatActionSender.typing(bot=bot, chat_id=message.chat.id):
        try:
            async with AsyncSession(impersonate=parser.impersonate) as session:
                items = await parser.fetch_dns(session)
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}"); return

    if not items:
        await message.answer("Пока нет скидок в DNS Shop."); return

    header = f"🔴 <b>Скидки DNS Shop (Найдено: {len(items)})</b>\n"
    await send_sale_chunks(message, items, header)


# ──────────────────────────────────────────────────────────────────────────────
# Broadcast helper (used by scheduler)
# ──────────────────────────────────────────────────────────────────────────────

async def broadcast_message(text: str, premium_only: bool = False, min_discount: int = 0, category: str = None):
    """
    Safe mass broadcast respecting Telegram API rate limits.
    premium_only=True — used for premium-exclusive notifications.
    min_discount — only send to users whose threshold is <= this value.
    category — filter users by selected categories (tech, fashion, other).
    """
    if category and min_discount > 0:
        # Фильтруем по категории И порогу скидки
        users = await db.get_users_by_category_and_threshold(category, min_discount, premium_only=premium_only)
    elif min_discount > 0:
        # Фильтруем только по порогу (старая логика)
        users = await db.get_users_with_threshold(min_discount, premium_only=premium_only)
    elif premium_only:
        users = await db.get_premium_users()
    else:
        users = await db.get_all_users()
    count = 0

    for user_id in users:
        try:
            await bot.send_message(user_id, text, parse_mode="HTML", disable_web_page_preview=False)
            count += 1
            await asyncio.sleep(1 / config.MAX_MESSAGES_PER_SECOND)
        except TelegramRetryAfter as e:
            logger.warning(f"Rate limit: waiting {e.retry_after}s")
            await asyncio.sleep(e.retry_after)
            await bot.send_message(user_id, text, parse_mode="HTML")
        except TelegramForbiddenError:
            logger.info(f"User {user_id} blocked the bot.")
        except Exception as e:
            logger.error(f"Broadcast error for {user_id}: {e}")

    return count


from admin_panel import admin_router
dp.include_router(router)
dp.include_router(admin_router)