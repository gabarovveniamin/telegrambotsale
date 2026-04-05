from aiogram import Router, types, F, Bot
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError

from config import config
from database import db
import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

admin_router = Router()

# ──────────────────────────────────────────────────────────────────────────────
# Admin FSM States
# ──────────────────────────────────────────────────────────────────────────────
class AdminState(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_user_search = State()
    waiting_for_premium_days = State()
    waiting_for_personal_message = State()

# ──────────────────────────────────────────────────────────────────────────────
# Admin Keyboards
# ──────────────────────────────────────────────────────────────────────────────

def build_admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
         InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👑 Упр. Premium", callback_data="admin_premium_list")],
        [InlineKeyboardButton(text="⚡️ Действия", callback_data="admin_actions")],
        [InlineKeyboardButton(text="◀️ Выйти из админки", callback_data="back_main")],
    ])

def build_admin_actions_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Запустить мониторинг", callback_data="admin_run_monitoring")],
        [InlineKeyboardButton(text="🎯 Запустить следилку", callback_data="admin_run_tracker")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")],
    ])

def build_admin_user_kb(user_id: int, is_prem: bool) -> InlineKeyboardMarkup:
    prem_text = "🚫 Забрать Premium" if is_prem else "👑 Дать Premium"
    prem_cb = f"admin_revoke_{user_id}" if is_prem else f"admin_give_{user_id}"
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=prem_text, callback_data=prem_cb)],
        [InlineKeyboardButton(text="💬 Написать сообщение", callback_data=f"admin_msg_{user_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_users")],
    ])

# ──────────────────────────────────────────────────────────────────────────────
# Admin Handlers
# ──────────────────────────────────────────────────────────────────────────────

async def is_admin(user_id: int) -> bool:
    return user_id in config.ADMIN_IDS

@admin_router.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if not await is_admin(message.from_user.id):
        return

    await message.answer(
        "🛠 <b>Панель администратора</b>\n\nДобро пожаловать в центр управления ботом!",
        reply_markup=build_admin_main_kb(),
        parse_mode="HTML"
    )

@admin_router.callback_query(F.data == "admin_menu")
async def cb_admin_menu(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("У вас нет доступа!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🛠 <b>Панель администратора</b>",
        reply_markup=build_admin_main_kb(),
        parse_mode="HTML"
    )
    await callback.answer()

# --- Статистика ---
@admin_router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id): return
    
    stats = await db.get_stats()
    # Доп. статистика по Premium
    premium_list = await db.get_all_premium_list()
    total_stars = sum(p.get("stars_paid", 0) for p in premium_list)
    
    text = (
        "📊 <b>Общая статистика бота</b>\n\n"
        f"👤 Всего пользователей: <b>{stats['users']}</b>\n"
        f"👑 Активных Premium: <b>{stats['premium']}</b>\n"
        f"⭐️ Всего собрано Stars: <b>{total_stars}</b>\n\n"
        f"🔗 Всего рефералов: <b>{stats['referrals']}</b>\n"
        f"🎯 Товаров в следилках: <b>{stats['tracked_items']}</b>\n"
        f"🔍 Найдено уникальных скидок: <b>{stats['seen_items']}</b>\n"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stats")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

# --- Поиск пользователей ---
@admin_router.callback_query(F.data == "admin_users")
async def cb_admin_users(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id): return
    
    await callback.message.edit_text(
        "🔍 <b>Поиск пользователя</b>\n\nПришлите ID или @username пользователя для управления его профилем.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminState.waiting_for_user_search)
    await callback.answer()

@admin_router.message(AdminState.waiting_for_user_search)
async def process_user_search(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    
    input_text = message.text.strip().replace("@", "")
    user_data = None
    
    if input_text.isdigit():
        uid = int(input_text)
        user_data = await db.pool.fetchrow("SELECT * FROM users WHERE user_id = $1", uid)
    else:
        user_data = await db.pool.fetchrow("SELECT * FROM users WHERE username = $1", input_text)
        
    if not user_data:
        await message.answer("❌ Пользователь не найден в базе данных.")
        return
    
    uid = user_data["user_id"]
    is_prem = await db.is_premium(uid)
    sub_info = await db.get_subscription_info(uid)
    ref_count = await db.get_referral_count(uid)
    
    status = "👑 PREMIUM" if is_prem else "🆓 Бесплатный"
    expires = "♾ навсегда"
    if sub_info and sub_info.get("expires_at"):
        expires = sub_info["expires_at"].astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M")
    
    text = (
        f"👤 <b>Управление пользователем</b>\n\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"👤 Username: @{user_data['username'] or '—'}\n"
        f"📌 Статус: <b>{status}</b>\n"
        f"⏳ Истекает: {expires}\n"
        f"⭐️ Оплачено: {sub_info['stars_paid'] if sub_info else 0} Stars\n"
        f"👥 Рефералов: {ref_count}\n"
        f"📅 Регистрация: {user_data['created_at'].strftime('%d.%m.%Y')}\n"
    )
    
    await message.answer(text, reply_markup=build_admin_user_kb(uid, is_prem), parse_mode="HTML")
    await state.clear()

# --- Рассылка ---
@admin_router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id): return
    
    await callback.message.edit_text(
        "📢 <b>Массовая рассылка</b>\n\nПришлите текст сообщения (можно с HTML-разметкой), который увидят все пользователи.\n\n<i>/cancel для отмены</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminState.waiting_for_broadcast)
    await callback.answer()

@admin_router.message(AdminState.waiting_for_broadcast)
async def process_broadcast(message: types.Message, state: FSMContext, bot: Bot):
    if not await is_admin(message.from_user.id): return
    if message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=build_admin_main_kb())
        return

    text = message.text
    await message.answer("🚀 <b>Начинаю рассылку...</b>", parse_mode="HTML")
    await state.clear()

    # Импортируем из bot.py для соблюдения лимитов
    from bot import broadcast_message
    count = await broadcast_message(text)
    
    await message.answer(f"✅ <b>Рассылка завершена!</b>\nОтправлено сообщений: <b>{count}</b>", parse_mode="HTML")

# --- Действия ---
@admin_router.callback_query(F.data == "admin_actions")
async def cb_admin_actions(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id): return
    await callback.message.edit_text("⚡️ <b>Быстрые действия</b>", reply_markup=build_admin_actions_kb(), parse_mode="HTML")
    await callback.answer()

@admin_router.callback_query(F.data == "admin_run_monitoring")
async def cb_admin_run_monitoring(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id): return
    from scheduler import run_monitoring_cycle
    await callback.answer("⏳ Мониторинг запущен...")
    await run_monitoring_cycle()
    await callback.message.answer("✅ <b>Мониторинг завершен!</b>", parse_mode="HTML")

@admin_router.callback_query(F.data == "admin_run_tracker")
async def cb_admin_run_tracker(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id): return
    from scheduler import run_personal_tracker_cycle
    await callback.answer("⏳ Следилка запущена...")
    await run_personal_tracker_cycle()
    await callback.message.answer("✅ <b>Следилка завершена!</b>", parse_mode="HTML")

# --- Управление Premium ---

@admin_router.callback_query(F.data == "admin_premium_list")
async def cb_admin_premium_list(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id): return
    
    prem_users = await db.get_all_premium_list()
    if not prem_users:
        await callback.message.edit_text(
            "📋 Активных Premium-пользователей нет.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]
            ])
        )
        return

    text = f"👑 <b>Список Premium ({len(prem_users)} чел.)</b>\n\n"
    for p in prem_users[:30]: # Лимит для текста
        username = f"@{p['username']}" if p['username'] else f"ID: {p['user_id']}"
        exp = p['expires_at'].strftime("%d.%m.%Y") if p['expires_at'] else "♾"
        text += f"• {username} (до {exp})\n"
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Выдать по ID", callback_data="admin_users")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@admin_router.callback_query(F.data.startswith("admin_give_"))
async def cb_admin_give_prem(callback: types.CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split("_")[-1])
    await state.update_data(target_user_id=user_id)
    
    await callback.message.answer(
        f"👑 <b>Выдача Premium для {user_id}</b>\n\nВведите количество дней (например, 30) или '0' для бессрочного Premium.",
        parse_mode="HTML"
    )
    await state.set_state(AdminState.waiting_for_premium_days)
    await callback.answer()

@admin_router.message(AdminState.waiting_for_premium_days)
async def process_give_prem(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    
    try:
        days = int(message.text.strip())
        data = await state.get_data()
        uid = data.get("target_user_id")
        
        if days == 0:
            await db.grant_permanent_premium(uid)
            msg = "✅ Выдан <b>бессрочный</b> Premium!"
        else:
            await db.activate_subscription(uid, days=days, stars_paid=0)
            msg = f"✅ Выдан Premium на <b>{days} дней</b>!"
            
        await message.answer(msg, parse_mode="HTML")
        # Уведомить пользователя
        try:
            await message.bot.send_message(uid, "🎉 <b>Администратор выдал вам Premium-статус!</b>\nТеперь вам доступны все функции мониторинга.", parse_mode="HTML")
        except Exception: pass
            
    except ValueError:
        await message.answer("❌ Введите число.")
    
    await state.clear()

@admin_router.callback_query(F.data.startswith("admin_revoke_"))
async def cb_admin_revoke_prem(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id): return
    user_id = int(callback.data.split("_")[-1])
    
    await db.deactivate_subscription(user_id)
    await callback.message.edit_text(f"🚫 Premium отозван у пользоватя {user_id}", reply_markup=build_admin_main_kb())
    await callback.answer("Premium отозван")

# --- Личные сообщения ---
@admin_router.callback_query(F.data.startswith("admin_msg_"))
async def cb_admin_send_msg(callback: types.CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split("_")[-1])
    await state.update_data(target_user_id=user_id)
    
    await callback.message.answer(f"💬 <b>Сообщение для {user_id}</b>\n\nВведите текст сообщения:")
    await state.set_state(AdminState.waiting_for_personal_message)
    await callback.answer()

@admin_router.message(AdminState.waiting_for_personal_message)
async def process_personal_msg(message: types.Message, state: FSMContext, bot: Bot):
    if not await is_admin(message.from_user.id): return
    
    data = await state.get_data()
    uid = data.get("target_user_id")
    
    try:
        await bot.send_message(uid, f"✉️ <b>Сообщение от администрации:</b>\n\n{message.text}", parse_mode="HTML")
        await message.answer("✅ Отправлено.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        
    await state.clear()
