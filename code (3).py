
import asyncio
import logging
import os
from datetime import datetime, timedelta
import asyncpg # Для работы с PostgreSQL
import time # Для генерации report_id

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest # Для обработки ошибок закрепления

# --- КОНФИГУРАЦИЯ ---
# На Railway в Variables добавь BOT_TOKEN, ADMIN_ID, DATABASE_URL
TOKEN = os.getenv("8336714025:AAFF028y4ae3n-0ul4y8DIZpvj69KffjKIU")
ADMIN_USERNAME = "aggentov" # Имя пользователя админа без @
# Исправлено: Проверяем, существует ли ADMIN_ID, прежде чем преобразовывать его в int
ADMIN_ID_STR = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_STR) if ADMIN_ID_STR else None # ID админа для личных уведомлений (должен быть числом!)
DATABASE_URL = os.getenv("DATABASE_URL") # URL базы данных PostgreSQL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if not TOKEN:
    logging.error("Ошибка: Переменная BOT_TOKEN не установлена!")
    exit("Ошибка: Переменная BOT_TOKEN не установлена!")
if ADMIN_ID is None: # Изменено: теперь проверяем на None
    logging.warning("Переменная ADMIN_ID не установлена. Уведомления админу и админ-панель могут работать некорректно.")
if not DATABASE_URL:
    logging.error("Ошибка: Переменная DATABASE_URL не установлена!")
    exit("Ошибка: Переменная DATABASE_URL не установлена!")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
db_pool = None # Пул соединений к БД

# --- СОСТОЯНИЯ ДЛЯ FSM ---
class ReportStates(StatesGroup):
    # Состояние для ввода причины жалобы (если выбрано "Другое")
    waiting_for_custom_reason = State()
    # Состояние для ввода ID/Username цели жалобы
    waiting_for_target = State()

# --- БАЗА ДАННЫХ ---
async def init_db():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        async with db_pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_reports INT DEFAULT 0,
                    is_banned BOOLEAN DEFAULT FALSE,
                    ban_message_id BIGINT DEFAULT NULL -- ID сообщения о бане для удаления
                );
            ''')
            # Таблица жалоб
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS reports (
                    report_id SERIAL PRIMARY KEY,
                    sender_id BIGINT,
                    sender_username TEXT,
                    reason TEXT,
                    target_id BIGINT,
                    target_username TEXT,
                    report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending', -- pending, approved, rejected
                    message_id BIGINT DEFAULT NULL -- ID сообщения с жалобой у пользователя (для редактирования)
                );
            ''')
        logging.info("База данных инициализирована и таблицы проверены/созданы.")
    except Exception as e:
        logging.error(f"Ошибка инициализации БД: {e}")
        exit(f"Ошибка инициализации БД: {e}")

async def register_user(user_id: int, username: str, first_name: str):
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO users (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE
            SET username = $2, first_name = $3;
        ''', user_id, username, first_name)

async def get_user_data(user_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)

async def increment_user_reports(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE users SET total_reports = total_reports + 1 WHERE user_id = $1', user_id)

async def add_report(sender_id: int, sender_username: str, reason: str, target_id: int | None, target_username: str | None, message_id: int | None):
    async with db_pool.acquire() as conn:
        report = await conn.fetchrow('''
            INSERT INTO reports (sender_id, sender_username, reason, target_id, target_username, message_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING report_id, report_time;
        ''', sender_id, sender_username, reason, target_id, target_username, message_id)
        return report['report_id'], report['report_time']

async def get_report_by_id(report_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow('SELECT * FROM reports WHERE report_id = $1', report_id)

async def update_report_status(report_id: int, status: str):
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE reports SET status = $1 WHERE report_id = $2', status, report_id)

async def get_pending_reports(limit: int, offset: int):
    async with db_pool.acquire() as conn:
        return await conn.fetch('SELECT * FROM reports WHERE status = \'pending\' ORDER BY report_time DESC LIMIT $1 OFFSET $2', limit, offset)

async def count_pending_reports():
    async with db_pool.acquire() as conn:
        return await conn.fetchval('SELECT COUNT(*) FROM reports WHERE status = \'pending\'')

async def get_all_users_db(limit: int, offset: int, banned: bool = False):
    async with db_pool.acquire() as conn:
        return await conn.fetch('SELECT * FROM users WHERE is_banned = $1 ORDER BY reg_date DESC LIMIT $2 OFFSET $3', banned, limit, offset)

async def count_all_users_db(banned: bool = False):
    async with db_pool.acquire() as conn:
        return await conn.fetchval('SELECT COUNT(*) FROM users WHERE is_banned = $1', banned)

async def ban_user_db(user_id: int, ban_message_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE users SET is_banned = TRUE, ban_message_id = $1 WHERE user_id = $2', ban_message_id, user_id)

async def unban_user_db(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE users SET is_banned = FALSE, ban_message_id = NULL WHERE user_id = $1', user_id)

# --- КЛАВИАТУРЫ ---

# 1. Приветственная клавиатура
def get_welcome_kb(is_admin: bool):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📩 Отправить донос", callback_data="start_report"))
    if is_admin:
        builder.row(InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin_panel"))
    return builder.as_markup()

# 2. Клавиатура выбора заготовки/своей жалобы
def get_report_options_kb():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🥷 Мошенничество", callback_data="report_preset:Мошенничество"),
        InlineKeyboardButton(text="🦠 Файл с вирусом", callback_data="report_preset:Файл с вирусом")
    )
    builder.row(
        InlineKeyboardButton(text="🔞 Взрослый контент", callback_data="report_preset:Взрослый контент"),
        InlineKeyboardButton(text="🛑 Опасная ссылка", callback_data="report_preset:Опасная ссылка")
    )
    builder.row(
        InlineKeyboardButton(text="🚨 Доксер", callback_data="report_preset:Доксер"),
        InlineKeyboardButton(text="🚓 Сватер", callback_data="report_preset:Сватер")
    )
    builder.row(
        InlineKeyboardButton(text="👹 Тролль", callback_data="report_preset:Тролль"),
        InlineKeyboardButton(text="⚙️ Другое", callback_data="report_custom")
    )
    return builder.as_markup()

# 3. Клавиатура после отправки жалобы
def get_report_sent_kb(admin_username: str):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="Моментальное одобрение 🟢", url=f"https://t.me/{admin_username}"))
    builder.row(InlineKeyboardButton(text="В меню", callback_data="back_to_main"))
    return builder.as_markup()

# 4. Админ-панель
def get_admin_panel_kb():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🛑 Жалобы", callback_data="admin_reports:0"))
    builder.row(InlineKeyboardButton(text="👤 Пользователи", callback_data="admin_users:0"))
    builder.row(InlineKeyboardButton(text="❌ Бан-лист", callback_data="admin_banlist:0"))
    builder.row(InlineKeyboardButton(text="🟢 Быстрое одобрение", callback_data="admin_fast_approve"))
    builder.row(InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_main"))
    return builder.as_markup()

# 5. Пагинация для админ-панели (списки жалоб/пользователей)
async def get_pagination_kb(callback_prefix: str, current_page: int, total_items: int, items_per_page: int, get_items_func, is_banned_list: bool = False):
    builder = InlineKeyboardBuilder()
    total_pages = (total_items + items_per_page - 1) // items_per_page if total_items > 0 else 1
    
    # Добавляем кнопки для каждого элемента на текущей странице
    items = []
    if callback_prefix == "admin_reports":
        items = await get_items_func(limit=items_per_page, offset=current_page * items_per_page)
        for item in items:
            text = f"#{item['report_id']} {item['reason']}"
            builder.row(InlineKeyboardButton(text=text, callback_data=f"view_report:{item['report_id']}"))
    elif callback_prefix in ["admin_users", "admin_banlist"]:
        items = await get_items_func(limit=items_per_page, offset=current_page * items_per_page, banned=is_banned_list)
        for item in items:
            text = f"@{item['username']}" if item['username'] else f"ID: {item['user_id']}"
            builder.row(InlineKeyboardButton(text=text, callback_data=f"view_user:{item['user_id']}"))

    # Кнопки пагинации
    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"{callback_prefix}:{current_page - 1}"))
    
    nav_row.append(InlineKeyboardButton(text=f"{current_page + 1}/{total_pages}", callback_data="noop"))
    
    if current_page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"{callback_prefix}:{current_page + 1}"))
    
    if nav_row:
        builder.row(*nav_row)
    
    builder.row(InlineKeyboardButton(text="◀️ Назад в Админ-панель", callback_data="admin_panel"))
    return builder.as_markup()

# 6. Клавиатура для просмотра конкретной жалобы
def get_report_actions_kb(report_id: int):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🟢 Одобрить", callback_data=f"report_action:approve:{report_id}"))
    builder.row(InlineKeyboardButton(text="🔴 Отказать", callback_data=f"report_action:reject:{report_id}"))
    builder.row(InlineKeyboardButton(text="▶ Следующая жалоба", callback_data="admin_reports:0")) # Просто возвращаемся к списку
    builder.row(InlineKeyboardButton(text="◀️ Назад в Админ-панель", callback_data="admin_panel"))
    return builder.as_markup()

# 7. Клавиатура для просмотра профиля пользователя
def get_user_profile_kb(user_id: int, is_banned: bool, from_banlist: bool = False):
    builder = InlineKeyboardBuilder()
    if is_banned:
        builder.row(InlineKeyboardButton(text="🟩 Разбан", callback_data=f"user_action:unban:{user_id}"))
    else:
        builder.row(InlineKeyboardButton(text="🛑 Бан", callback_data=f"user_action:ban:{user_id}"))
    
    if from_banlist:
        builder.row(InlineKeyboardButton(text="◀️ Назад к Бан-листу", callback_data="admin_banlist:0"))
    else:
        builder.row(InlineKeyboardButton(text="◀️ Назад к Пользователям", callback_data="admin_users:0"))
    return builder.as_markup()

# --- ХЭЛПЕРЫ ---
async def check_admin(user_id: int) -> bool:
    # Исправлено: Проверяем, что ADMIN_ID не None, прежде чем сравнивать
    return ADMIN_ID is not None and user_id == ADMIN_ID

async def get_user_mention(user_id: int, username: str | None, first_name: str | None) -> str:
    if username:
        return f"@{username}"
    elif first_name:
        return f"<a href='tg://user?id={user_id}'>{first_name}</a>"
    else:
        return f"ID: {user_id}"

# --- ОБРАБОТЧИКИ ---

# Приветствие
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    user_data = await get_user_data(message.from_user.id)
    
    if user_data and user_data['is_banned']:
        user_mention = await get_user_mention(message.from_user.id, message.from_user.username, message.from_user.first_name)
        await message.answer(f"🛑 **{user_mention}**, Вы были заблокированы!\n❌ Теперь бот не будет отвечать на команды, сколько вы бы ни пытались.", parse_mode="HTML")
        return

    is_admin = await check_admin(message.from_user.id)
    
    sent_message = await message.answer(
        "👋 Добро пожаловать в Telegram Donos.\n\n"
        "🤖 Я бот, который пишет множество жалоб на пользователя, я являюсь предметом для защиты личных данных пользователей!\n\n"
        "‼️ Важно ‼️\n"
        "Если вы будете злоупотреблять ботом, вы будете заблокированы в боте и в скором, возможно, заблокированы в телеграм по причине сноса обычных пользователей.",
        reply_markup=get_welcome_kb(is_admin)
    )
    try:
        await bot.pin_chat_message(chat_id=message.chat.id, message_id=sent_message.message_id)
    except TelegramBadRequest as e:
        logging.warning(f"Не удалось закрепить сообщение в чате {message.chat.id}: {e}")

# Проверка на бан для всех callback_query
@dp.callback_query()
async def check_ban_callback(callback: CallbackQuery, state: FSMContext):
    user_data = await get_user_data(callback.from_user.id)
    if user_data and user_data['is_banned']:
        await callback.answer(f"🛑 Вы заблокированы и не можете использовать бота.", show_alert=True)
        return
    await process_callback_query(callback, state)

# Проверка на бан для всех message
@dp.message()
async def check_ban_message(message: types.Message, state: FSMContext):
    await register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    user_data = await get_user_data(message.from_user.id)
    if user_data and user_data['is_banned']:
        user_mention = await get_user_mention(message.from_user.id, message.from_user.username, message.from_user.first_name)
        await message.answer(f"🛑 **{user_mention}**, Вы заблокированы и не можете использовать бота.", parse_mode="HTML")
        return
    await process_message(message, state)


# Обработчик callback_query (после проверки на бан)
async def process_callback_query(callback: CallbackQuery, state: FSMContext):
    # back_to_main
    if callback.data == "back_to_main":
        await state.clear()
        is_admin = await check_admin(callback.from_user.id)
        await callback.message.edit_text(
            "👋 Добро пожаловать в Telegram Donos.\n\n"
            "🤖 Я бот, который пишет множество жалоб на пользователя, я являюсь предметом для защиты личных данных пользователей!\n\n"
            "‼️ Важно ‼️\n"
            "Если вы будете злоупотреблять ботом, вы будете заблокированы в боте и в скором, возможно, заблокированы в телеграм по причине сноса обычных пользователей.",
            reply_markup=get_welcome_kb(is_admin)
        )
        await callback.answer()
        return

    # start_report
    if callback.data == "start_report":
        await callback.message.edit_text(
            "Хорошо, выберите заготовку или введите свою жалобу",
            reply_markup=get_report_options_kb()
        )
        await callback.answer()
        return

    # report_preset
    if callback.data.startswith("report_preset:"):
        reason = callback.data.split(":")[1]
        await state.update_data(reason=reason) # Сохраняем причину
        await callback.message.edit_text("Пожалуйста, ответьте на сообщение пользователя, на которого подаете жалобу, или введите его ID/Username:")
        await state.set_state(ReportStates.waiting_for_target) # Переходим в состояние ожидания цели
        await callback.answer()
        return

    # report_custom (ввод своей жалобы)
    if callback.data == "report_custom":
        await callback.message.edit_text("Введите жалобу до 16 символов:")
        await state.set_state(ReportStates.waiting_for_custom_reason) # Переходим в состояние ожидания пользовательской причины
        await callback.answer()
        return
    
    # admin_panel
    if callback.data == "admin_panel":
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа к админ-панели.", show_alert=True)
            return
        
        admin_mention_text = "Pavel Durov"
        if ADMIN_ID: # Если ADMIN_ID установлен, используем его для упоминания
            admin_mention = await get_user_mention(ADMIN_ID, ADMIN_USERNAME, admin_mention_text)
        else: # Иначе просто текст
            admin_mention = admin_mention_text

        await callback.message.edit_text(
            f"Привет! {admin_mention}\n",
            reply_markup=get_admin_panel_kb()
        )
        await callback.answer()
        return

    # admin_reports
    if callback.data.startswith("admin_reports:"):
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа.", show_alert=True)
            return
        page = int(callback.data.split(":")[1])
        items_per_page = 5
        total_reports = await count_pending_reports()
        
        kb = await get_pagination_kb("admin_reports", page, total_reports, items_per_page, get_pending_reports)
        await callback.message.edit_text("Нерешённые жалобы:", reply_markup=kb)
        await callback.answer()
        return

    # view_report
    if callback.data.startswith("view_report:"):
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа.", show_alert=True)
            return
        report_id = int(callback.data.split(":")[1])
        report = await get_report_by_id(report_id)
        if report:
            sender_user_data = await get_user_data(report['sender_id'])
            sender_mention = await get_user_mention(report['sender_id'], report['sender_username'], sender_user_data['first_name'])
            target_mention = await get_user_mention(report['target_id'], report['target_username'], "Неизвестный") # Если target_id нет, то username
            
            await callback.message.edit_text(
                f"№{report['report_id']} жалоба\n"
                f"Причина: {report['reason']}\n"
                f"ID отправителя: {report['sender_id']}\n"
                f"Username отправителя: {sender_mention}\n"
                f"ID/Username на кого подана жалоба: {target_mention}\n"
                f"Статус: 🟡 Ждет одобрения...",
                reply_markup=get_report_actions_kb(report_id)
            )
        else:
            await callback.message.edit_text("Жалоба не найдена.")
        await callback.answer()
        return

    # report_action (approve/reject)
    if callback.data.startswith("report_action:"):
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа.", show_alert=True)
            return
        action = callback.data.split(":")[1]
        report_id = int(callback.data.split(":")[2])
        
        report = await get_report_by_id(report_id)
        if report:
            new_status = "approved" if action == "approve" else "rejected"
            await update_report_status(report_id, new_status)
            
            sender_user_data = await get_user_data(report['sender_id'])
            sender_mention = await get_user_mention(report['sender_id'], report['sender_username'], sender_user_data['first_name'])
            
            # Уведомляем пользователя, который отправил жалобу
            try:
                if action == "approve":
                    await bot.send_message(report['sender_id'], f"🟢 Жалоба №{report_id} одобрена!")
                else:
                    await bot.send_message(report['sender_id'], f"🔴 Жалоба №{report_id} отказана!")
            except Exception as e:
                logging.error(f"Не удалось отправить уведомление пользователю {report['sender_id']}: {e}")
            
            # Обновляем сообщение в админ-панели
            target_mention = await get_user_mention(report['target_id'], report['target_username'], "Неизвестный")
            await callback.message.edit_text(
                f"№{report['report_id']} жалоба\n"
                f"Причина: {report['reason']}\n"
                f"ID отправителя: {report['sender_id']}\n"
                f"Username отправителя: {sender_mention}\n"
                f"ID/Username на кого подана жалоба: {target_mention}\n"
                f"Статус: {'🟢 Одобрена' if action == 'approve' else '🔴 Отказана'}",
                reply_markup=get_report_actions_kb(report_id) # Можно обновить на другую клавиатуру, без кнопок одобрения/отказа
            )
        await callback.answer()
        return

    # admin_users
    if callback.data.startswith("admin_users:"):
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа.", show_alert=True)
            return
        page = int(callback.data.split(":")[1])
        items_per_page = 10
        total_users = await count_all_users_db(banned=False)
        
        kb = await get_pagination_kb("admin_users", page, total_users, items_per_page, get_all_users_db, is_banned_list=False)
        await callback.message.edit_text("Пользователи, зарегистрированные в боте:", reply_markup=kb)
        await callback.answer()
        return

    # admin_banlist
    if callback.data.startswith("admin_banlist:"):
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа.", show_alert=True)
            return
        page = int(callback.data.split(":")[1])
        items_per_page = 10
        total_banned_users = await count_all_users_db(banned=True)
        
        kb = await get_pagination_kb("admin_banlist", page, total_banned_users, items_per_page, get_all_users_db, is_banned_list=True)
        await callback.message.edit_text("Пользователи в бан-листе:", reply_markup=kb)
        await callback.answer()
        return

    # view_user
    if callback.data.startswith("view_user:"):
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа.", show_alert=True)
            return
        user_id = int(callback.data.split(":")[1])
        user = await get_user_data(user_id)
        if user:
            user_mention = f"@{user['username']}" if user['username'] else f"ID: {user['user_id']}"
            
            from_banlist = False
            if callback.message.text and "Пользователи в бан-листе:" in callback.message.text: # Проверяем откуда пришел запрос
                from_banlist = True

            await callback.message.edit_text(
                f"👤 Username: **{user_mention}**\n"
                f"🆔 ID: **{user['user_id']}**\n"
                f"⏳ Время регистрации: **{user['reg_date'].strftime('%d.%m.%Y %H:%M:%S')}**\n"
                f"🔢 Всего доносов: **{user['total_reports']}**\n"
                f"🎂 Тариф: **Стандартный**", # Тариф не реализован, пока заглушка
                reply_markup=get_user_profile_kb(user_id, user['is_banned'], from_banlist),
                parse_mode="HTML"
            )
        else:
            await callback.message.edit_text("Пользователь не найден.")
        await callback.answer()
        return

    # user_action (ban/unban)
    if callback.data.startswith("user_action:"):
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа.", show_alert=True)
            return
        action = callback.data.split(":")[1]
        target_user_id = int(callback.data.split(":")[2])
        
        target_user = await get_user_data(target_user_id)
        if target_user:
            target_mention = await get_user_mention(target_user['user_id'], target_user['username'], target_user['first_name'])
            
            if action == "ban":
                ban_msg = await bot.send_message(target_user_id, f"🛑 **{target_mention}**, Вы были заблокированы!\n❌ Теперь бот не будет отвечать на команды, сколько вы бы ни пытались.", parse_mode="HTML")
                await ban_user_db(target_user_id, ban_msg.message_id)
                # Определяем, откуда пришел запрос, чтобы вернуться в нужный список
                from_banlist = False
                if callback.message.reply_markup and callback.message.reply_markup.inline_keyboard:
                    for row in callback.message.reply_markup.inline_keyboard:
                        for button in row:
                            if button.callback_data == "admin_banlist:0":
                                from_banlist = True
                                break
                        if from_banlist: break

                await callback.message.edit_text(f"Пользователь {target_mention} заблокирован.", reply_markup=get_user_profile_kb(target_user_id, True, from_banlist=from_banlist))
            elif action == "unban":
                await unban_user_db(target_user_id)
                if target_user['ban_message_id']:
                    try:
                        await bot.delete_message(target_user_id, target_user['ban_message_id'])
                    except TelegramBadRequest:
                        logging.warning(f"Не удалось удалить сообщение о бане для {target_user_id}")
                await bot.send_message(target_user_id, f"✅ **{target_mention}**, Вы были разблокированы!", parse_mode="HTML")
                
                from_banlist = False
                if callback.message.reply_markup and callback.message.reply_markup.inline_keyboard:
                    for row in callback.message.reply_markup.inline_keyboard:
                        for button in row:
                            if button.callback_data == "admin_banlist:0":
                                from_banlist = True
                                break
                        if from_banlist: break

                await callback.message.edit_text(f"Пользователь {target_mention} разблокирован.", reply_markup=get_user_profile_kb(target_user_id, False, from_banlist=from_banlist))
        else:
            await callback.message.edit_text("Пользователь не найден.")
        await callback.answer()
        return
    
    # admin_fast_approve (пока заглушка)
    if callback.data == "admin_fast_approve":
        if not await check_admin(callback.from_user.id):
            await callback.answer("У вас нет доступа.", show_alert=True)
            return
        await callback.answer("Функция быстрого одобрения пока не реализована.", show_alert=True)
        return

    # noop (пустая кнопка)
    if callback.data == "noop":
        await callback.answer()
        return

# Обработчик message (после проверки на бан)
async def process_message(message: types.Message, state: FSMContext):
    current_state = await state.get_state()

    # Состояние для ввода собственной причины жалобы
    if current_state == ReportStates.waiting_for_custom_reason:
        custom_reason = message.text
        if len(custom_reason) > 16:
            await message.answer("🛑 Ошибка! Много символов. Введите жалобу до 16 символов:")
            return # Остаемся в том же состоянии
        
        await state.update_data(reason=custom_reason) # Сохраняем пользовательскую причину
        await message.answer("Теперь, пожалуйста, ответьте на сообщение пользователя, на которого подаете жалобу, или введите его ID/Username:")
        await state.set_state(ReportStates.waiting_for_target) # Переходим в состояние ожидания цели
        return

    # Состояние для ввода ID/Username цели жалобы (после выбора заготовки или ввода своей причины)
    if current_state == ReportStates.waiting_for_target:
        state_data = await state.get_data()
        reason = state_data.get('reason')
        
        target_id = None
        target_username = None

        if message.reply_to_message: # Если ответом на сообщение
            target_id = message.reply_to_message.from_user.id
            target_username = message.reply_to_message.from_user.username
        elif message.text and (message.text.startswith('@') or message.text.isdigit()): # Если ввели ID/Username
            if message.text.startswith('@'):
                target_username = message.text[1:]
            else:
                try:
                    target_id = int(message.text)
                except ValueError:
                    await message.answer("Некорректный ID пользователя. Пожалуйста, введите числовой ID или @username.")
                    return
        else:
            await message.answer("Не удалось определить пользователя, на которого подается жалоба. Пожалуйста, ответьте на сообщение или введите ID/Username.")
            return

        # Если дошли сюда, значит цель определена
        sender_mention = await get_user_mention(message.from_user.id, message.from_user.username, message.from_user.first_name)
        
        # Отправляем сообщение пользователю
        sent_msg_user = await message.answer(
            f"Отправил: {sender_mention}\n"
            f"Причина: {reason}\n"
            f"Статус: 🟡 Ждет одобрения..",
            reply_markup=get_report_sent_kb(ADMIN_USERNAME)
        )
        
        report_id, report_time = await add_report(
            message.from_user.id,
            message.from_user.username,
            reason,
            target_id,
            target_username,
            sent_msg_user.message_id
        )
        await increment_user_reports(message.from_user.id)

        # Отправляем уведомление админу
        if ADMIN_ID: # Уведомление отправляем только если ADMIN_ID установлен
            target_mention_admin = await get_user_mention(target_id, target_username, "Неизвестный")
            await bot.send_message(
                ADMIN_ID,
                f"📩 **Новая жалоба!**\n"
                f"**Данные:**\n"
                f"👤 Username: {sender_mention}\n"
                f"🆔 ID: {message.from_user.id}\n"
                f"📄 Текст жалобы: {reason}\n"
                f"🎯 На цель: {target_mention_admin}\n"
                f"⏳ Время жалобы: {report_time.strftime('%d.%m.%Y | %H:%M:%S')}",
                parse_mode="HTML"
            )
        
        await state.clear() # Сбрасываем состояние
        return
    
    # Если сообщение не является ответом на запрос состояния, просто игнорируем его
    # или можно добавить какой-то дефолтный ответ
    await message.answer("Извините, я не понял вашу команду. Используйте кнопки.")


async def main():
    await init_db()
    logging.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
