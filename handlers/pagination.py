# handlers/pagination.py - Вспомогательные функции пагинации и выборок

from aiogram import types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from datetime import datetime, timedelta
import aiosqlite
import os
import logging

from database import get_user_db_path
from database import safe_json_loads
from database import get_scheduled_posts
from config import Config

logger = logging.getLogger(__name__)

# --- New helpers for admin users pagination ---

async def fetch_all_users():
    """Собирает список пользователей из всех БД.
    Возвращает список словарей: {user_id, username, subscription_end, days_left, is_banned, db_path}
    """
    users = []
    if not os.path.exists(Config.DB_DIR):
        return users
    for filename in os.listdir(Config.DB_DIR):
        if not filename.endswith('.db'):
            continue
        db_path = os.path.join(Config.DB_DIR, filename)
        try:
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute(
                    "SELECT telegram_user_id, telegram_username, subscription_end, is_banned FROM info LIMIT 1"
                )
                row = await cursor.fetchone()
                if not row:
                    continue
                u_id, uname, sub_end, is_banned = row
                days_left = 0
                if sub_end:
                    try:
                        end_dt = datetime.fromisoformat(sub_end)
                        days_left = (end_dt - datetime.now()).days
                    except Exception:
                        logger.exception("Ошибка парсинга даты подписки", extra={"db_path": db_path})
                        days_left = 0
                users.append({
                    'user_id': u_id,
                    'username': uname or str(u_id),
                    'subscription_end': sub_end,
                    'days_left': days_left,
                    'is_banned': bool(is_banned),
                    'db_path': db_path,
                })
        except Exception:
            logger.exception("Ошибка чтения БД пользователя", extra={"db_path": db_path})
            continue
    return users

async def display_users_paginated(callback: types.CallbackQuery, users: list, page: int = 0,
                                  items_per_page: int = 5, sort_type: str = 'alpha'):
    """Отображает список пользователей с пагинацией для админки."""
    if not users:
        await callback.message.edit_text(
            "👥 Пользователи не найдены",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]
            ])
        )
        return

    # Сортировка
    if sort_type == 'alpha':
        users = sorted(users, key=lambda u: (u['username'] or '').lower())
    elif sort_type == 'expiry':
        users = sorted(users, key=lambda u: u['days_left'])
    elif sort_type == 'banned':
        users = sorted(users, key=lambda u: (not u['is_banned'], u['username']))  # banned first

    total_pages = (len(users) - 1) // items_per_page
    page = max(0, min(page, total_pages))
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(users))

    text = f"👥 Пользователи (стр. {page + 1} из {total_pages + 1})\n\n"
    kb = InlineKeyboardBuilder()

    for i in range(start_idx, end_idx):
        u = users[i]
        status = "🚫 Забанен" if u['is_banned'] else "✅ Активен"
        days = u['days_left']
        title = f"{u['username']} | {days} д | {status}"
        kb.row(InlineKeyboardButton(
            text=title,
            callback_data=f"admin_user_{u['user_id']}_{page}_{sort_type}"
        ))

    # Навигация
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_users_page_{page-1}_{sort_type}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_users_page_{page+1}_{sort_type}"))
    if nav:
        kb.row(*nav)

    # Сортировка (разнесена в отдельное меню)
    kb.row(InlineKeyboardButton(text="⚙️ Сортировка", callback_data="admin_users_sort_menu"))
    kb.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu"))

    await callback.message.edit_text(text, reply_markup=kb.as_markup())

# Selection mode rendering
async def display_users_paginated_select(callback: types.CallbackQuery, users: list, page: int = 0,
                                         items_per_page: int = 5, sort_type: str = 'alpha',
                                         selected_ids: list[int] | None = None):
    if selected_ids is None:
        selected_ids = []
    if not users:
        await callback.message.edit_text(
            "👥 Пользователи не найдены",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_users")]
            ])
        )
        return

    # Сортировка
    if sort_type == 'alpha':
        users = sorted(users, key=lambda u: (u['username'] or '').lower())
    elif sort_type == 'expiry':
        users = sorted(users, key=lambda u: u['days_left'])
    elif sort_type == 'banned':
        users = sorted(users, key=lambda u: (not u['is_banned'], u['username']))

    total_pages = (len(users) - 1) // items_per_page
    page = max(0, min(page, total_pages))
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(users))

    text = f"👥 Выбор пользователей (стр. {page + 1} из {total_pages + 1})\nВыбрано: {len(selected_ids)}\n\n"
    kb = InlineKeyboardBuilder()

    for i in range(start_idx, end_idx):
        u = users[i]
        mark = "✅" if u['user_id'] in selected_ids else "➕"
        title = f"{mark} {u['username']}"
        kb.row(InlineKeyboardButton(
            text=title,
            callback_data=f"admin_user_toggle_{u['user_id']}_{page}_{sort_type}"
        ))

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_users_sel_page_{page-1}_{sort_type}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_users_sel_page_{page+1}_{sort_type}"))
    if nav:
        kb.row(*nav)

    kb.row(
        InlineKeyboardButton(text="🚫 Забанить выбранных", callback_data="admin_users_apply_ban_selected"),
        InlineKeyboardButton(text="✅ Разбанить выбранных", callback_data="admin_users_apply_unban_selected")
    )
    kb.row(
        InlineKeyboardButton(text="💳 +7 дней", callback_data="admin_bulk_license_7d"),
        InlineKeyboardButton(text="💳 +30 дней", callback_data="admin_bulk_license_30д")
    )
    kb.row(
        InlineKeyboardButton(text="♾ Бессрочно", callback_data="admin_bulk_license_forever"),
        InlineKeyboardButton(text="🗑 Удалить лицензию", callback_data="admin_bulk_license_delete")
    )
    kb.row(InlineKeyboardButton(text="◀️ Готово", callback_data="admin_users"))

    await callback.message.edit_text(text, reply_markup=kb.as_markup())

async def handle_admin_users_pagination(callback: types.CallbackQuery):
    data_parts = callback.data.split("_")
    page = int(data_parts[3])
    sort_type = data_parts[4] if len(data_parts) > 4 else 'alpha'
    users = await fetch_all_users()
    await display_users_paginated(callback, users, page, 5, sort_type)

async def handle_admin_users_select_mode(callback: types.CallbackQuery):
    await callback.answer()  # перерисовка в handlers.py

async def handle_admin_users_sel_page(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    page = int(parts[4])
    sort_type = parts[5] if len(parts) > 5 else 'alpha'
    users = await fetch_all_users()
    await callback.answer()  # handlers.py перерисует

async def handle_admin_users_sel_sort(callback: types.CallbackQuery):
    await callback.answer()  # перерисовка в handlers.py

async def get_all_user_channels(user_id: int, username: str):
    """Получение всех привязанных каналов пользователя"""
    db_path = await get_user_db_path(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT channel_id, channel_title FROM channels")
        channels = await cursor.fetchall()
    return channels

async def display_channels_paginated(callback: types.CallbackQuery, channels: list, page: int = 0,
                                     items_per_page: int = 5, title: str = "📋 Ваши каналы:",
                                     back_callback: str = "back_to_menu", sort_type: str = None):
    """Универсальная функция для отображения каналов с пагинацией"""
    if not channels:
        await callback.message.edit_text(
            "📋 У вас нет привязанных каналов",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=back_callback)]
            ])
        )
        return

    total_pages = (len(channels) - 1) // items_per_page
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(channels))

    text = f"{title}\n\n"
    for i in range(start_idx, end_idx):
        channel = channels[i]
        # Если сортировка по постам, структура другая: id, title, username, today_count, total_count
        if sort_type == "posts" and len(channel) >= 5:
            channel_id = channel[0]
            channel_title = channel[1]
            username_raw = channel[2]
            today_count = channel[3]
            total_count = channel[4]
        else:
            channel_id = channel[0]
            channel_title = channel[3] if len(channel) > 3 else channel[1]
            username_raw = channel[2] if len(channel) > 2 else None
            today_count = None
            total_count = None
        if username_raw:
            tag_part = f"@{username_raw}"
        else:
            tag_part = f"id: {channel_id}"
        channel_title = channel_title or "Без названия"
        text += f"{i + 1}. {channel_title} ({tag_part})\n"
        if sort_type == "posts":
            text += f"   📝 Сегодня: {today_count or 0} | Всего: {total_count or 0}\n"

    text += f"\n📄 Страница {page + 1} из {total_pages + 1} (всего каналов: {len(channels)})"

    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"channels_page_{page-1}_{sort_type or 'default'}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"channels_page_{page+1}_{sort_type or 'default'}"))
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back_callback)])

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )

async def handle_channels_pagination(callback: types.CallbackQuery):
    """Обработчик пагинации каналов"""
    data_parts = callback.data.split("_")
    page = int(data_parts[2])
    sort_type = data_parts[3] if len(data_parts) > 3 else None

    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)

    async with aiosqlite.connect(db_path) as db:
        if sort_type == "alpha":
            query = "SELECT * FROM channels ORDER BY channel_title"
            title = "📋 Ваши каналы (по алфавиту):"
        elif sort_type == "posts":
            query = (
                """
                SELECT 
                  c.channel_id,
                  c.channel_title,
                  c.channel_username,
                  COALESCE(today_cnt.cnt, 0) as today_count,
                  COALESCE(total_cnt.cnt, 0) as total_count
                FROM channels c
                LEFT JOIN (
                  SELECT channel_id, COUNT(*) as cnt
                  FROM posts
                  WHERE is_published = 0 AND date(scheduled_time) = date('now','localtime')
                  GROUP BY channel_id
                ) today_cnt ON today_cnt.channel_id = c.channel_id
                LEFT JOIN (
                  SELECT channel_id, COUNT(*) as cnt
                  FROM posts
                  WHERE is_published = 0
                  GROUP BY channel_id
                ) total_cnt ON total_cnt.channel_id = c.channel_id
                ORDER BY today_count DESC, total_count DESC
                """
            )
            title = "📋 Ваши каналы (по кол-ву постов сегодня):"
        else:
            query = "SELECT * FROM channels"
            title = "📋 Ваши каналы:"
        cursor = await db.execute(query)
        channels = await cursor.fetchall()

    await display_channels_paginated(callback, channels, page, 5, title, "list_channels", sort_type)

async def display_scheduled_posts_paginated(callback: types.CallbackQuery, posts: list, page: int = 0,
                                            items_per_page: int = 5, title: str = "📋 Запланированные посты:",
                                            back_callback: str = "scheduled_posts", post_type: str = "post"):
    """Универсальная функция для отображения запланированных постов с пагинацией"""
    if not posts:
        await callback.message.edit_text(
            f"📝 У вас нет запланированных постов",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data=back_callback)]
            ])
        )
        return

    total_pages = (len(posts) - 1) // items_per_page
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(posts))

    text = f"{title}\n\n"
    keyboard = InlineKeyboardBuilder()

    for i in range(start_idx, end_idx):
        post = posts[i]
        if post_type == "post":
            post_id, channel_id, channel_username, content_type, content, scheduled_time, is_periodic, period_hours, is_published, channel_title = post
            channel_name = channel_title or channel_username or f"Канал {channel_id}"
            try:
                scheduled_dt = datetime.fromisoformat(scheduled_time)
                formatted_time = scheduled_dt.strftime("%d.%m.%Y %H:%M")
            except Exception:
                logger.exception("Ошибка парсинга времени поста", extra={"scheduled_time": scheduled_time})
                formatted_time = "Неизвестно"
            button_text = f"📺 {channel_name} | {formatted_time}"
            keyboard.row(InlineKeyboardButton(text=button_text, callback_data=f"show_post_details_{post_id}"))
        elif post_type == "stream":
            stream_id, donor_channel, target_channels, phone_number, is_public_channel, post_freshness = post
            if target_channels and target_channels.startswith('['):
                target_channels_list = safe_json_loads(target_channels, [])
            else:
                target_channels_list = [int(cid.strip()) for cid in target_channels.split(',') if cid.strip()] if target_channels else []
            channels_count = len(target_channels_list) if isinstance(target_channels_list, list) else 0
            button_text = f"🔄 {donor_channel} → {channels_count} каналов"
            keyboard.row(InlineKeyboardButton(text=button_text, callback_data=f"show_stream_details_{stream_id}"))
        elif post_type == "random_stream_config":
            stream_id, donor_channels, target_channels, min_interval_hours, max_interval_hours, posts_per_day, post_freshness, is_active, last_post_time, phone_number, is_public_channel, next_post_times_json = post
            if not is_active:
                continue
            next_times = safe_json_loads(next_post_times_json, [])
            future_times = []
            current_time = datetime.now()
            now_buf = current_time + timedelta(seconds=60)
            for time_str in next_times:
                try:
                    post_time = datetime.fromisoformat(time_str)
                    if post_time > now_buf:
                        future_times.append(post_time)
                except Exception:
                    logger.exception("Ошибка парсинга времени random_stream_config", extra={"time_str": time_str})
                    continue
            if not future_times:
                continue
            user_id = callback.from_user.id
            username = callback.from_user.username or str(user_id)
            db_path = await get_user_db_path(user_id, username)
            donor_channels_list = safe_json_loads(donor_channels, [])
            target_channels_list = safe_json_loads(target_channels, [])
            donor_names = []
            async with aiosqlite.connect(db_path) as db_conn:
                for donor_val in donor_channels_list:
                    if isinstance(donor_val, int):
                        cursor = await db_conn.execute("SELECT channel_title FROM channels WHERE channel_id = ?", (donor_val,))
                        donor_row = await cursor.fetchone()
                        donor_names.append(donor_row[0] if donor_row and donor_row[0] else str(donor_val))
                    else:
                        donor_names.append(str(donor_val))
            target_names = []
            async with aiosqlite.connect(db_path) as db_conn:
                for target_val in target_channels_list:
                    if isinstance(target_val, int):
                        cursor = await db_conn.execute("SELECT channel_title FROM channels WHERE channel_id = ?", (target_val,))
                        target_row = await cursor.fetchone()
                        target_names.append(target_row[0] if target_row and target_row[0] else str(target_val))
                    else:
                        target_names.append(str(target_val))
            donor_text = ", ".join(donor_names) if donor_names else "Неизвестно"
            target_text = ", ".join(target_names) if target_names else "Неизвестно"
            next_post_count = len(future_times)
            button_text = f"🎲 {donor_text} → {len(target_channels_list) if isinstance(target_channels_list, list) else 0} каналов ({next_post_count} постов в будущем)"
            keyboard.row(InlineKeyboardButton(text=button_text, callback_data=f"show_random_post_details_{stream_id}"))
        elif post_type == "random_individual":
            post_id, channel_id, channel_username, content_type, content, scheduled_time, is_periodic, period_hours, is_published, channel_title, donor_channels_json, target_channels_json, post_freshness, phone_number, is_public_channel, random_post_id = post[:16]
            channel_name = channel_title or channel_username or f"Канал {channel_id}"
            try:
                scheduled_dt = datetime.fromisoformat(scheduled_time)
                formatted_time = scheduled_dt.strftime("%d.%m.%Y %H:%M")
            except Exception:
                logger.exception("Ошибка парсинга времени random_individual", extra={"scheduled_time": scheduled_time})
                formatted_time = "Неизвестно"
            text_row = f"🗓️ {formatted_time} | 📺 {channel_name}"
            if content_type == 'random' and content:
                text_row += f" | {content}"
            keyboard.row(InlineKeyboardButton(text=text_row, callback_data=f"show_post_details_{post_id}"))
        elif post_type == "random_stream":
            stream_id, donor_channels_json, target_channels_json, min_interval_hours, max_interval_hours, posts_per_day, post_freshness, is_active, last_post_time, phone_number, is_public_channel, next_post_times_json = post
            donor_channels_list = safe_json_loads(donor_channels_json, [])
            target_channels_list = safe_json_loads(target_channels_json, [])
            summary = f"🎲 Поток #{stream_id} | Доноры: {len(donor_channels_list) if isinstance(donor_channels_list, list) else 0} | Целей: {len(target_channels_list) if isinstance(target_channels_list, list) else 0} | {posts_per_day}/день"
            keyboard.row(InlineKeyboardButton(text=summary, callback_data=f"show_random_post_details_{stream_id}"))

    text += f"\n📄 Страница {page + 1} из {total_pages + 1} (всего потоков: {len(posts)})\n\n"
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"scheduled_posts_page_{page-1}_{post_type}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"scheduled_posts_page_{page+1}_{post_type}"))
    if nav_row:
        keyboard.row(*nav_row)
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data=back_callback))

    await callback.message.edit_text(text, reply_markup=keyboard.as_markup())

async def handle_scheduled_posts_pagination(callback: types.CallbackQuery):
    """Обработчик пагинации запланированных постов"""
    data_parts = callback.data.split("_")
    page = int(data_parts[3])
    post_type = data_parts[4]

    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)

    posts_data = await get_scheduled_posts(user_id, username)

    if post_type == "post":
        posts = posts_data['posts']
        title = "📝 Ваши единичные запланированные посты:"
    elif post_type == "stream":
        posts = posts_data['repost_streams']
        title = "🔄 Ваши потоки репостов:"
    elif post_type == "random_stream_config":
        posts = posts_data['old_random_posts']
        title = "🎲 Ваши рандомные публикации:"
    elif post_type == "random_stream":
        posts = posts_data['old_random_posts']
        title = "🎲 Ваши рандомные публикации:"
    else:
        posts = []
        title = "📋 Запланированные посты:"

    await display_scheduled_posts_paginated(
        callback, posts, page, 5, title, "scheduled_posts", post_type
    ) 