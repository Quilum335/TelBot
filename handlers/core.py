from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

async def admin_users_sort_menu(callback: 'types.CallbackQuery'):
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔤 По алфавиту", callback_data="admin_users_sort_alpha"))
    kb.row(InlineKeyboardButton(text="⏳ По сроку", callback_data="admin_users_sort_expiry"))
    kb.row(InlineKeyboardButton(text="🚫 Забаненные", callback_data="admin_users_sort_banned"))
    kb.row(InlineKeyboardButton(text="✅ Выбрать несколько", callback_data="admin_users_select_mode"))
    kb.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_users"))
    await callback.message.edit_text("⚙️ Сортировка пользователей:", reply_markup=kb.as_markup())

# handlers.py - Обработчики команд и callback'ов

from aiogram import types, F, Router, Bot
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove,
    KeyboardButtonRequestChat, ChatAdministratorRights
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.state import StatesGroup, State
from datetime import datetime, timedelta, date
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired, ChannelsAdminPublicTooMuch
from pyrogram.types import ChatPrivileges
import aiosqlite
import asyncio
import json
import logging
import os
import random
import re
import string
from functools import partial
from glob import glob
from random import choices
from typing import Any, Optional

# Локальные импорты
from config import Config
from database import (
    create_user_database, get_user_db_path, check_subscription, 
    get_scheduled_posts, delete_scheduled_post, update_post_donor, 
    user_database_exists, safe_json_loads, ensure_user_database
)
from states import PostStates, AdminStates, ChannelStates, AccountStates, ScheduledPostsStates
from keyboards import (
    get_main_menu_keyboard, get_post_type_keyboard, get_auto_post_keyboard,
    get_channel_sort_keyboard, get_admin_menu_keyboard, get_license_duration_keyboard,
    get_license_duration_keyboard_with_username, create_calendar,
    get_channel_name_method_keyboard, get_auto_source_keyboard, get_periodic_source_keyboard,
    get_post_freshness_keyboard, get_scheduled_posts_keyboard, get_post_action_keyboard,
    get_confirm_delete_keyboard, get_channel_type_keyboard, get_channel_count_keyboard,
    get_donor_type_keyboard, get_manage_binding_keyboard, get_accounts_menu_keyboard,
    get_accounts_list_keyboard, get_accounts_for_channels_keyboard,
    get_manage_channels_for_account_keyboard, get_channels_list_keyboard, get_manage_posts_keyboard,
    get_donor_count_keyboard, get_periodic_donor_count_keyboard, get_donors_confirm_keyboard
)
from utils import clean_post_content
from schu import PostScheduler
from .pagination import (
    get_all_user_channels as _get_all_user_channels,
    display_channels_paginated as _display_channels_paginated,
    handle_channels_pagination as _handle_channels_pagination,
    display_scheduled_posts_paginated as _display_scheduled_posts_paginated,
    handle_scheduled_posts_pagination as _handle_scheduled_posts_pagination,
    fetch_all_users as _fetch_all_users,
    display_users_paginated as _display_users_paginated,
    handle_admin_users_pagination as _handle_admin_users_pagination,
    display_users_paginated_select as _display_users_paginated_select,
)
from .selection import (
    toggle_id_in_state_list as _toggle_id_in_state_list,
    fetch_user_channels as _fetch_user_channels,
    render_select_list as _render_select_list,
)
# Новые импорты обработчиков привязки аккаунта
from auth_handler import link_account, process_phone, process_code, resend_code, process_password

logger = logging.getLogger(__name__)

# Safe-patch for Message.edit_text to ignore "message is not modified" errors
try:
    _orig_edit_text = types.Message.edit_text

    async def _safe_edit_text(self, *args, **kwargs):
        try:
            return await _orig_edit_text(self, *args, **kwargs)
        except TelegramBadRequest as e:
            try:
                msg = str(e)
            except Exception:
                msg = ''
            if 'message is not modified' in msg:
                # Игнорируем ошибку, когда содержимое/markup не изменились
                return None
            raise

    types.Message.edit_text = _safe_edit_text
except Exception:
    # В случае проблем — не ломаем приложение
    pass

# Глобальное хранилище клиентов для предотвращения пересоздания сессий
active_clients: dict[int, Client] = {}

# --- Async TTL Cache (замена простого словаря) ---
class AsyncTTLCache:
    def __init__(self, default_ttl: int = 300):
        self._data: dict[str, tuple[Any, float, int]] = {}
        self._default_ttl = default_ttl
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key not in self._data:
                return None
            value, ts, ttl = self._data[key]
            if (datetime.now().timestamp() - ts) < ttl:
                return value
            # expired
            del self._data[key]
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        async with self._lock:
            self._data[key] = (value, datetime.now().timestamp(), ttl or self._default_ttl)

    async def delete_prefix(self, prefix: str) -> None:
        async with self._lock:
            to_delete = [k for k in self._data.keys() if k.startswith(prefix)]
            for k in to_delete:
                del self._data[k]

_cache = AsyncTTLCache()

# Кэш утилиты

def get_cache_key(user_id: int, key: str) -> str:
    return f"user_{user_id}_{key}"

async def get_cached_data(user_id: int, key: str, ttl: int = 300):
    cache_key = get_cache_key(user_id, key)
    data = await _cache.get(cache_key)
    return data

async def set_cached_data(user_id: int, key: str, data, ttl: int = 300):
    cache_key = get_cache_key(user_id, key)
    await _cache.set(cache_key, data, ttl)

async def clear_user_cache(user_id: int):
    await _cache.delete_prefix(f"user_{user_id}_")

# Утилитарные функции для уменьшения дублирования кода
async def get_user_info(user_id: int, username: str):
    """Получение информации о пользователе с кэшированием"""
    cached = await get_cached_data(user_id, "user_info")
    if cached:
        return cached
    await create_user_database(user_id, username)
    db_path = await get_user_db_path(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            "SELECT subscription_end, is_banned FROM info WHERE telegram_user_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        if result:
            subscription_end = datetime.fromisoformat(result[0])
            is_banned = result[1]
            days_left = (subscription_end - datetime.now()).days
            trial_end = datetime.now() + timedelta(days=Config.TRIAL_DAYS)
            is_trial = subscription_end == trial_end
            user_info = {
                'has_subscription': days_left > 0 and not is_banned,
                'subscription_end': subscription_end,
                'days_left': days_left,
                'is_banned': is_banned,
                'is_trial': is_trial,
                'db_path': db_path
            }
            await set_cached_data(user_id, "user_info", user_info)
            return user_info
    return {
        'has_subscription': False,
        'subscription_end': None,
        'days_left': 0,
        'is_banned': False,
        'is_trial': False,
        'db_path': None
    }

async def check_user_access(user_id: int, username: str) -> tuple[bool, str]:
    user_info = await get_user_info(user_id, username)
    if user_info['is_banned']:
        return False, "❌ Ваш аккаунт заблокирован"
    if not user_info['has_subscription']:
        return False, f"❌ Ваша лицензия истекла\n💳 Для продления обратитесь к {Config.ADMIN_USERNAME}"
    return True, ""

async def create_client_session(user_id: int, session_string: str) -> Client:
    if user_id in active_clients:
        try:
            await active_clients[user_id].disconnect()
        except Exception:
            logger.exception("Ошибка при отключении предыдущего клиента Pyrogram")
        finally:
            active_clients.pop(user_id, None)
    session_name = os.path.join(Config.SESSIONS_DIR, f"user_{user_id}")
    client = Client(
        session_name,
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        session_string=session_string,
        in_memory=True
    )
    active_clients[user_id] = client
    return client

async def cleanup_inactive_clients():
    for user_id in list(active_clients.keys()):
        try:
            client = active_clients[user_id]
            if not client.is_connected:
                del active_clients[user_id]
        except Exception:
            logger.exception("Ошибка очистки неактивных клиентов")
            active_clients.pop(user_id, None)

async def send_error_message(message_or_callback, error_text: str, back_callback: str = "back_to_menu"):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data=back_callback)]
    ])
    if hasattr(message_or_callback, 'message'):
        try:
            await message_or_callback.message.edit_text(error_text, reply_markup=keyboard)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                await message_or_callback.message.answer(error_text, reply_markup=keyboard)
            else:
                raise
    else:
        await message_or_callback.answer(error_text, reply_markup=keyboard)

async def send_success_message(message_or_callback, success_text: str, back_callback: str = "back_to_menu"):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ В меню", callback_data=back_callback)]
    ])
    if hasattr(message_or_callback, 'message'):
        try:
            await message_or_callback.message.edit_text(success_text, reply_markup=keyboard)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                await message_or_callback.message.answer(success_text, reply_markup=keyboard)
            else:
                raise
    else:
        await message_or_callback.answer(success_text, reply_markup=keyboard)

class ChannelCreateStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_name_method = State()
    waiting_for_channel_name = State()
    waiting_for_generate_count = State()
    waiting_for_channel_type = State()
    waiting_for_channel_count = State()

class PostCreateStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_channel = State()
    waiting_for_content = State()
    waiting_for_mode = State()
    waiting_for_auto_donor = State()
    waiting_for_auto_targets = State()
    waiting_for_auto_confirm = State()

class AccountDeleteStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_confirm = State()

def register_handlers(dp, bot):
    """Регистрация всех обработчиков"""
    # Обработчики команд
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_menu, Command("menu"))
    dp.message.register(cmd_admin, Command("admin54"))
    
    # Обработчик добавления бота в канал
    dp.my_chat_member.register(on_bot_added_to_channel)
    
    # Получение выбранного канала через request_chat
    dp.message.register(on_chat_shared, F.chat_shared)
    
    # Обработчики callback
    dp.callback_query.register(back_to_menu, F.data == "back_to_menu")
    dp.callback_query.register(buy_license, F.data == "buy_license")
    dp.callback_query.register(create_post, F.data == "create_post")
    dp.callback_query.register(list_channels, F.data == "list_channels")
    dp.callback_query.register(post_manual, F.data == "post_manual")
    dp.callback_query.register(post_auto, F.data == "post_auto")
    dp.callback_query.register(auto_source_linked, F.data == "auto_source_linked")
    dp.callback_query.register(auto_source_public, F.data == "auto_source_public")
    dp.callback_query.register(public_auto_once, F.data == "public_auto_once")
    dp.callback_query.register(public_auto_periodic, F.data == "public_auto_periodic")
    dp.callback_query.register(link_channel, F.data == "link_channel")
    dp.callback_query.register(lambda c: select_link_channel(c, bot), F.data.startswith("select_link_channel_"))
    dp.callback_query.register(partial(sort_channels, bot=bot), F.data.startswith("sort_"))
    dp.callback_query.register(select_channel, F.data.startswith("select_channel_"))
    dp.callback_query.register(channels_selected, F.data == "channels_selected")
    dp.callback_query.register(admin_licenses_list, F.data == "admin_licenses")
    # NOTE: do not register a generic handler for prefixes 'admin_license_'
    # because it would intercept more specific license callbacks (like admin_license_extend_{user_id}).
    dp.callback_query.register(admin_license_by_username, F.data == "admin_license_by_username")
    dp.callback_query.register(process_license_duration_unified, F.data.startswith("license_"), AdminStates.waiting_for_license_duration)
    dp.callback_query.register(admin_users_list, F.data == "admin_users")
    dp.callback_query.register(admin_user_action, F.data.startswith("admin_user_"))
    # Confirm and execute ban/unban flows
    dp.callback_query.register(admin_confirm_ban, F.data.startswith("admin_confirm_ban_"))
    dp.callback_query.register(admin_do_ban, F.data.startswith("admin_do_ban_"))
    dp.callback_query.register(admin_confirm_unban, F.data.startswith("admin_confirm_unban_"))
    dp.callback_query.register(admin_do_unban, F.data.startswith("admin_do_unban_"))
    # License extend flows
    dp.callback_query.register(admin_license_extend, F.data.startswith("admin_license_extend_"))
    dp.callback_query.register(admin_confirm_license, F.data.startswith("admin_confirm_license_"))
    dp.callback_query.register(admin_do_license, F.data.startswith("admin_do_license_"))
    dp.callback_query.register(back_to_admin_menu, F.data == "admin_menu")
    dp.callback_query.register(exit_admin, F.data == "exit_admin")
    dp.callback_query.register(resend_code_removed, F.data == "resend_code")
    dp.callback_query.register(manage_accounts_removed, F.data == "manage_accounts")
    dp.callback_query.register(unlink_account_removed, F.data.startswith("unlink_account_"))
    dp.callback_query.register(admin_link_main_account_removed, F.data == "admin_link_main_account")
    dp.callback_query.register(create_channel_start, F.data == "create_channel_start")
    dp.callback_query.register(create_channel_account, F.data.startswith("create_channel_account_"))
    dp.callback_query.register(create_channel_type, F.data.startswith("channel_type_"))
    dp.callback_query.register(create_channel_count, F.data.startswith("channel_count_"))
    dp.message.register(process_channel_count_custom, ChannelCreateStates.waiting_for_generate_count)
    dp.callback_query.register(create_channel_name_method, F.data.in_(["channel_name_manual", "channel_name_random"]))
    dp.message.register(create_channel_name_input, ChannelCreateStates.waiting_for_channel_name)
    dp.callback_query.register(check_bot_admin, F.data.startswith("check_bot_admin_"))
    dp.callback_query.register(create_post_from_account, F.data == "create_post_from_account")
    dp.callback_query.register(post_account_select, F.data.startswith("post_account_"))
    dp.callback_query.register(post_channel_select, F.data.startswith("post_channel_"))
    dp.message.register(post_content_send, PostCreateStates.waiting_for_content)
    dp.callback_query.register(periodic_donor_select, F.data.startswith("periodic_donor_"))
    dp.callback_query.register(post_from_account_manual, F.data == "post_from_account_manual")
    dp.callback_query.register(post_from_account_auto, F.data == "post_from_account_auto")
    dp.callback_query.register(autoacc_donor_select, F.data.startswith("autoacc_donor_"))
    dp.callback_query.register(autoacc_target_select, F.data.startswith("autoacc_target_"))
    dp.callback_query.register(autoacc_targets_selected, F.data == "autoacc_targets_selected")
    dp.callback_query.register(public_once_target_select, F.data.startswith("public_once_target_"))
    dp.callback_query.register(public_periodic_target_select, F.data.startswith("public_periodic_target_"))
    dp.callback_query.register(public_periodic_targets_selected, F.data == "public_periodic_targets_selected")
    dp.callback_query.register(auto_random, F.data == "auto_random")
    dp.callback_query.register(auto_periodic, F.data == "auto_periodic")
    dp.callback_query.register(periodic_source_linked, F.data == "periodic_source_linked")
    dp.callback_query.register(periodic_source_public, F.data == "periodic_source_public")
    dp.callback_query.register(donor_count_one, F.data == "donor_count_one")
    dp.callback_query.register(donor_count_many, F.data == "donor_count_many")
    dp.callback_query.register(periodic_count_one, F.data == "periodic_count_one")
    dp.callback_query.register(periodic_count_many, F.data == "periodic_count_many")
    
    # Новые обработчики для рандомных постов
    dp.callback_query.register(random_donor_select, F.data.startswith("random_donor_"))
    dp.callback_query.register(random_target_select, F.data.startswith("random_target_"))
    dp.callback_query.register(random_donors_selected, F.data == "random_donors_selected")
    dp.callback_query.register(random_targets_selected, F.data == "random_targets_selected")
    dp.message.register(process_random_interval, PostStates.waiting_for_random_interval)
    dp.message.register(process_random_posts_per_day, PostStates.waiting_for_random_posts_per_day)
    
    # Обработчики состояний
    dp.message.register(process_post_time, PostStates.waiting_for_time)
    dp.callback_query.register(process_calendar, F.data.startswith(("date_", "month_", "ignore")), PostStates.waiting_for_date)
    dp.message.register(partial(process_post_content, bot=bot), PostStates.waiting_for_content)
    dp.message.register(process_public_channel_input, PostStates.waiting_for_public_channel_input)

    # Регистрация новых обработчиков
    dp.callback_query.register(channel_generate_one, F.data == "channel_generate_one")
    dp.callback_query.register(channel_generate_many, F.data == "channel_generate_many")
    dp.message.register(process_generate_count, ChannelCreateStates.waiting_for_generate_count)

    # Обработчики для рандомных постов из публичных каналов
    dp.callback_query.register(public_random_target_select, F.data.startswith("public_random_target_"))
    dp.callback_query.register(public_random_targets_selected, F.data == "public_random_targets_selected")

    dp.callback_query.register(process_post_freshness, F.data.startswith("freshness_"), PostStates.waiting_for_post_freshness)
    dp.callback_query.register(process_random_freshness, F.data.startswith("freshness_"), PostStates.waiting_for_random_freshness)

    # Обработчики для управления запланированными постами
    dp.callback_query.register(scheduled_posts, F.data == "scheduled_posts")
    dp.callback_query.register(scheduled_posts_single, F.data == "scheduled_posts_single")
    dp.callback_query.register(scheduled_posts_streams, F.data == "scheduled_posts_streams")
    dp.callback_query.register(scheduled_posts_random, F.data == "scheduled_posts_random")
    dp.callback_query.register(show_random_post_details, F.data.startswith("show_random_post_details_"))
    dp.callback_query.register(post_action, F.data.startswith("post_action_"))
    dp.callback_query.register(delete_post, F.data.startswith("delete_post_"))
    dp.callback_query.register(confirm_delete, F.data.startswith("confirm_delete_"))

    dp.callback_query.register(change_donor, F.data.startswith("change_donor_"))
    dp.message.register(process_new_donor, ScheduledPostsStates.waiting_for_new_donor)
    
    # Обработчики для пагинации каналов
    dp.callback_query.register(handle_channels_pagination, F.data.startswith("channels_page_"))
    
    # Обработчики для пагинации запланированных постов
    dp.callback_query.register(handle_scheduled_posts_pagination, F.data.startswith("scheduled_posts_page_"))
    
    # Обработчики для выбора каналов (кнопки "выбрать все" нет)
    
    # Обработчики для привязки аккаунта
    dp.message.register(process_username_by_admin, AdminStates.waiting_for_username_by_admin)
    
    # Обработчики для выбора типа поста в ручном режиме
    dp.callback_query.register(post_type_text, F.data == "post_type_text")
    dp.callback_query.register(post_type_channel, F.data == "post_type_channel")
    dp.callback_query.register(select_source_channel, F.data.startswith("select_source_channel_"))
    dp.callback_query.register(select_target_channel, F.data.startswith("select_target_channel_"))
    dp.callback_query.register(target_channels_selected, F.data == "target_channels_selected")
    
    dp.callback_query.register(scheduled_posts_random, F.data == "scheduled_posts_random")
    dp.callback_query.register(post_action, F.data.startswith("post_action_"))

    # Обработчики подтверждения создания постов
    dp.callback_query.register(confirm_create_periodic, F.data == "confirm_create_periodic", PostStates.waiting_for_confirm_periodic)
    dp.callback_query.register(confirm_create_random, F.data == "confirm_create_random", PostStates.waiting_for_confirm_random)
    dp.callback_query.register(confirm_create_single, F.data == "confirm_create_single")
    dp.callback_query.register(cancel_create_post, F.data == "cancel_create_post")

    # Дополнительный обработчик для лицензий с username в callback data
    # dp.callback_query.register(process_license_duration_unified, F.data.startswith("license_") & F.data.contains("_"), AdminStates.waiting_for_license_duration)

    # Кнопка "Все привязанные ТГК" удалена

    dp.callback_query.register(admin_users_management, F.data == "admin_users_management")
    dp.callback_query.register(admin_users_sorted_list, F.data.startswith("admin_users_sort_"))
    dp.callback_query.register(admin_users_sort_menu, F.data == "admin_users_sort_menu")
    dp.callback_query.register(handle_admin_users_pagination, F.data.startswith("admin_users_page_"))
    dp.callback_query.register(admin_users_select_mode, F.data == "admin_users_select_mode")
    dp.callback_query.register(admin_users_sel_toggle, F.data.startswith("admin_user_toggle_"))
    dp.callback_query.register(admin_users_sel_page, F.data.startswith("admin_users_sel_page_"))
    dp.callback_query.register(admin_users_apply_ban_selected, F.data == "admin_users_apply_ban_selected")
    dp.callback_query.register(admin_users_apply_unban_selected, F.data == "admin_users_apply_unban_selected")
    dp.callback_query.register(admin_bulk_license, F.data.startswith("admin_bulk_license_"))
    dp.callback_query.register(admin_user_action, F.data.startswith("admin_user_"))
    dp.callback_query.register(admin_quick_license, F.data.startswith("admin_license_quick_"))
    dp.message.register(process_admin_password, AdminStates.waiting_for_password)

    # Новые обработчики для выбора типа донора
    dp.callback_query.register(donor_type_linked, F.data == "donor_type_linked")
    dp.callback_query.register(donor_type_public, F.data == "donor_type_public")
    # Ввод публичных доноров текстом
    dp.message.register(process_public_random_donors, PostStates.waiting_for_random_donors)

    # Новые обработчики для выбора типа названий каналов
    dp.callback_query.register(channel_names_auto, F.data == "channel_names_auto")
    dp.callback_query.register(channel_names_manual, F.data == "channel_names_manual")

    # Обработчик для деталей потоков
    dp.callback_query.register(show_stream_details, F.data.startswith("show_stream_details_"))
    
    # Новый обработчик для показа всех запланированных постов
    # dp.callback_query.register(scheduled_posts_random, F.data == "scheduled_posts_random")
    # dp.callback_query.register(post_action, F.data.startswith("post_action_"))

    # Деталка единичного поста
    dp.callback_query.register(show_post_details, F.data.startswith("show_post_details_"))

    dp.callback_query.register(paginate_random_times, F.data.startswith("random_times_page_"))

    dp.callback_query.register(manage_binding_menu, F.data == "manage_binding")
    # Управлять аккаунтами
    dp.callback_query.register(manage_accounts_menu, F.data == "manage_accounts_menu")
    dp.callback_query.register(accounts_list, F.data == "accounts_list")
    dp.callback_query.register(unlink_account, F.data.startswith("unlink_account_"))
    dp.callback_query.register(link_account, F.data == "link_account")
    dp.message.register(process_phone, AccountStates.waiting_for_phone)
    dp.message.register(process_code, AccountStates.waiting_for_code)
    dp.message.register(process_password, AccountStates.waiting_for_password)
    # Управлять каналами
    dp.callback_query.register(manage_channels_menu, F.data == "manage_channels_menu")
    dp.callback_query.register(manage_channels_for_account, F.data.startswith("manage_channels_for_"))
    dp.callback_query.register(create_channels_for_account, F.data.startswith("create_channels_for_"))
    dp.callback_query.register(delete_channels_for_account, F.data.startswith("delete_channels_for_"))
    dp.callback_query.register(delete_channel_for_account, F.data.startswith("delete_channel_"))
    # Управлять постами
    dp.callback_query.register(manage_posts_menu, F.data == "manage_posts_menu")

# Обработчик добавления бота в канал
async def on_bot_added_to_channel(update: types.ChatMemberUpdated):
    """Обработка добавления бота в канал"""
    if update.chat.type == "channel" and update.new_chat_member.status in ["administrator", "creator"]:
        user_id = update.from_user.id
        username = update.from_user.username or str(user_id)
        channel_id = update.chat.id
        channel_title = update.chat.title
        channel_username = update.chat.username or ""
        
        db_path = await get_user_db_path(user_id, username)
        
        try:
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute(
                    "SELECT id FROM channels WHERE channel_id = ?",
                    (channel_id,)
                )
                existing = await cursor.fetchone()
                
                if not existing:
                    await db.execute('''
                        INSERT INTO channels (channel_id, channel_username, channel_title)
                        VALUES (?, ?, ?)
                    ''', (channel_id, channel_username, channel_title))
                    await db.commit()
                    
                    await update.bot.send_message(
                        user_id,
                        f"✅ Канал '{channel_title}' успешно привязан!\n"
                        f"Теперь вы можете создавать отложенные посты для этого канала.",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="📝 Создать пост", callback_data="create_post")],
                            [InlineKeyboardButton(text="📱 В меню", callback_data="back_to_menu")]
                        ])
                    )
        except Exception:
            logger.exception("Ошибка при добавлении канала")

async def on_chat_shared(message: types.Message):
    """Обработка выбранного канала из системного выбора Telegram (request_chat)."""
    if not getattr(message, 'chat_shared', None):
        return
    channel_id = message.chat_shared.chat_id
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    # Ensure DB initialized
    await ensure_user_database(user_id, username)
    # Попробуем получить сведения о канале через Bot API
    channel_username = ''
    channel_title = ''
    try:
        chat = await message.bot.get_chat(channel_id)
        channel_title = chat.title or ''
        if getattr(chat, 'username', None):
            channel_username = chat.username
    except Exception:
        pass
    # Ensure DB initialized before accessing tables
    await ensure_user_database(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT id FROM channels WHERE channel_id = ?", (channel_id,))
        row = await cursor.fetchone()
        if not row:
            await db.execute(
                "INSERT INTO channels (channel_id, channel_username, channel_title) VALUES (?, ?, ?)",
                (channel_id, channel_username, channel_title)
            )
        else:
            await db.execute(
                "UPDATE channels SET channel_username = COALESCE(NULLIF(?, ''), channel_username), channel_title = COALESCE(NULLIF(?, ''), channel_title) WHERE channel_id = ?",
                (channel_username, channel_title, channel_id)
            )
        await db.commit()
    await message.answer("✅ Канал привязан! Откройте '📋 Список каналов' для проверки.", reply_markup=ReplyKeyboardRemove())

# Обработчики команд
async def cmd_start(message: types.Message):
    """Обработка команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    
    # Проверяем доступ пользователя
    has_access, error_message = await check_user_access(user_id, username)
    if not has_access:
        await message.answer(error_message)
        return
    
    # Получаем информацию о лицензии
    user_info = await get_user_info(user_id, username)
    
    welcome_text = (
        "🎉 Добро пожаловать в Telegram Post Bot!\n\n"
        "🤖 Этот бот поможет вам:\n"
        "• Создавать и планировать посты\n"
        "• Автоматически репостить контент\n"
        "• Управлять множественными каналами\n\n"
    )
    
    if user_info['days_left'] > 0:
        welcome_text += f"✅ Ваша лицензия активна еще {user_info['days_left']} дней"
    else:
        welcome_text += "❌ Лицензия истекла"
    
    await message.answer(welcome_text, reply_markup=get_main_menu_keyboard(user_info))

async def cmd_menu(message: types.Message):
    """Обработка команды /menu"""
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    
    # Проверяем доступ пользователя
    has_access, error_message = await check_user_access(user_id, username)
    if not has_access:
        await message.answer(error_message)
        return
    
    # Получаем информацию о лицензии
    user_info = await get_user_info(user_id, username)
    
    await message.answer(
        "📋 Главное меню",
        reply_markup=get_main_menu_keyboard(user_info)
    )

# (удалено) обработчик любого текста

async def cmd_admin(message: types.Message, state: FSMContext):
    """Обработка команды /admin54"""
    user_id = message.from_user.id
    
    # Убираем проверку на ADMIN_IDS, чтобы дать доступ всем
    await state.set_state(AdminStates.waiting_for_password)
    await send_success_message(message, "🔐 Введите пароль для доступа к админ-панели:", back_callback="back_to_menu")

# Обработчики callback
async def back_to_menu(callback: types.CallbackQuery):
    """Возврат в главное меню"""
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    # Проверяем доступ пользователя
    has_access, error_message = await check_user_access(user_id, username)
    if not has_access:
        await send_error_message(callback, error_message)
        return
    # Получаем информацию о лицензии
    user_info = await get_user_info(user_id, username)
    try:
        await callback.message.edit_text(
            "📋 Главное меню",
            reply_markup=get_main_menu_keyboard(user_info)
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            await callback.message.answer(
                "📋 Главное меню",
                reply_markup=get_main_menu_keyboard(user_info)
            )
        else:
            raise

async def buy_license(callback: types.CallbackQuery):
    """Покупка лицензии"""
    await send_success_message(callback, "💳 Для покупки лицензии обратитесь к @CEKYHDA\n\n", back_callback="back_to_menu")

async def create_post(callback: types.CallbackQuery):
    """Создание отложенного поста"""
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    
    if not await check_subscription(user_id, username):
        await callback.answer(
            "❌ У вас закончилась подписка! Купите лицензию для продолжения.",
            show_alert=True
        )
        await send_error_message(callback, f"💳 Для продолжения работы необходимо купить лицензию у {Config.ADMIN_USERNAME}", back_callback="back_to_menu")
        return
    
    await callback.message.edit_text(
        "📝 Выберите режим создания поста:",
        reply_markup=get_post_type_keyboard()
    )

async def list_channels(callback: types.CallbackQuery):
    """Список каналов"""
    await callback.message.edit_text(
        "📋 Выберите способ сортировки каналов:",
        reply_markup=get_channel_sort_keyboard()
    )

# Обработчики для создания постов
async def post_manual(callback: types.CallbackQuery, state: FSMContext):
    """Ручной режим создания поста"""
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    await callback.message.edit_text(
        "⏰ Введите время публикации в формате HH:MM (например, 13:32):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")]
        ])
    )
    await state.set_state(PostStates.waiting_for_time)

async def process_post_time(message: types.Message, state: FSMContext):
    """Обработка времени поста"""
    try:
        time_parts = message.text.strip().split(":")
        if len(time_parts) != 2:
            raise ValueError
        hour = int(time_parts[0])
        minute = int(time_parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        await state.update_data(post_time=message.text, post_hour=hour, post_minute=minute)
        now = datetime.now()
        await message.answer(
            "📅 Выберите дату публикации:",
            reply_markup=create_calendar(now.year, now.month)
        )
        await state.set_state(PostStates.waiting_for_date)
    except ValueError:
        await message.answer("❌ Неверный формат времени. Введите время в формате HH:MM")

async def process_calendar(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора даты в календаре"""
    await callback.answer("Календарь обработан!")
    data = callback.data.split("_")
    if data[0] == "ignore":
        await callback.answer()
        return
    elif data[0] == "month":
        year = int(data[1])
        month = int(data[2])
        await callback.message.edit_reply_markup(
            reply_markup=create_calendar(year, month)
        )
    elif data[0] == "date":
        try:
            date_str = data[1]  # формат: YYYY-MM-DD
            year, month, day = map(int, date_str.split("-"))
        except (IndexError, ValueError):
            await callback.answer("❌ Ошибка при обработке даты", show_alert=True)
            return
        state_data = await state.get_data()
        post_hour = state_data.get("post_hour", 0)
        post_minute = state_data.get("post_minute", 0)
        now = datetime.now()
        selected_date = date(year, month, day)
        today = now.date()
        if selected_date < today:
            await callback.answer(
                "❌ Нельзя запланировать пост на прошедшую дату!",
                show_alert=True
            )
            return
        if selected_date == today:
            selected_time = datetime(year, month, day, post_hour, post_minute)
            if selected_time <= now:
                await callback.answer(
                    "❌ Нельзя запланировать пост на прошедшее время!",
                    show_alert=True
                )
                return
        await state.update_data(post_date=f"{year}-{month:02d}-{day:02d}")
        data_state = await state.get_data()
        post_time = data_state.get("post_time", "00:00")
        post_date = data_state.get("post_date", f"{year}-{month:02d}-{day:02d}")
        full_datetime = f"{post_date} {post_time}"
        if data_state.get("is_repost"):
            user_id = callback.from_user.id
            username = callback.from_user.username or str(user_id)
            db_path = await get_user_db_path(user_id, username)
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute("SELECT channel_id, channel_title FROM channels")
                channels = await cursor.fetchall()
            if not channels:
                await send_error_message(callback, "❌ У вас нет привязанных каналов.", back_callback="back_to_menu")
                await state.clear()
                return
            await _render_select_list(
                callback,
                items=channels,
                selected_ids=[],
                build_callback_prefix="select_channel",
                title_text="📢 Выберите каналы для публикации:",
                done_callback="channels_selected",
                back_callback="create_post",
            )
            await state.set_state(PostStates.waiting_for_channel)
            await state.update_data(selected_channels=[])
            return
        if data_state.get("is_periodic"):
            donor_channel = data_state.get("donor_channel")
            selected_channels = data_state.get("selected_channels", [])
            await callback.message.edit_text(
                f"📅 Выберите свежесть постов для репостинга:\n\n"
                f"📤 Донор: {donor_channel}\n"
                f"📥 Каналы: {', '.join(str(cid) for cid in selected_channels)}\n\n"
                f"• Система будет репостить только посты указанной свежести\n"
                f"• Старые посты будут игнорироваться",
                reply_markup=get_post_freshness_keyboard()
            )
            await state.set_state(PostStates.waiting_for_post_freshness)
            return
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(text="📝 Написать текст поста", callback_data="post_type_text"),
            InlineKeyboardButton(text="📤 Выбрать из канала", callback_data="post_type_channel")
        )
        keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="create_post"))
        await callback.message.edit_text(
            "📝 Выберите тип поста:",
            reply_markup=keyboard.as_markup()
        )
        await state.set_state(PostStates.waiting_for_content)

async def process_post_content(message: types.Message, state: FSMContext, bot: Bot):
    """Обработка контента поста"""
    data = await state.get_data()
    post_type = data.get("post_type", "text")
    
    if post_type == "text":
        # Обработка текстового поста
        content_type = "text"
        content = ""
        media_id = None
        
        if message.text:
            content_type = "text"
            content = message.text
        elif message.photo:
            content_type = "photo"
            content = message.caption or ""
            media_id = message.photo[-1].file_id
        elif message.video:
            content_type = "video"
            content = message.caption or ""
            media_id = message.video.file_id
        
        # Сохраняем контент поста
        await state.update_data(
            content_type=content_type,
            content=content,
            media_id=media_id
        )
        
        # Запрашиваем выбор каналов для публикации
        user_id = message.from_user.id
        username = message.from_user.username or str(user_id)
        db_path = await get_user_db_path(user_id, username)
        
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT channel_id, channel_title FROM channels")
            channels = await cursor.fetchall()
        
        if not channels:
            await send_error_message(message, "❌ У вас нет привязанных каналов.", back_callback="back_to_menu")
            await state.clear()
            return
        
        # Унифицированная отрисовка списка выбора каналов
        await _render_select_list(
            message,
            items=channels,
            selected_ids=[],
            build_callback_prefix="select_target_channel",
            title_text="📢 Выберите каналы для публикации:",
            done_callback="target_channels_selected",
            back_callback="create_post",
        )
        await state.set_state(PostStates.waiting_for_channel)
        await state.update_data(selected_channels=[])
        
    elif post_type == "channel":
        # Обработка поста из канала
        if not message.text:
            await message.answer("❌ Пожалуйста, введите ID поста (число)")
            return
        
        try:
            post_id = int(message.text)
        except ValueError:
            await message.answer("❌ ID поста должен быть числом")
            return
        
        # Сохраняем ID поста
        await state.update_data(source_post_id=post_id)
        
        # Запрашиваем выбор каналов для публикации
        user_id = message.from_user.id
        username = message.from_user.username or str(user_id)
        db_path = await get_user_db_path(user_id, username)
        
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT channel_id, channel_title FROM channels")
            channels = await cursor.fetchall()
        
        if not channels:
            await send_error_message(message, "❌ У вас нет привязанных каналов.", back_callback="back_to_menu")
            await state.clear()
            return
        
        source_channel_title = data.get("source_channel_title", "Неизвестный канал")
        await _render_select_list(
            message,
            items=channels,
            selected_ids=[],
            build_callback_prefix="select_target_channel",
            title_text=(
                f"📤 Репост поста #{post_id} из канала {source_channel_title}\n\n"
                f"📢 Выберите каналы для публикации:"
            ),
            done_callback="target_channels_selected",
            back_callback="create_post",
        )
        await state.set_state(PostStates.waiting_for_channel)
        await state.update_data(selected_channels=[])

async def process_public_channel_input(message: types.Message, state: FSMContext):
    """Прием ввода публичного канала от пользователя."""
    channel = (message.text or "").strip()
    if not channel:
        await message.answer("❌ Введите @username или ссылку на канал")
        return
    # Нормализуем username/ссылку
    if channel.startswith("http"):
        try:
            username = channel.split("/")[-1]
            # Отрежем параметры типа ?start=... и фрагменты
            username = username.split("?")[0].split("#")[0]
            if not username.startswith("@"):
                channel = f"@{username}"
            else:
                channel = username
        except Exception:
            pass
    elif not channel.startswith("@") and not channel.lstrip("-").isdigit():
        channel = f"@{channel}"
    # Убираем дубли @ и лишние части, берём последний сегмент
    if "@" in channel[1:]:
        channel = "@" + channel.split("@")[-1]
    # Финальная чистка допустимых символов username
    m = re.search(r"@([A-Za-z0-9_]{3,})", channel)
    if m:
        channel = f"@{m.group(1)}"
 
    await state.update_data(public_channel=channel)

    data = await state.get_data()
    if data.get("periodic_flow") == "public":
        # После ввода донора предлагаем выбрать целевые каналы
        user_id = message.from_user.id
        username = message.from_user.username or str(user_id)
        channels = await _fetch_user_channels(user_id, username)
        if not channels:
            await message.answer("❌ У вас нет привязанных каналов. Сначала привяжите каналы в меню.")
            await state.clear()
            return
        await _render_select_list(
            message,
            items=channels,
            selected_ids=[],
            build_callback_prefix="public_periodic_target",
            title_text=(
                "📥 Выберите целевые каналы (можно несколько):\n\n"
                f"📡 Донор: {channel}"
            ),
            done_callback="public_periodic_targets_selected",
            back_callback="auto_periodic",
        )
        await state.set_state(PostStates.waiting_for_auto_targets)
        await state.update_data(selected_targets=[])
    else:
        # Если это не шаг настройки потока — вернемся в меню
        user_id = message.from_user.id
        username = message.from_user.username or str(user_id)
        user_info = await get_user_info(user_id, username)
        await message.answer("✅ Готово", reply_markup=get_main_menu_keyboard(user_info))
        await state.clear()

async def post_type_text(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(post_type="text")
    await callback.message.edit_text("✍️ Отправьте текст поста или медиа с подписью")
    await state.set_state(PostStates.waiting_for_content)

async def post_type_channel(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(post_type="channel")
    await callback.message.edit_text("🔢 Введите ID поста (число) из исходного канала")
    await state.set_state(PostStates.waiting_for_content)

async def select_target_channel(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    try:
        parts = callback.data.split("_")
        if parts[-1] == 'all':
            return await select_all_channels_toggle(callback, state)
        channel_id = int(parts[-1])
    except Exception:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    selected_ids, _ = await _toggle_id_in_state_list(state, 'selected_channels', channel_id)
    channels = await _fetch_user_channels(user_id, username)
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=selected_ids,
        build_callback_prefix="select_target_channel",
        title_text="📢 Выберите каналы для публикации:",
        done_callback="target_channels_selected",
        back_callback="create_post",
    )

async def target_channels_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_channels: list[int] = data.get('selected_channels', []) or []
    if not selected_channels:
        await callback.answer("Выберите хотя бы один канал", show_alert=True)
        return
    # Build scheduled datetime
    post_time: str = data.get('post_time')  # HH:MM
    post_date: str = data.get('post_date')  # YYYY-MM-DD
    if not post_time or not post_date:
        await send_error_message(callback, "Не задана дата/время публикации", back_callback="create_post")
        return
    try:
        scheduled_dt = datetime.fromisoformat(f"{post_date} {post_time}")
    except Exception:
        await send_error_message(callback, "Некорректная дата/время", back_callback="create_post")
        return
    # Use ISO format so string comparisons with datetime.now().isoformat() are correct
    scheduled_str = scheduled_dt.isoformat()
    # Collect content
    content_type = data.get('content_type') or ('repost' if data.get('post_type') == 'channel' else 'text')
    content = data.get('content', '') or ''
    media_id = data.get('media_id')
    # For repost from channel, compose content as marker: repost_{source_channel_id}_{source_post_id}
    if content_type == 'repost':
        source_channel_id = data.get('source_channel_id')
        source_post_id = data.get('source_post_id')
        if not source_channel_id or not source_post_id:
            await send_error_message(callback, "Для репоста необходимо выбрать исходный канал и ID поста", back_callback="create_post")
            return
        content = f"repost_{int(source_channel_id)}_{int(source_post_id)}"
        media_id = None
    # Insert posts for each selected channel
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        for channel_id in selected_channels:
            await db.execute(
                """
                INSERT INTO posts (
                    channel_id, channel_username, content_type, content, media_id, scheduled_time,
                    is_periodic, period_hours, is_published, last_post_time
                ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, 0, NULL)
                """,
                (
                    channel_id,
                    '',
                    content_type,
                    content,
                    media_id,
                    scheduled_str
                )
            )
        await db.commit()
    await state.clear()
    await send_success_message(callback, "✅ Пост(ы) запланированы", back_callback="scheduled_posts")

async def channel_names_auto(callback: types.CallbackQuery, state: FSMContext):
    """Автоматическая генерация названий каналов"""
    data = await state.get_data()
    channel_count = data.get("channel_count", 1)
    
    # Генерируем автоматические названия
    if channel_count == 1:
        names = ["TG_" + ''.join(choices(string.ascii_uppercase + string.digits, k=8))]
    else:
        # Генерируем разные названия для каждого канала
        names = []
        for i in range(channel_count):
            name = "TG_" + ''.join(choices(string.ascii_uppercase + string.digits, k=8))
            names.append(name)
    
    await state.update_data(channel_names=names, channel_create_pending=True)
    
    # Если тип уже выбран — сразу создаём каналы, иначе предложим выбрать тип
    data = await state.get_data()
    if data.get("channel_type"):
        # Можно вызывать с callback или message
        target = callback.message if hasattr(callback, 'message') else callback
        await create_channel_do(target, state)
    else:
        if hasattr(callback, 'message'):
            await callback.message.edit_text(
                f"Названий подготовлено: {channel_count}. Выберите тип каналов:",
                reply_markup=get_channel_type_keyboard()
            )
        else:
            await callback.answer(
                f"Названий подготовлено: {channel_count}. Выберите тип каналов:"
            )

async def channel_names_manual(callback: types.CallbackQuery, state: FSMContext):
    """Ручной ввод названий каналов"""
    data = await state.get_data()
    channel_count = data.get("channel_count", 1)
    
    if channel_count == 1:
        await callback.message.edit_text("Введите название канала:")
    else:
        await callback.message.edit_text(
            f"Введите названия для {channel_count} каналов через запятую:\n"
            f"Например: Канал 1, Канал 2, Канал 3"
        )
    
    await state.set_state(ChannelCreateStates.waiting_for_channel_name)

async def show_stream_details(callback: types.CallbackQuery, state: FSMContext):
    """Отображение деталей потока репостов"""
    stream_id = int(callback.data.split("_")[-1])
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute('''
            SELECT donor_channel, target_channels, last_message_id, phone_number, is_public_channel, post_freshness
            FROM repost_streams
            WHERE id = ?
        ''', (stream_id,))
        stream = await cursor.fetchone()
        
        if not stream:
            await callback.answer("❌ Поток не найден", show_alert=True)
            return
        
        donor_channel, target_channels, last_message_id, phone_number, is_public_channel, post_freshness = stream
        
        # Парсим целевые каналы
        if target_channels and target_channels.startswith('['):
            target_channels_list = safe_json_loads(target_channels, [])
        else:
            target_channels_list = [int(cid.strip()) for cid in target_channels.split(',') if cid.strip()] if target_channels else []
        
        # Получаем названия целевых каналов
        target_names = []
        for target_id in target_channels_list:
            cursor = await db.execute("SELECT channel_title FROM channels WHERE channel_id = ?", (target_id,))
            result = await cursor.fetchone()
            target_names.append(result[0] if result else str(target_id))
        
        # Формируем текст с информацией о потоке
        text = f"🔄 Детали потока репостов #{stream_id}\n\n"
        text += f"📡 Донор: {donor_channel}\n"
        text += f"📊 Целевые каналы ({len(target_names)}):\n"
        
        for i, name in enumerate(target_names, 1):
            text += f"  {i}. {name}\n"
        
        text += f"🌐 Тип донора: {'Публичный канал' if is_public_channel else 'Привязанный канал'}\n"

        
        if last_message_id:
            text += f"📝 Пос: {last_message_id}\n"
        
        from keyboards import get_post_action_keyboard
        await callback.message.edit_text(
            text,
            reply_markup=get_post_action_keyboard(stream_id, 'repost_stream')
        )

async def admin_users_management(callback: types.CallbackQuery):
    # Simplified admin management menu (kept for compatibility)
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="👥 Список пользователей", callback_data="admin_users"))
    kb.row(InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_to_menu"))
    await callback.message.edit_text("👥 Управление пользователями:", reply_markup=kb.as_markup())

async def admin_users_list(callback: types.CallbackQuery):
    users = await _fetch_all_users()
    # reuse pagination helper; default items_per_page set to 10 per request
    await _display_users_paginated(callback, users, page=0, items_per_page=10, sort_type='alpha')

async def admin_users_sorted_list(callback: types.CallbackQuery):
    sort_type = callback.data.split("_")[-1]
    users = await _fetch_all_users()
    await _display_users_paginated(callback, users, page=0, items_per_page=5, sort_type=sort_type)

async def admin_users_select_mode(callback: types.CallbackQuery, state: FSMContext):
    users = await _fetch_all_users()
    await state.update_data(admin_selected_users=[])
    await _display_users_paginated_select(callback, users, page=0, items_per_page=5, sort_type='alpha', selected_ids=[])

async def admin_users_sel_page(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    page = int(parts[3])
    sort_type = parts[4] if len(parts) > 4 else 'alpha'
    users = await _fetch_all_users()
    data = await state.get_data()
    selected = data.get('admin_selected_users', [])
    await _display_users_paginated_select(callback, users, page=page, items_per_page=5, sort_type=sort_type, selected_ids=selected)

async def admin_users_sel_toggle(callback: types.CallbackQuery, state: FSMContext):
    # admin_user_toggle_{user_id}_{page}_{sort}
    parts = callback.data.split("_")
    user_id = int(parts[3])
    page = int(parts[4]) if len(parts) > 4 else 0
    sort_type = parts[5] if len(parts) > 5 else 'alpha'
    data = await state.get_data()
    selected: list[int] = data.get('admin_selected_users', [])
    if user_id in selected:
        selected.remove(user_id)
    else:
        selected.append(user_id)
    await state.update_data(admin_selected_users=selected)
    users = await _fetch_all_users()
    await _display_users_paginated_select(callback, users, page=page, items_per_page=5, sort_type=sort_type, selected_ids=selected)

async def admin_users_apply_ban_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected: list[int] = data.get('admin_selected_users', [])
    if not selected:
        await callback.answer("Выберите пользователей", show_alert=True)
        return
    users = await _fetch_all_users()
    for u in users:
        if u['user_id'] in selected:
            async with aiosqlite.connect(u['db_path']) as db:
                await db.execute("UPDATE info SET is_banned = 1 WHERE telegram_user_id = ?", (u['user_id'],))
                await db.commit()
    await send_success_message(callback, "✅ Выбранные пользователи забанены", back_callback="admin_users")
    await state.update_data(admin_selected_users=[])

async def admin_users_apply_unban_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected: list[int] = data.get('admin_selected_users', [])
    if not selected:
        await callback.answer("Выберите пользователей", show_alert=True)
        return
    users = await _fetch_all_users()
    for u in users:
        if u['user_id'] in selected:
            async with aiosqlite.connect(u['db_path']) as db:
                await db.execute("UPDATE info SET is_banned = 0 WHERE telegram_user_id = ?", (u['user_id'],))
                await db.commit()
    await send_success_message(callback, "✅ Выбранные пользователи разбанены", back_callback="admin_users")
    await state.update_data(admin_selected_users=[])

async def admin_bulk_license(callback: types.CallbackQuery, state: FSMContext):
    # admin_bulk_license_{7d|30d|forever|delete}
    action = callback.data.split("_")[-1]
    data_state = await state.get_data()
    selected: list[int] = data_state.get('admin_selected_users', [])
    if not selected:
        await callback.answer("Выберите пользователей", show_alert=True)
        return
    duration_map = {'7d': 7, '30d': 30}
    now = datetime.now()
    users = await _fetch_all_users()
    for u in users:
        if u['user_id'] not in selected:
            continue
        if action == 'forever':
            new_end = datetime(2100, 1, 1)
        elif action == 'delete':
            new_end = now
        else:
            days = duration_map.get(action)
            if not days:
                continue
            new_end = now + timedelta(days=days)
        async with aiosqlite.connect(u['db_path']) as db:
            await db.execute("UPDATE info SET subscription_end = ? WHERE telegram_user_id = ?", (new_end.isoformat(), u['user_id']))
            await db.commit()
    await send_success_message(callback, "✅ Лицензии обновлены", back_callback="admin_users")
    await state.update_data(admin_selected_users=[])
 
# --- Лицензии: минимальные обработчики ---
async def admin_licenses_list(callback: types.CallbackQuery):
    # Deprecated: direct license management via standalone menu is removed.
    # Prefer using per-user "Продлить лицензию" flow from the users list.
    await callback.message.edit_text("💳 Управление лицензиями:\n\nИспользуйте список пользователей и кнопку 'Продлить лицензию' для конкретного пользователя.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_menu")]]))

async def admin_license_by_username(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_username_by_admin)
    await callback.message.edit_text("Введите @username пользователя:")

async def process_username_by_admin(message: types.Message, state: FSMContext):
    username = (message.text or "").strip().lstrip("@")
    if not username:
        await message.answer("❌ Укажите корректный username")
        return
    from keyboards import get_license_duration_keyboard_with_username
    await state.set_state(AdminStates.waiting_for_license_duration)
    await message.answer(
        f"Выберите длительность лицензии для @{username}:",
        reply_markup=get_license_duration_keyboard_with_username(username)
    )

async def admin_license_action(callback: types.CallbackQuery):
    # Ранее эта функция была заглушкой. Теперь перенаправляем в список управления лицензиями.
    await admin_licenses_list(callback)

async def process_license_duration_unified(callback: types.CallbackQuery, state: FSMContext):
    """Обработка выбора длительности лицензии с username в callback data."""
    try:
        parts = callback.data.split("_")  # e.g., license_30d_username
        if len(parts) < 2:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        action = parts[1]
        username = parts[2] if len(parts) > 2 else None

        # datetime, timedelta, aiosqlite, os, Config импортированы вверху файла

        if not username:
            await callback.answer("Требуется username", show_alert=True)
            return

        # Нормализуем
        username = username.lstrip('@')

        # Определяем новое значение окончания подписки
        now = datetime.now()
        if action == 'delete':
            new_end = now
        elif action == 'forever':
            new_end = datetime(2100, 1, 1)
        else:
            suffix_to_days = {
                '1d': 1,
                '7d': 7,
                '14d': 14,
                '30d': 30,
                '365d': 365,
            }
            days = suffix_to_days.get(action)
            if not days:
                await callback.answer("Неизвестная длительность", show_alert=True)
                return
            new_end = now + timedelta(days=days)

        # Ищем БД пользователя по username и обновляем
        updated = False
        if os.path.exists(Config.DB_DIR):
            for filename in os.listdir(Config.DB_DIR):
                if not filename.endswith('.db'):
                    continue
                db_path = os.path.join(Config.DB_DIR, filename)
                try:
                    async with aiosqlite.connect(db_path) as db:
                        cursor = await db.execute(
                            "SELECT telegram_user_id FROM info WHERE telegram_username = ? LIMIT 1",
                            (username,)
                        )
                        row = await cursor.fetchone()
                        if not row:
                            continue
                        await db.execute(
                            "UPDATE info SET subscription_end = ? WHERE telegram_username = ?",
                            (new_end.isoformat(), username)
                        )
                        await db.commit()
                        updated = True
                        break
                except Exception:
                    continue

        if updated:
            await send_success_message(callback, "✅ Лицензия обновлена", back_callback="admin_licenses")
        else:
            await callback.answer("Пользователь не найден", show_alert=True)
    finally:
        await state.clear()

# --- Реализация недостающих админских хендлеров ---
async def back_to_admin_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("🛠 Админ-панель", reply_markup=get_admin_menu_keyboard())

async def exit_admin(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await send_success_message(callback, "🚪 Вы вышли из админ-панели", back_callback="back_to_menu")

async def admin_user_action(callback: types.CallbackQuery):
    # admin_user_{user_id}_{page}_{sort}
    parts = callback.data.split("_")
    try:
        user_id = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        sort_type = parts[4] if len(parts) > 4 else 'alpha'
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    # Show current ban status and license actions
    # Fetch user's current ban status
    is_banned = False
    try:
        users = await _fetch_all_users()
        for u in users:
            if u['user_id'] == user_id:
                is_banned = bool(u.get('is_banned', False))
                break
    except Exception:
        pass

    kb = InlineKeyboardBuilder()
    if is_banned:
        kb.row(InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin_confirm_unban_{user_id}"))
    else:
        kb.row(InlineKeyboardButton(text="🚫 Забанить", callback_data=f"admin_confirm_ban_{user_id}"))
    kb.row(InlineKeyboardButton(text="💳 Продлить лицензию", callback_data=f"admin_license_extend_{user_id}"))
    kb.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"admin_users_page_{page}_{sort_type}"))
    await callback.message.edit_text(f"👤 Пользователь ID: {user_id}", reply_markup=kb.as_markup())

async def admin_toggle_ban(callback: types.CallbackQuery):
    """Toggle ban/unban for a user across all DBs"""
    try:
        user_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    # Deprecated: replaced by explicit confirm/do handlers
    await callback.answer("Используйте кнопки Бан/Разбан в пользовательском меню", show_alert=True)

async def admin_license_extend(callback: types.CallbackQuery):
    """Show license extend options for a user"""
    try:
        user_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="+1 день", callback_data=f"admin_confirm_license_1d_{user_id}"),
        InlineKeyboardButton(text="+7 дней", callback_data=f"admin_confirm_license_7d_{user_id}")
    )
    kb.row(
        InlineKeyboardButton(text="+1 месяц", callback_data=f"admin_confirm_license_30d_{user_id}"),
        InlineKeyboardButton(text="+1 год", callback_data=f"admin_confirm_license_365d_{user_id}")
    )
    kb.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"admin_users_page_0_alpha"))
    await callback.message.edit_text("💳 Продлить лицензию:", reply_markup=kb.as_markup())

async def admin_quick_license(callback: types.CallbackQuery):
    # admin_license_quick_{dur}_{user_id}
    parts = callback.data.split("_")
    try:
        dur = parts[3]
        user_id = int(parts[4])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    suffix_to_days = {
        '1d': 1,
        '2d': 2,
        '7d': 7,
        '14d': 14,
        '30d': 30,
        '365d': 365,
    }
    from datetime import datetime, timedelta
    days = suffix_to_days.get(dur)
    if not days:
        await callback.answer("Неизвестная длительность", show_alert=True)
        return
    new_end = datetime.now() + timedelta(days=days)
    # Ask for confirmation before applying
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_do_license_{dur}_{user_id}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_users_page_0_alpha")
    )
    await callback.message.edit_text(f"Подтвердите продление на {days} дней для пользователя {user_id}", reply_markup=kb.as_markup())


async def admin_confirm_ban(callback: types.CallbackQuery):
    try:
        user_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Подтвердить бан", callback_data=f"admin_do_ban_{user_id}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_user_{user_id}_0_alpha")
    )
    await callback.message.edit_text(f"Вы уверены, что хотите забанить пользователя {user_id}?", reply_markup=kb.as_markup())


async def admin_do_ban(callback: types.CallbackQuery):
    try:
        user_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    updated = False
    try:
        import os
        import aiosqlite
        if os.path.exists(Config.DB_DIR):
            for filename in os.listdir(Config.DB_DIR):
                if not filename.endswith('.db'):
                    continue
                db_path = os.path.join(Config.DB_DIR, filename)
                async with aiosqlite.connect(db_path) as db:
                    await db.execute("UPDATE info SET is_banned = 1 WHERE telegram_user_id = ?", (user_id,))
                    await db.commit()
                    updated = True
    except Exception:
        updated = False
    if updated:
        await send_success_message(callback, "✅ Пользователь забанен", back_callback="admin_users")
    else:
        await callback.answer("Пользователь не найден", show_alert=True)


async def admin_confirm_unban(callback: types.CallbackQuery):
    try:
        user_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Подтвердить разбан", callback_data=f"admin_do_unban_{user_id}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_user_{user_id}_0_alpha")
    )
    await callback.message.edit_text(f"Вы уверены, что хотите разбанить пользователя {user_id}?", reply_markup=kb.as_markup())


async def admin_do_unban(callback: types.CallbackQuery):
    try:
        user_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    updated = False
    try:
        import os
        import aiosqlite
        if os.path.exists(Config.DB_DIR):
            for filename in os.listdir(Config.DB_DIR):
                if not filename.endswith('.db'):
                    continue
                db_path = os.path.join(Config.DB_DIR, filename)
                async with aiosqlite.connect(db_path) as db:
                    await db.execute("UPDATE info SET is_banned = 0 WHERE telegram_user_id = ?", (user_id,))
                    await db.commit()
                    updated = True
    except Exception:
        updated = False
    if updated:
        await send_success_message(callback, "✅ Пользователь разбанен", back_callback="admin_users")
    else:
        await callback.answer("Пользователь не найден", show_alert=True)


async def admin_confirm_license(callback: types.CallbackQuery):
    # callback: admin_confirm_license_{dur}_{user_id}
    parts = callback.data.split("_")
    try:
        dur = parts[3]
        user_id = int(parts[4])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    # ask confirmation
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_do_license_{dur}_{user_id}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"admin_license_extend_{user_id}")
    )
    await callback.message.edit_text(f"Подтвердите продление ({dur}) для пользователя {user_id}", reply_markup=kb.as_markup())


async def admin_do_license(callback: types.CallbackQuery):
    # callback: admin_do_license_{dur}_{user_id}
    parts = callback.data.split("_")
    try:
        dur = parts[3]
        user_id = int(parts[4])
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    suffix_to_days = {
        '1d': 1,
        '7d': 7,
        '30d': 30,
        '365d': 365,
    }
    days = suffix_to_days.get(dur)
    if not days:
        await callback.answer("Неизвестная длительность", show_alert=True)
        return
    now = datetime.now()
    updated = False
    try:
        import os
        import aiosqlite
        if os.path.exists(Config.DB_DIR):
            for filename in os.listdir(Config.DB_DIR):
                if not filename.endswith('.db'):
                    continue
                db_path = os.path.join(Config.DB_DIR, filename)
                async with aiosqlite.connect(db_path) as db:
                    cursor = await db.execute("SELECT subscription_end FROM info WHERE telegram_user_id = ? LIMIT 1", (user_id,))
                    row = await cursor.fetchone()
                    if not row:
                        continue
                    cur_end = None
                    try:
                        if row[0]:
                            cur_end = datetime.fromisoformat(row[0])
                    except Exception:
                        cur_end = None
                    base = cur_end if cur_end and cur_end > now else now
                    new_end = base + timedelta(days=days)
                    await db.execute("UPDATE info SET subscription_end = ? WHERE telegram_user_id = ?", (new_end.isoformat(), user_id))
                    await db.commit()
                    updated = True
    except Exception:
        updated = False
    if updated:
        await send_success_message(callback, "✅ Лицензия обновлена", back_callback="admin_users")
    else:
        await callback.answer("Пользователь не найден", show_alert=True)

# --- Заглушки для отсутствующих хендлеров (не переопределяют уже реализованные) ---
async def not_implemented(callback_or_message, *args, **kwargs):
    text = "⛔️ Функция временно недоступна"
    try:
        if hasattr(callback_or_message, 'message'):
            await callback_or_message.message.answer(text)
        elif hasattr(callback_or_message, 'answer'):
            await callback_or_message.answer(text)
    except Exception:
        pass

def _ensure_handler_names(names: list[str]):
    g = globals()
    for name in names:
        if name not in g:
            g[name] = not_implemented

_ensure_handler_names([
    # авто/источники/публичные
    'post_auto','auto_source_linked','auto_source_public','public_auto_once','public_auto_periodic',
    # аккаунты/каналы/создание
    'select_link_channel','sort_channels','manage_accounts','unlink_account',
    'admin_link_main_account','create_channel_start','create_channel_account','create_channel_type',
    'create_channel_count','process_channel_count_custom','create_channel_name_method','create_channel_name_input',
    'check_bot_admin','create_post_from_account','delete_account_menu','post_account_select','post_channel_select',
    'post_content_send','delete_account_confirm','delete_account_do','periodic_donor_select','post_from_account_manual',
    'post_from_account_auto','autoacc_donor_select','autoacc_target_select','autoacc_targets_selected',
    'public_once_target_select','public_periodic_target_select','public_periodic_targets_selected',
    # рандом/периодические
    'auto_random','auto_periodic','periodic_source_linked','periodic_source_public','random_donor_select',
    'random_target_select','random_donors_selected','random_targets_selected','process_random_interval',
    'process_random_posts_per_day','process_post_freshness','process_random_freshness',
    # запланированные/действия
    'post_action',
    'delete_post','confirm_delete','change_donor','process_new_donor','handle_channels_pagination',
    'handle_scheduled_posts_pagination','select_source_channel','select_target_channel','target_channels_selected',
    'show_random_post_details','confirm_create_periodic','confirm_create_random','confirm_create_single',
    'cancel_create_post','process_admin_password','donor_type_linked','donor_type_public',
    # админ/пользователи
    'admin_users_select_mode','admin_users_sel_toggle','admin_users_sel_page','admin_users_apply_ban_selected',
    'admin_users_apply_unban_selected',
    # публичные рандомные выборы
    'public_random_target_select','public_random_targets_selected',
    # новые/прочие отсутствующие
    'channel_generate_one','channel_generate_many','process_generate_count',
    'admin_user_action','admin_ban_user','admin_unban_user','back_to_admin_menu','exit_admin',
    'process_public_channel_input',
])

# --- Реализация выбора каналов для репостов/постов (select_channel / channels_selected) ---
async def select_channel(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    try:
        parts = callback.data.split("_")
        if parts[-1] == "all":
            return await select_all_channels_toggle(callback, state)
        channel_id = int(parts[-1])
    except Exception:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    selected_ids, _ = await _toggle_id_in_state_list(state, 'selected_channels', channel_id)
    channels = await _fetch_user_channels(user_id, username)
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=selected_ids,
        build_callback_prefix="select_channel",
        title_text="📢 Выберите каналы для публикации:",
        done_callback="channels_selected",
        back_callback="create_post",
    )

async def channels_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get('selected_channels', []) or []
    if not selected:
        await callback.answer("Выберите хотя бы один канал", show_alert=True)
        return
    await send_success_message(callback, "✅ Каналы выбраны", back_callback="create_post")

async def select_all_channels_toggle(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    channels = await _fetch_user_channels(user_id, username)
    all_ids = [cid for cid, _ in channels]
    await state.update_data(selected_channels=all_ids)
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=all_ids,
        build_callback_prefix="select_channel",
        title_text="📢 Выберите каналы для публикации:",
        done_callback="channels_selected",
        back_callback="create_post",
    )

# --- Обертки для пагинации, чтобы вызывать реальные функции из pagination.py ---
async def handle_channels_pagination(callback: types.CallbackQuery):
    await _handle_channels_pagination(callback)

async def handle_scheduled_posts_pagination(callback: types.CallbackQuery):
    await _handle_scheduled_posts_pagination(callback)

async def handle_admin_users_pagination(callback: types.CallbackQuery):
    await _handle_admin_users_pagination(callback)

# ----------------- Implementations for previously undefined handlers -----------------

# Auto posting menu and sources
async def post_auto(callback: types.CallbackQuery):
    await callback.message.edit_text("🔄 Выберите автоматический режим:", reply_markup=get_auto_post_keyboard())

async def auto_source_linked(callback: types.CallbackQuery):
    await callback.message.edit_text("🔗 Источник: привязанные каналы. Дальнейшая настройка скоро.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="post_auto")]]))

async def auto_source_public(callback: types.CallbackQuery):
    await callback.message.edit_text("🌐 Источник: публичные каналы. Дальнейшая настройка скоро.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="post_auto")]]))

async def public_auto_once(callback: types.CallbackQuery):
    await callback.message.edit_text("🗓 Одноразовая публикация из публичного канала: настройка скоро.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="post_auto")]]))

async def public_auto_periodic(callback: types.CallbackQuery):
    await callback.message.edit_text("🔁 Периодическая публикация из публичного канала: настройка скоро.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="post_auto")]]))

# Channel linking and sorting
async def link_channel(callback: types.CallbackQuery):
    text = (
        "🔗 Привязать канал\n\n"
        "Нажмите кнопку ниже, выберите ваш канал и добавьте бота администратором."
    )
    req = KeyboardButtonRequestChat(
        request_id=1,
        chat_is_channel=True,
        bot_administrator_rights=ChatAdministratorRights(
            is_anonymous=False,
            can_manage_chat=True,
            can_delete_messages=True,
            can_post_messages=True,
            can_edit_messages=True,
            can_invite_users=True,
            can_restrict_members=False,
            can_promote_members=False,
            can_change_info=False,
            can_pin_messages=True,
            can_manage_topics=False,
            can_manage_video_chats=True,
            can_post_stories=False,
            can_edit_stories=False,
            can_delete_stories=False,
        ),
        user_administrator_rights=ChatAdministratorRights(
            is_anonymous=False,
            can_manage_chat=True,
            can_delete_messages=True,
            can_post_messages=True,
            can_edit_messages=True,
            can_invite_users=True,
            can_restrict_members=False,
            can_promote_members=False,
            can_change_info=False,
            can_pin_messages=True,
            can_manage_topics=False,
            can_manage_video_chats=True,
            can_post_stories=False,
            can_edit_stories=False,
            can_delete_stories=False,
        ),
        bot_is_member=False,
    )
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[[KeyboardButton(text="Выбрать канал", request_chat=req)], [KeyboardButton(text="Отмена")]]
    )
    await callback.message.answer(text, reply_markup=kb)

async def select_link_channel(callback: types.CallbackQuery, bot: Bot):
    await callback.answer("Выбор канала сохранен")

async def sort_channels(callback: types.CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    sort_type = callback.data.split("_")[-1]
    db_path = await get_user_db_path(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        if sort_type == "alpha":
            query = "SELECT * FROM channels ORDER BY channel_title"
            title = "📋 Ваши каналы (по алфавиту):"
        elif sort_type == "posts":
            query = (
                """
                SELECT c.*, COUNT(p.id) as post_count 
                FROM channels c 
                LEFT JOIN posts p ON c.channel_id = p.channel_id 
                GROUP BY c.channel_id 
                ORDER BY post_count DESC
                """
            )
            title = "📋 Ваши каналы (по количеству постов):"
        else:
            query = "SELECT * FROM channels"
            title = "📋 Ваши каналы:"
        cursor = await db.execute(query)
        channels = await cursor.fetchall()
    await _display_channels_paginated(callback, channels, page=0, items_per_page=5, title=title, back_callback="list_channels", sort_type=sort_type)

# Accounts management
async def manage_accounts(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    await ensure_user_database(user_id, username)
    accounts = []
    async with aiosqlite.connect(db_path) as db:
        try:
            cursor = await db.execute("SELECT phone_number, is_main FROM linked_accounts")
            rows = await cursor.fetchall()
            accounts = [(r[0], bool(r[1])) for r in rows]
        except Exception:
            accounts = []
    await callback.message.edit_text("👤 Управление аккаунтами:", reply_markup=get_manage_accounts_keyboard(accounts))

async def unlink_account(callback: types.CallbackQuery):
    # unlink_account_{phone}
    phone = callback.data.split("_")[-1]
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        await db.execute("DELETE FROM linked_accounts WHERE phone_number = ?", (phone,))
        await db.commit()
    await callback.answer("Удалено")
    await manage_accounts(callback)


async def set_main_account(callback: types.CallbackQuery):
    # set_main_{phone}
    phone = callback.data.split("_")[-1]
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    updated = False
    async with aiosqlite.connect(db_path) as db:
        try:
            # Сбрасываем флаг is_main у всех
            await db.execute("UPDATE linked_accounts SET is_main = 0")
            # Устанавливаем is_main для выбранного
            await db.execute("UPDATE linked_accounts SET is_main = 1 WHERE phone_number = ?", (phone,))
            await db.commit()
            updated = True
        except Exception:
            updated = False
    if updated:
        await callback.answer("Основной аккаунт установлен")
    else:
        await callback.answer("Не удалось установить основной аккаунт", show_alert=True)
    await manage_accounts(callback)

async def admin_link_main_account(callback: types.CallbackQuery, state: FSMContext):
    await link_account(callback, state)

# Channel creation flow (minimal)
async def create_channel_start(callback: types.CallbackQuery):
    await callback.message.edit_text("Создание канала:", reply_markup=get_channel_name_method_keyboard())

async def create_channel_account(callback: types.CallbackQuery):
    await callback.answer("Выбор аккаунта пока не требуется")

async def create_channel_type(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(channel_type=callback.data.replace("channel_type_", ""))
    await callback.answer("Тип канала сохранен")
    data = await state.get_data()
    if data.get("channel_create_pending") and data.get("channel_names"):
        await create_channel_do(callback, state)
    else:
        # Если названия ещё не заданы — спросим способ задания названий
        await callback.message.edit_text("Выберите способ задания названий:", reply_markup=get_channel_name_method_keyboard())
        await state.set_state(ChannelCreateStates.waiting_for_name_method)

async def create_channel_count(callback: types.CallbackQuery, state: FSMContext):
    try:
        count = int(callback.data.replace("channel_count_", ""))
        await state.update_data(channel_count=count)
        await callback.answer(f"Кол-во: {count}")
    except Exception:
        await callback.answer("Некорректное число", show_alert=True)

async def process_channel_count_custom(message: types.Message, state: FSMContext):
    try:
        count = int(message.text.strip())
        if count <= 0 or count > 100:
            raise ValueError
        await state.update_data(channel_count=count)
        await message.answer(f"Кол-во каналов установлено: {count}")
        # Переходим к выбору типа каналов
        await message.answer("Выберите тип каналов:", reply_markup=get_channel_type_keyboard())
        await state.set_state(ChannelCreateStates.waiting_for_channel_type)
    except Exception:
        await message.answer("Введите положительное число (<=100)")

async def create_channel_name_method(callback: types.CallbackQuery, state: FSMContext):
    if callback.data == "channel_name_manual":
        await channel_names_manual(callback, state)
    else:
        await channel_names_auto(callback, state)

async def create_channel_name_input(message: types.Message, state: FSMContext):
    names = [n.strip() for n in (message.text or "").split(",") if n.strip()]
    if not names:
        await message.answer("Введите хотя бы одно название")
        return
    await state.update_data(channel_names=names, channel_create_pending=True)
    await message.answer("Выберите тип каналов:", reply_markup=get_channel_type_keyboard())

async def check_bot_admin(callback: types.CallbackQuery):
    await callback.answer("Проверка прав администратора пока не реализована")

async def create_post_from_account(callback: types.CallbackQuery):
    await create_post(callback)

async def delete_account_menu(callback: types.CallbackQuery):
    await manage_accounts(callback)

async def post_account_select(callback: types.CallbackQuery):
    await callback.answer("Выбор аккаунта сохранен")

async def post_channel_select(callback: types.CallbackQuery):
    await callback.answer("Выбор канала сохранен")

async def post_content_send(message: types.Message, state: FSMContext):
    await message.answer("Отправьте контент поста (текст/медиа) в основном режиме создания поста.")

async def delete_account_confirm(callback: types.CallbackQuery):
    await callback.answer("Подтверждение удаления аккаунта пока не реализовано")

async def delete_account_do(callback: types.CallbackQuery):
    await callback.answer("Удаление аккаунта пока не реализовано")

async def periodic_donor_select(callback: types.CallbackQuery):
    await callback.answer("Выбор донора для периодических постов скоро")

async def post_from_account_manual(callback: types.CallbackQuery):
    await callback.answer("Ручной пост от аккаунта скоро")

async def post_from_account_auto(callback: types.CallbackQuery):
    await callback.answer("Автопост от аккаунта скоро")

async def autoacc_donor_select(callback: types.CallbackQuery):
    await callback.answer("Выбор донора (авто) скоро")

async def autoacc_target_select(callback: types.CallbackQuery):
    await callback.answer("Выбор целей (авто) скоро")

async def autoacc_targets_selected(callback: types.CallbackQuery):
    await callback.answer("Цели выбраны")

async def public_once_target_select(callback: types.CallbackQuery):
    await callback.answer("Выбор цели (разовая) скоро")

async def public_periodic_target_select(callback: types.CallbackQuery, state: FSMContext):
    """Тоггл выбора целевых каналов для периодического потока из публичного донора."""
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    parts = callback.data.split("_")
    last = parts[-1]
    if last == "all":
        channels = await _fetch_user_channels(user_id, username)
        all_ids = [cid for cid, _ in channels]
        await state.update_data(selected_targets=all_ids)
        selected_ids = all_ids
    else:
        try:
            channel_id = int(last)
        except Exception:
            await callback.answer("Некорректные данные", show_alert=True)
            return
        selected_ids, _ = await _toggle_id_in_state_list(state, 'selected_targets', channel_id)
        channels = await _fetch_user_channels(user_id, username)

    await _render_select_list(
        callback,
        items=channels,
        selected_ids=selected_ids,
        build_callback_prefix="public_periodic_target",
        title_text="📥 Выберите целевые каналы (можно несколько):",
        done_callback="public_periodic_targets_selected",
        back_callback="auto_periodic",
    )

async def public_periodic_targets_selected(callback: types.CallbackQuery, state: FSMContext):
    # После выбора целей — сразу создаем поток репостов без выбора свежести
    await create_repost_stream_from_state(callback, state)

async def auto_random(callback: types.CallbackQuery):
    # Сначала количество доноров
    await callback.message.edit_text("🎲 Сколько доноров использовать?", reply_markup=get_donor_count_keyboard())

async def auto_periodic(callback: types.CallbackQuery):
    # Для потоков репостов — тоже спросим количество
    await callback.message.edit_text("🔁 Доноры для потока: один или несколько?", reply_markup=get_periodic_donor_count_keyboard())

async def periodic_source_linked(callback: types.CallbackQuery):
    await callback.answer("Поток из привязанных: настройка скоро")

async def periodic_source_public(callback: types.CallbackQuery, state: FSMContext):
    # Старт настройки потока из публичного канала: просим ввести донор(ов)
    data = await state.get_data()
    allow_multi = bool(data.get('periodic_allow_multiple', False))
    hint = "одного" if not allow_multi else "одного или нескольких (через запятую)"
    await callback.message.edit_text(
        f"Введите @username или ссылку на публичный канал-донора — {hint}:" 
        "\nНапример: @telegrammm, https://t.me/telegrammm"
    )
    # Помечаем, что мы настраиваем поток репостов из публичного канала
    await state.update_data(periodic_flow="public", selected_targets=[])
    await state.set_state(PostStates.waiting_for_public_channel_input)

async def random_donor_select(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    try:
        channel_id = int(callback.data.split("_")[-1])
    except Exception:
        if callback.data.endswith("_all"):
            return await random_select_all_donors(callback, state)
        await callback.answer("Некорректный формат", show_alert=True)
        return
    data = await state.get_data()
    allow_multi = bool(data.get('allow_multiple_donors', False))
    selected_ids, _ = await _toggle_id_in_state_list(state, 'selected_donors', channel_id)
    channels = await _get_all_user_channels(user_id, username)
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=selected_ids,
        build_callback_prefix="random_donor",
        title_text=("📡 Выберите каналы-доноры (можно несколько):" if allow_multi else "📡 Выберите один канал-донора:"),
        done_callback="random_donors_selected",
        back_callback="auto_random",
    )

async def random_target_select(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    try:
        channel_id = int(callback.data.split("_")[-1])
    except Exception:
        if callback.data.endswith("_all"):
            return await random_select_all_targets(callback, state)
        await callback.answer("Некорректный формат", show_alert=True)
        return
    selected_ids, _ = await _toggle_id_in_state_list(state, 'selected_targets', channel_id)
    data = await state.get_data()
    donors = data.get('selected_donors', [])
    channels = await _fetch_user_channels(user_id, username, exclude_ids=donors)
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=selected_ids,
        build_callback_prefix="random_target",
        title_text="📥 Выберите целевые каналы (можно несколько):",
        done_callback="random_targets_selected",
        back_callback="auto_random",
    )

async def random_donors_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected_donors: list[int] = data.get('selected_donors', [])
    if not selected_donors:
        await callback.answer("Выберите хотя бы одного донора", show_alert=True)
        return
    await state.update_data(random_is_public=False)
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    channels = await _fetch_user_channels(user_id, username, exclude_ids=selected_donors)
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=[],
        build_callback_prefix="random_target",
        title_text="📥 Выберите целевые каналы (можно несколько):",
        done_callback="random_targets_selected",
        back_callback="auto_random",
    )
    await state.set_state(PostStates.waiting_for_random_targets)
    await state.update_data(selected_targets=[])

async def random_targets_selected(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    targets: list[int] = data.get('selected_targets', [])
    if not targets:
        await callback.answer("Выберите хотя бы один канал", show_alert=True)
        return
    await callback.message.edit_text("Введите количество постов в день (число):")
    await state.set_state(PostStates.waiting_for_random_posts_per_day)

async def process_random_interval(message: types.Message, state: FSMContext):
    await state.update_data(random_interval=message.text.strip())
    await message.answer("Интервал сохранен")

async def process_random_posts_per_day(message: types.Message, state: FSMContext):
    try:
        posts = int(message.text.strip())
        if posts <= 0:
            raise ValueError
    except Exception:
        await message.answer("Введите положительное число")
        return
    await state.update_data(posts_per_day=posts)
    await message.answer(
        "Выберите свежесть постов донора:",
        reply_markup=get_post_freshness_keyboard()
    )
    await state.set_state(PostStates.waiting_for_random_freshness)

async def process_post_freshness(callback: types.CallbackQuery, state: FSMContext):
    freshness = int(callback.data.split("_")[-1])
    await state.update_data(post_freshness=freshness)
    await callback.answer("Свежесть сохранена")
    data = await state.get_data()
    # Если это настройка потока репостов из публичного канала — создаем его сразу
    if data.get("create_repost_stream") or data.get("periodic_flow") == "public":
        await create_repost_stream_from_state(callback, state)
        return
    await callback.answer("Свежесть сохранена")

async def process_random_freshness(callback: types.CallbackQuery, state: FSMContext):
    freshness = int(callback.data.split("_")[-1])
    await state.update_data(random_freshness=freshness)
    await callback.answer("Свежесть сохранена")
    await confirm_create_random_stream(callback, state)

# Scheduled posts management
async def scheduled_posts(callback: types.CallbackQuery):
    await callback.message.edit_text("📋 Запланированные посты:", reply_markup=get_scheduled_posts_keyboard())

async def scheduled_posts_single(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    data = await get_scheduled_posts(user_id, username)
    await _display_scheduled_posts_paginated(callback, data.get('posts', []), page=0, items_per_page=5, title="📝 Ваши единичные запланированные посты:", back_callback="scheduled_posts", post_type="post")

async def scheduled_posts_streams(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    data = await get_scheduled_posts(user_id, username)
    await _display_scheduled_posts_paginated(callback, data.get('repost_streams', []), page=0, items_per_page=5, title="🔄 Ваши потоки репостов:", back_callback="scheduled_posts", post_type="stream")

async def scheduled_posts_random(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    data = await get_scheduled_posts(user_id, username)
    # Display random stream configurations (from random_posts table)
    await _display_scheduled_posts_paginated(
        callback,
        data.get('old_random_posts', []),
        page=0,
        items_per_page=5,
        title="🎲 Ваши рандомные публикации:",
        back_callback="scheduled_posts",
        post_type="random_stream_config",
    )

async def post_action(callback: types.CallbackQuery):
    # post_action_{post_type}_{id}
    parts = callback.data.split("_")
    post_id = int(parts[-1])
    post_type = "_".join(parts[2:-1])
    await callback.message.edit_text("🛠 Действия с постом:", reply_markup=get_post_action_keyboard(post_id, post_type))

async def delete_post(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    post_id = int(parts[-1])
    post_type = "_".join(parts[2:-1])
    await callback.message.edit_text("❓ Подтвердите удаление:", reply_markup=get_confirm_delete_keyboard(post_id, post_type))

async def confirm_delete(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    post_id = int(parts[-1])
    post_type = "_".join(parts[2:-1])
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    # Map type for DB
    db_type = {
        'post': 'post',
        'repost_stream': 'repost_stream',
        'random_stream': 'random_post',
        'random_stream_config': 'random_post',
    }.get(post_type, 'post')
    await delete_scheduled_post(user_id, username, post_id, db_type)
    await send_success_message(callback, "✅ Удалено", back_callback="scheduled_posts")

async def change_donor(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    post_id = int(parts[-1])
    post_type = "_".join(parts[2:-1])
    await state.update_data(change_donor_target=(post_id, post_type))
    await callback.message.edit_text("✏️ Введите нового донора (username или id):")
    await state.set_state(ScheduledPostsStates.waiting_for_new_donor)

async def process_new_donor(message: types.Message, state: FSMContext):
    data = await state.get_data()
    target = data.get('change_donor_target')
    if not target:
        await message.answer("Нет поста для изменения")
        return
    post_id, post_type = target
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    await update_post_donor(user_id, username, post_id, post_type, message.text.strip())
    await state.clear()
    await message.answer("✅ Донор обновлен")

async def select_source_channel(callback: types.CallbackQuery, state: FSMContext):
    try:
        channel_id = int(callback.data.split("_")[-1])
        await state.update_data(source_channel_id=channel_id)
        await callback.answer("Источник выбран")
    except Exception:
        await callback.answer("Ошибка выбора источника", show_alert=True)

async def show_random_post_details(callback: types.CallbackQuery):
    # Support optional page in callback data formats:
    # - show_random_post_details_{post_id}
    # - show_random_post_details_{post_id}_{page}
    parts = callback.data.split("_")
    try:
        # If last part is page and second-last is id
        if len(parts) >= 2 and parts[-1].isdigit() and parts[-2].isdigit():
            page = int(parts[-1])
            post_id = int(parts[-2])
        elif len(parts) >= 1 and parts[-1].isdigit():
            post_id = int(parts[-1])
            page = 0
        else:
            raise ValueError("no id")
    except Exception:
        await callback.answer("Некорректный id", show_alert=True)
        return

    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)

    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("""
            SELECT donor_channels, target_channels, posts_per_day, post_freshness
            FROM random_posts WHERE id = ?
        """, (post_id,))
        row = await cursor.fetchone()
    if not row:
        # Поток могли удалить/деактивировать — перерисуем список вместо алерта
        data = await get_scheduled_posts(user_id, username)
        await _display_scheduled_posts_paginated(
            callback,
            data.get('old_random_posts', []),
            page=0,
            items_per_page=5,
            title="🎲 Ваши рандомные публикации:",
            back_callback="scheduled_posts",
            post_type="random_stream_config",
        )
        return
    donors_json, targets_json, ppd, freshness = row
    donors = safe_json_loads(donors_json, [])
    targets = safe_json_loads(targets_json, [])

    async def resolve_channel_name(db_path_local, ch):
        if isinstance(ch, int):
            async with aiosqlite.connect(db_path_local) as db_l:
                cur = await db_l.execute("SELECT channel_title, channel_username FROM channels WHERE channel_id = ?", (ch,))
                r = await cur.fetchone()
                if r:
                    title, uname = r
                    return (f"@{uname}" if uname else None) or (title or str(ch))
            return str(ch)
        s = str(ch)
        if s.startswith('@'):
            return s
        if s.lstrip('-').isdigit():
            return await resolve_channel_name(db_path_local, int(s))
        return s

    donor_names = []
    for d in (donors if isinstance(donors, list) else []):
        donor_names.append(await resolve_channel_name(db_path, d))

    header = (
        f"🎲 Поток #{post_id}\n"
        f"Доноры: {len(donors) if isinstance(donors, list) else 0}\n"
        f"Целей: {len(targets) if isinstance(targets, list) else 0}\n\n"
    )

    from datetime import datetime, timedelta
    now_buf = datetime.now() + timedelta(seconds=60)

    per_page = 10
    per_target_times = []
    if isinstance(targets, list):
        async with aiosqlite.connect(db_path) as db_l:
            for target in targets:
                display_target = await resolve_channel_name(db_path, target)
                try:
                    cur = await db_l.execute(
                        """
                        SELECT scheduled_time FROM posts
                        WHERE random_post_id = ? AND channel_id = ? AND is_published = 0
                          AND scheduled_time > ?
                        ORDER BY scheduled_time ASC
                        """,
                        (post_id, target, datetime.now().isoformat())
                    )
                    rows = await cur.fetchall()
                except Exception:
                    rows = []
                times_list = []
                for (tval,) in rows:
                    try:
                        dt = datetime.fromisoformat(str(tval))
                        if dt > now_buf:
                            times_list.append(dt)
                    except Exception:
                        continue
                times_list.sort()
                per_target_times.append((display_target, times_list))

    # compute max pages across targets
    max_pages = 0
    for _, tl in per_target_times:
        pages = max(0, (len(tl) - 1) // per_page)
        if pages > max_pages:
            max_pages = pages

    sections = []
    for idx, (display_target, tl) in enumerate(per_target_times, start=1):
        start = page * per_page
        chunk = tl[start:start + per_page]
        formatted_times = [dt.strftime("%d.%m - %H:%M") for dt in chunk]
        section_text = (
            f"«{display_target}#{idx}:\n"
            f"Постов/день: {ppd}\n"
            f"Свежесть: {freshness} д\n"
            f"Доноры: {', '.join(donor_names) if donor_names else '—'}\n"
            f"Цель: {display_target}\n"
            + ("Время публикации:\n" + ",\n ".join(formatted_times) if formatted_times else "Время публикации:\n—")
        )
        sections.append(section_text)

    text = header + "\n\n".join(sections) if sections else header.rstrip()

    # build keyboard: top row actions, middle nav, bottom back
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    kb_rows = []
    # actions row
    kb_rows.append([
        InlineKeyboardButton(text="❌ Отменить публикации", callback_data=f"delete_post_random_stream_{post_id}"),
        InlineKeyboardButton(text="🔄 Сменить донора", callback_data=f"change_donor_random_stream_{post_id}")
    ])
    # nav row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"show_random_post_details_{post_id}_{page-1}"))
    if page < max_pages:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"show_random_post_details_{post_id}_{page+1}"))
    if nav:
        kb_rows.append(nav)
    # back row
    kb_rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="scheduled_posts")])

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))

async def paginate_random_times(callback: types.CallbackQuery):
    """Пагинация списка времен публикаций для рандомного потока."""
    try:
        _, _, _, stream_id_str, page_str = callback.data.split("_")
        stream_id = int(stream_id_str)
        page = int(page_str)
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT next_post_times_json, donor_channels, target_channels, posts_per_day, post_freshness FROM random_posts WHERE id = ?", (stream_id,))
        row = await cursor.fetchone()
    if not row:
        data = await get_scheduled_posts(user_id, username)
        await _display_scheduled_posts_paginated(
            callback,
            data.get('old_random_posts', []),
            page=0,
            items_per_page=5,
            title="🎲 Ваши рандомные публикации:",
            back_callback="scheduled_posts",
            post_type="random_stream_config",
        )
        return
    times_json, donors_json, targets_json, ppd, freshness = row
    all_times_raw = safe_json_loads(times_json, []) or []
    # Фильтруем прошедшие
    future_times_dt = []
    for t in all_times_raw:
        try:
            dt = datetime.fromisoformat(str(t))
            if dt > datetime.now():
                future_times_dt.append(dt)
        except Exception:
            continue
    future_times_dt.sort()
    # Обновим БД при необходимости
    if len(future_times_dt) != len(all_times_raw):
        try:
            async with aiosqlite.connect(db_path) as db:
                await db.execute(
                    "UPDATE random_posts SET next_post_times_json = ? WHERE id = ?",
                    (json.dumps([dt.isoformat() for dt in future_times_dt]), stream_id)
                )
                await db.commit()
        except Exception:
            pass
    donors = safe_json_loads(donors_json, [])
    targets = safe_json_loads(targets_json, [])
    start = page * 10
    end = start + 10
    chunk = future_times_dt[start:end]
    formatted_times = [dt.strftime("%d.%m - %H:%M") for dt in chunk]
    text = (
        f"🎲 Поток #{stream_id}\n"
        f"Доноров: {len(donors) if isinstance(donors, list) else 0}\n"
        f"Целей: {len(targets) if isinstance(targets, list) else 0}\n"
        f"Постов/день: {ppd}\n"
        f"Свежесть: {freshness} д\n"
        f"Время публикаций:\n " + ",\n ".join(formatted_times)
    )
    # Построим клавиатуру с навигацией
    total_pages = max(0, (len(future_times_dt) - 1) // 10)
    kb = InlineKeyboardBuilder()
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"random_times_page_{stream_id}_{page-1}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"random_times_page_{stream_id}_{page+1}"))
    if nav_row:
        kb.row(*nav_row)
    actions = get_post_action_keyboard(stream_id, 'random_stream')
    nav_markup = kb.as_markup()
    nav_inline = nav_markup.inline_keyboard
    final_inline = nav_inline + (actions.inline_keyboard if hasattr(actions, 'inline_keyboard') else [])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=final_inline))

async def confirm_create_periodic(callback: types.CallbackQuery):
    await callback.answer("Создание периодического поста скоро")

async def confirm_create_random(callback: types.CallbackQuery, state: FSMContext):
    # При подтверждении создаем поток по данным состояния
    await create_random_stream_from_state(callback, state)

async def confirm_create_single(callback: types.CallbackQuery):
    await callback.answer("Создание обычного поста скоро")

async def cancel_create_post(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await send_success_message(callback, "❌ Создание отменено", back_callback="back_to_menu")

async def process_admin_password(message: types.Message, state: FSMContext):
    # Примитивная авторизация: пропускаем всех
    await state.clear()
    await message.answer("🛠 Админ-панель:", reply_markup=get_admin_menu_keyboard())

async def donor_type_linked(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    channels = await _get_all_user_channels(user_id, username)
    if not channels:
        await callback.answer("Нет привязанных каналов", show_alert=True)
        return
    # Если разрешено только один — отметим подсказку и сделаем автосброс после выбора второго
    data = await state.get_data()
    allow_multi = bool(data.get('allow_multiple_donors', False))
    title = "📡 Выберите каналы-доноры (можно несколько):" if allow_multi else "📡 Выберите один канал-донора:"
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=[],
        build_callback_prefix="random_donor",
        title_text=title,
        done_callback="random_donors_selected",
        back_callback="auto_random",
    )
    # Выбор доноров будет продолжен в обработчиках random_donor_select/random_donors_selected
    await state.set_state(PostStates.waiting_for_random_donors)

async def donor_type_public(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите @username или ссылку на канал(ы)-доноры через запятую.\nПример: @news, https://t.me/example",
    )
    # Сбрасываем выбранных доноров и помечаем, что доноры публичные
    await state.update_data(selected_donors=[], random_is_public=True)
    await state.set_state(PostStates.waiting_for_random_donors)

async def public_random_target_select(callback: types.CallbackQuery):
    await callback.answer("Выбор целей (публичный рандом) скоро")

async def public_random_targets_selected(callback: types.CallbackQuery):
    await callback.answer("Цели сохранены")

async def channel_generate_one(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(channel_count=1)
    await channel_names_auto(callback, state)

async def channel_generate_many(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Введите количество каналов (число):")
    await state.set_state(ChannelCreateStates.waiting_for_generate_count)

async def process_generate_count(message: types.Message, state: FSMContext):
    try:
        count = int(message.text.strip())
    except Exception:
        await message.answer("Введите число")
        return
    await state.update_data(channel_count=count)
    await channel_names_auto(callback=types.CallbackQuery(message=message), state=state)

async def cancel_reply_keyboard(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    user_info = await get_user_info(user_id, username)
    await message.answer("📋 Главное меню", reply_markup=get_main_menu_keyboard(user_info))

# --- Доп. функции для рандомных постов ---
async def process_public_random_donors(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите хотя бы одного донора")
        return
    raw_items = [p.strip() for p in text.replace(";", ",").split(",") if p.strip()]
    donors: list[str | int] = []
    for item in raw_items:
        if item.startswith("http"):
            username = item.split("/")[-1]
            username = username.split("?")[0].split("#")[0]
            donors.append(f"@{username}")
        elif item.startswith("@"):
            # Уберём дубли @ и возьмём валидный сегмент
            seg = item.split("@")[-1]
            donors.append(f"@{seg}")
        elif item.isdigit():
            donors.append(int(item))
        else:
            donors.append(f"@{item}")
    data = await state.get_data()
    allow_multi = bool(data.get('allow_multiple_donors', False))
    if not allow_multi and donors:
        donors = donors[:1]
    await state.update_data(selected_donors=donors, random_is_public=True)
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    channels = await _fetch_user_channels(user_id, username)
    await _render_select_list(
        message,
        items=channels,
        selected_ids=[],
        build_callback_prefix="random_target",
        title_text="📥 Выберите целевые каналы (можно несколько):",
        done_callback="random_targets_selected",
        back_callback="auto_random",
    )
    await state.set_state(PostStates.waiting_for_random_targets)
    await state.update_data(selected_targets=[])

async def random_select_all_donors(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    channels = await _get_all_user_channels(user_id, username)
    all_ids = [cid for cid, _ in channels]
    await state.update_data(selected_donors=all_ids)
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=all_ids,
        build_callback_prefix="random_donor",
        title_text="📡 Выберите каналы-доноры (можно несколько):",
        done_callback="random_donors_selected",
        back_callback="auto_random",
    )

async def random_select_all_targets(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    data = await state.get_data()
    donors = data.get('selected_donors', [])
    channels = await _fetch_user_channels(user_id, username, exclude_ids=donors)
    all_ids = [cid for cid, _ in channels]
    await state.update_data(selected_targets=all_ids)
    await _render_select_list(
        callback,
        items=channels,
        selected_ids=all_ids,
        build_callback_prefix="random_target",
        title_text="📥 Выберите целевые каналы (можно несколько):",
        done_callback="random_targets_selected",
        back_callback="auto_random",
    )

async def confirm_create_random_stream(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    donors = data.get('selected_donors', [])
    targets = data.get('selected_targets', [])
    posts_per_day = data.get('posts_per_day', 1)
    freshness = data.get('random_freshness', data.get('post_freshness', 7))
    is_public = data.get('random_is_public', False)
    # Попытаемся показать целевые каналы в виде @username, если они есть в таблице channels
    donor_text = "—"
    target_text = "—"
    try:
        user_id = callback.from_user.id
        username = callback.from_user.username or str(user_id)
        db_path = await get_user_db_path(user_id, username)
        async with aiosqlite.connect(db_path) as db:
            donor_names = []
            for d in (donors if isinstance(donors, list) else []):
                try:
                    s = str(d)
                except Exception:
                    donor_names.append(str(d))
                    continue
                if s.startswith('@'):
                    donor_names.append(s)
                    continue
                if s.lstrip('-').isdigit():
                    try:
                        cur = await db.execute("SELECT channel_username FROM channels WHERE channel_id = ?", (int(s),))
                        r = await cur.fetchone()
                        if r and r[0]:
                            donor_names.append(f"@{r[0]}")
                        else:
                            donor_names.append(s)
                    except Exception:
                        donor_names.append(s)
                    continue
                donor_names.append(s)

            target_names = []
            for t in (targets if isinstance(targets, list) else []):
                try:
                    s = str(t)
                except Exception:
                    target_names.append(str(t))
                    continue
                if s.startswith('@'):
                    target_names.append(s)
                    continue
                if s.lstrip('-').isdigit():
                    try:
                        cur = await db.execute("SELECT channel_username FROM channels WHERE channel_id = ?", (int(s),))
                        r = await cur.fetchone()
                        if r and r[0]:
                            target_names.append(f"@{r[0]}")
                        else:
                            target_names.append(s)
                    except Exception:
                        target_names.append(s)
                    continue
                target_names.append(s)

            donor_text = ", ".join(donor_names) if donor_names else "—"
            target_text = ", ".join(target_names) if target_names else "—"
    except Exception:
        donor_text = ", ".join([str(d) for d in donors]) if donors else "—"
        target_text = ", ".join([str(t) for t in targets]) if targets else "—"
    summary = (
        "Подтвердите создание рандомного потока:\n\n"
        f"Доноры: {donor_text}\n"
        f"Цели: {target_text}\n"
        f"Постов в день: {posts_per_day}\n"
        f"Свежесть: {freshness} дн\n"
        f"Источник: {'публичные' if is_public else 'привязанные'}\n"
    )
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_create_random"))
    kb.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_create_post"))
    if hasattr(callback, 'message'):
        await callback.message.edit_text(summary, reply_markup=kb.as_markup())
    else:
        await callback.answer(summary)

    # Set state so the confirm button handler is active
    await state.set_state(PostStates.waiting_for_confirm_random)

async def create_random_stream_from_state(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    donors = data.get('selected_donors', [])
    targets = data.get('selected_targets', [])
    posts_per_day: int = int(data.get('posts_per_day', 1))
    freshness: int = int(data.get('random_freshness', data.get('post_freshness', 7)))
    is_public: bool = bool(data.get('random_is_public', False))

    if not donors or not targets:
        await callback.answer("Не выбраны доноры или цели", show_alert=True)
        return

    db_path = await get_user_db_path(user_id, username)
    phone_number = None
    if not is_public:
        async with aiosqlite.connect(db_path) as db:
            try:
                cur = await db.execute("SELECT phone_number FROM linked_accounts LIMIT 1")
                row = await cur.fetchone()
                phone_number = row[0] if row else None
            except Exception:
                phone_number = None
        if not phone_number:
            await send_error_message(callback, "Добавьте хотя бы один аккаунт в разделе 'Управлять привязкой'", back_callback="back_to_menu")
            return

    now = datetime.now()
    
    # Распределяем посты на оставшееся время СЕГОДНЯ (до 23:59:59)
    day_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    times: list[str] = []
    remaining_seconds_total = (day_end - now).total_seconds()
    if remaining_seconds_total <= 0:
        # На всякий случай: если день уже закончился, сдвинем на ближайшие минуты вперед
        remaining_seconds_total = 60
        day_end = now + timedelta(seconds=remaining_seconds_total)

    remaining_minutes = max(1, int(remaining_seconds_total // 60))

    # Минимальная отсечка, чтобы исключить около-прошедшие слоты
    min_future = now + timedelta(minutes=2)

    generated_datetimes: list[datetime] = []
    if posts_per_day <= remaining_minutes:
        picked_minutes = sorted(random.sample(range(remaining_minutes), posts_per_day))
        for m in picked_minutes:
            dt = now + timedelta(minutes=m, seconds=random.randint(0, 59))
            if dt < min_future:
                dt = min_future
            if dt > day_end:
                dt = day_end
            if dt > now and dt <= day_end:
                generated_datetimes.append(dt)
    else:
        # Постов больше, чем минут осталось — распределяем равномерно по минутам
        step = remaining_minutes / posts_per_day if posts_per_day > 0 else 1
        for i in range(posts_per_day):
            offset_minutes = int(i * step)
            dt = now + timedelta(minutes=offset_minutes, seconds=random.randint(0, 59))
            if dt < min_future:
                dt = min_future
            if dt > day_end:
                dt = day_end
            generated_datetimes.append(dt)

    # Сортируем и нормализуем к ISO
    generated_datetimes = sorted(generated_datetimes)
    # Для каждой цели генерируем отдельное расписание (случайное).
    # Важно: `posts_per_day` в UI трактуется как общее количество постов в день
    # для всего потока, поэтому распределяем его по целям
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            """
            INSERT INTO random_posts (
                donor_channels, target_channels, posts_per_day, post_freshness,
                is_active, last_post_time, phone_number, is_public_channel, next_post_times_json
            ) VALUES (?, ?, ?, ?, 1, NULL, ?, ?, ?)
            """,
            (
                json.dumps(donors), json.dumps(targets), posts_per_day, freshness,
                phone_number, 1 if is_public else 0, json.dumps([]) # will fill below
            )
        )
        stream_id = cursor.lastrowid
        union_times = []
        # Генерируем индивидуальные времена для каждой цели
        all_targets = targets if isinstance(targets, list) else []
        # Интерпретируем `posts_per_day` как количество постов В ДЕНЬ НА КАЖДУЮ ЦЕЛЬ
        for idx_target, target_channel in enumerate((all_targets if isinstance(all_targets, list) else []), start=0):
            # Генерируем список времён для этой цели
            target_generated: list[datetime] = []
            per_target_posts = int(posts_per_day)
            if per_target_posts <= remaining_minutes:
                picked_minutes = sorted(random.sample(range(remaining_minutes), per_target_posts))
                for m in picked_minutes:
                    dt = now + timedelta(minutes=m, seconds=random.randint(0, 59))
                    if dt < min_future:
                        dt = min_future
                    if dt > day_end:
                        dt = day_end
                    if dt > now and dt <= day_end:
                        target_generated.append(dt)
            else:
                step = remaining_minutes / per_target_posts if per_target_posts > 0 else 1
                for i in range(per_target_posts):
                    offset_minutes = int(i * step)
                    dt = now + timedelta(minutes=offset_minutes, seconds=random.randint(0, 59))
                    if dt < min_future:
                        dt = min_future
                    if dt > day_end:
                        dt = day_end
                    target_generated.append(dt)
            target_generated = sorted(target_generated)
            # Вставим эти времена в posts
            for dt in target_generated:
                try:
                    await db.execute(
                        """
                        INSERT INTO posts (
                            channel_id, content_type, content, scheduled_time, is_periodic,
                            period_hours, is_published, random_post_id, donor_channels_json,
                            target_channels_json, post_freshness, phone_number, is_public_channel
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            target_channel,
                            'random',
                            f'Рандомный пост ({dt.strftime("%d.%m %H:%M")})',
                            dt.isoformat(),
                            0,
                            0,
                            0,
                            stream_id,
                            json.dumps(donors),
                            json.dumps(targets),
                            freshness,
                            phone_number,
                            1 if is_public else 0,
                        )
                    )
                    union_times.append(dt)
                except Exception:
                    continue
        # Обновим next_post_times_json как объединение будущих времён
        try:
            await db.execute(
                "UPDATE random_posts SET next_post_times_json = ? WHERE id = ?",
                (json.dumps([t.isoformat() for t in sorted(union_times)]), stream_id)
            )
        except Exception:
            pass
        await db.commit()

    await state.clear()
    await send_success_message(callback, "✅ Рандомные публикации настроены", back_callback="scheduled_posts")

async def admin_search(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_for_search_query)
    await callback.message.edit_text("Введите часть username или ID для поиска:")

async def process_admin_search(message: types.Message, state: FSMContext):
    query = (message.text or '').strip().lstrip('@')
    users = await _fetch_all_users()
    # Простая фильтрация по username/ID
    filtered = []
    for u in users:
        if query.isdigit() and str(u['user_id']).startswith(query):
            filtered.append(u)
        elif u['username'] and query.lower() in u['username'].lower():
            filtered.append(u)
    await state.clear()
    if not filtered:
        await message.answer("Ничего не найдено")
        return
    # Отображаем первые 10 результатов
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardBuilder()
    for u in filtered[:10]:
        kb.row(InlineKeyboardButton(text=f"@{u['username']} ({u['days_left']} д)", callback_data=f"admin_user_{u['user_id']}_0_alpha"))
    kb.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_users_management"))
    await message.answer("Результаты поиска:", reply_markup=kb.as_markup())

async def show_post_details(callback: types.CallbackQuery):
    """Деталка единичного запланированного поста из таблицы posts."""
    try:
        post_id = int(callback.data.split("_")[-1])
    except Exception:
        await callback.answer("Некорректный id", show_alert=True)
        return
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            """
            SELECT p.id, p.channel_id, p.channel_username, p.content_type, p.content,
                   p.media_id, p.scheduled_time, p.is_published, c.channel_title
            FROM posts p
            LEFT JOIN channels c ON p.channel_id = c.channel_id
            WHERE p.id = ?
            """,
            (post_id,)
        )
        row = await cursor.fetchone()
    if not row:
        await callback.answer("Пост не найден", show_alert=True)
        return
    (_pid, channel_id, channel_username, content_type, content, media_id, scheduled_time, is_published, channel_title) = row
    channel_name = channel_title or channel_username or f"Канал {channel_id}"
    try:
        from datetime import datetime
        scheduled_dt = datetime.fromisoformat(str(scheduled_time))
        formatted_time = scheduled_dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        formatted_time = str(scheduled_time)
    text = (
        f"📝 Пост #{post_id}\n"
        f"Канал: {channel_name}\n"
        f"Тип: {content_type}\n"
        f"Время: {formatted_time}\n"
        f"Статус: {'опубликован' if is_published else 'ожидает'}"
    )
    await callback.message.edit_text(text, reply_markup=get_post_action_keyboard(post_id, 'post'))

async def create_repost_stream_from_state(callback: types.CallbackQuery, state: FSMContext):
    """Создание потока репостов из публичного канала по данным состояния."""
    data = await state.get_data()
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    donor_channel = data.get('public_channel')
    donor_list = data.get('public_channel_list')
    targets: list[int] = data.get('selected_targets', []) or []
    if not donor_channel and not donor_list or not targets:
        await callback.answer("Укажите донора и выберите целевые каналы", show_alert=True)
        return
    db_path = await get_user_db_path(user_id, username)
    async with aiosqlite.connect(db_path) as db:
        if donor_list and isinstance(donor_list, list):
            for donor in donor_list:
                await db.execute(
                    """
                    INSERT INTO repost_streams (
                        donor_channel, target_channels, last_message_id, phone_number,
                        is_public_channel, post_freshness, is_active
                    ) VALUES (?, ?, 0, ?, 1, 0, 1)
                    """,
                    (donor, json.dumps(targets), "")
                )
        else:
            await db.execute(
                """
                INSERT INTO repost_streams (
                    donor_channel, target_channels, last_message_id, phone_number,
                    is_public_channel, post_freshness, is_active
                ) VALUES (?, ?, 0, ?, 1, 0, 1)
                """,
                (donor_channel, json.dumps(targets), "")
            )
        await db.commit()
    await state.clear()
    # Покажем главное меню сразу после создания потока
    user_info = await get_user_info(user_id, username)
    await callback.message.edit_text("✅ Поток репостов создан!", reply_markup=get_main_menu_keyboard(user_info))

async def create_channels_for_account(callback: types.CallbackQuery, state: FSMContext):
    # Переиспользуем существующий флоу создания каналов
    phone = callback.data.split("_")[-1]
    await state.update_data(channel_create_phone=phone)
    await channel_generate_many(callback, state)

async def delete_channels_for_account(callback: types.CallbackQuery):
    # Покажем список каналов для удаления
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    channels = []
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT channel_id, channel_title FROM channels")
        channels = await cursor.fetchall()
    phone = callback.data.split("_")[-1]
    await callback.message.edit_text("Выберите каналы для удаления:", reply_markup=get_channels_list_keyboard(channels, phone))

async def delete_channel_for_account(callback: types.CallbackQuery):
    # delete_channel_{channel_id}_{phone}
    parts = callback.data.split("_")
    try:
        channel_id = int(parts[2])
        phone = parts[3]
    except Exception:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    # Удаляем канал через Pyrogram и из БД
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    # Получаем session_string для телефона
    session_string = None
    async with aiosqlite.connect(db_path) as db:
        try:
            cur = await db.execute("SELECT session_string FROM linked_accounts WHERE phone_number = ?", (phone,))
            row = await cur.fetchone()
            session_string = row[0] if row else None
        except Exception:
            session_string = None
    if not session_string:
        await callback.answer("Нет сессии для аккаунта", show_alert=True)
    else:
        client = Client("delete_channel", api_id=Config.API_ID, api_hash=Config.API_HASH, session_string=session_string, in_memory=True)
        try:
            await client.start()
            try:
                await client.delete_channel(channel_id)
            except Exception as e:
                # Если метод недоступен, просто игнорируем удаление на стороне Telegram
                pass
        finally:
            try:
                await client.stop()
            except Exception:
                pass
    # Удаляем запись из БД
    async with aiosqlite.connect(db_path) as db:
        await db.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
        await db.commit()
    await callback.answer("Канал удалён")
    # Обновляем список каналов
    channels = []
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT channel_id, channel_title FROM channels")
        channels = await cursor.fetchall()
    await callback.message.edit_text("Выберите каналы для удаления:", reply_markup=get_channels_list_keyboard(channels, phone))

async def manage_posts_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("Управлять постами:", reply_markup=get_manage_posts_keyboard())

# --- Missing binding menu handler ---
async def manage_binding_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("Раздел управления:", reply_markup=get_manage_binding_keyboard())

# --- Compatibility wrappers for removed/renamed handlers ---
async def resend_code_removed(callback: types.CallbackQuery, state: FSMContext):
    return await resend_code(callback, state)

async def manage_accounts_removed(callback: types.CallbackQuery):
    # Redirect old callback to the new accounts menu
    return await manage_accounts_menu(callback)

async def unlink_account_removed(callback: types.CallbackQuery):
    # Keep supporting old callback prefix by delegating to the current handler
    return await unlink_account(callback)

async def admin_link_main_account_removed(callback: types.CallbackQuery, state: FSMContext):
    # Delegate to the current link flow
    return await admin_link_main_account(callback, state)

# --- Added missing channel management handlers ---
async def manage_channels_menu(callback: types.CallbackQuery):
    """Show accounts to pick for channel management."""
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    accounts: list[tuple[str]] = []
    async with aiosqlite.connect(db_path) as db:
        try:
            cursor = await db.execute("SELECT phone_number FROM linked_accounts")
            rows = await cursor.fetchall()
            accounts = [(r[0],) for r in rows]
        except Exception:
            accounts = []
    await callback.message.edit_text(
        "📡 Выберите аккаунт для управления каналами:",
        reply_markup=get_accounts_for_channels_keyboard(accounts)
    )

async def manage_channels_for_account(callback: types.CallbackQuery):
    """Render channel actions for a chosen account."""
    phone = callback.data.replace("manage_channels_for_", "")
    await callback.message.edit_text(
        f"📡 Аккаунт {phone}: управление каналами",
        reply_markup=get_manage_channels_for_account_keyboard(phone)
    )

# Fallback to ensure name exists at runtime
if 'manage_accounts_menu' not in globals():
    async def manage_accounts_menu(callback: types.CallbackQuery):
        await callback.message.edit_text("👥 Управлять аккаунтами:", reply_markup=get_accounts_menu_keyboard())

async def accounts_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)
    db_path = await get_user_db_path(user_id, username)
    accounts = []
    async with aiosqlite.connect(db_path) as db:
        try:
            cursor = await db.execute("SELECT phone_number FROM linked_accounts")
            rows = await cursor.fetchall()
            accounts = [(r[0],) for r in rows]
        except Exception:
            accounts = []
    await callback.message.edit_text("📋 Привязанные аккаунты:", reply_markup=get_accounts_list_keyboard(accounts))

async def donor_count_one(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(allow_multiple_donors=False)
    await callback.message.edit_text("🎲 Выберите тип донора:", reply_markup=get_donor_type_keyboard())

async def donor_count_many(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(allow_multiple_donors=True)
    await callback.message.edit_text("🎲 Выберите тип донора:", reply_markup=get_donor_type_keyboard())

async def periodic_count_one(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(periodic_allow_multiple=False)
    await callback.message.edit_text("🔁 Источник для потока репостов:", reply_markup=get_periodic_source_keyboard())

async def periodic_count_many(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(periodic_allow_multiple=True)
    await callback.message.edit_text("🔁 Источник для потока репостов:", reply_markup=get_periodic_source_keyboard())
