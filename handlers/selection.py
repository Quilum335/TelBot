# handlers/selection.py - Вспомогательные функции выбора каналов/доноров

from aiogram import types
from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
import aiosqlite
import logging
from database import get_user_db_path

logger = logging.getLogger(__name__)

async def toggle_id_in_state_list(state, key: str, id_value: int) -> list[int]:
    """Переключение ID в списке состояния и возврат обновленного списка."""
    data = await state.get_data()
    current: list[int] = data.get(key, [])
    if id_value in current:
        current.remove(id_value)
        toggled_added = False
    else:
        current.append(id_value)
        toggled_added = True
    await state.update_data(**{key: current})
    return current, toggled_added

async def fetch_user_channels(user_id: int, username: str, exclude_ids: list[int] | None = None):
    """Получить список каналов пользователя с необязательным исключением по ID."""
    db_path = await get_user_db_path(user_id, username)
    query = "SELECT channel_id, channel_title FROM channels"
    params = []
    if exclude_ids:
        placeholders = ",".join(["?"] * len(exclude_ids))
        query += f" WHERE channel_id NOT IN ({placeholders})"
        params = exclude_ids
    try:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute(query, params)
            channels = await cursor.fetchall()
        return channels
    except Exception:
        logger.exception("Ошибка получения списка каналов", extra={"user_id": user_id})
        return []

async def render_select_list(callback_or_message, items, selected_ids: list[int],
                             build_callback_prefix: str, title_text: str,
                             done_callback: str, back_callback: str):
    """Отрисовать список выбора с отметками и кнопками Завершить/Назад.
    Принимает как CallbackQuery, так и Message.
    """
    keyboard = InlineKeyboardBuilder()
    for item_id, item_title in items:
        is_selected = item_id in selected_ids
        mark = "✅" if is_selected else "➕"
        keyboard.row(InlineKeyboardButton(text=f"{mark} {item_title}", callback_data=f"{build_callback_prefix}_{item_id}"))
    keyboard.row(InlineKeyboardButton(text="✅ Выбрать все", callback_data=f"{build_callback_prefix}_all"))
    keyboard.row(InlineKeyboardButton(text="✅ Завершить", callback_data=done_callback))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data=back_callback))

    if hasattr(callback_or_message, 'message'):
        await callback_or_message.message.edit_text(title_text, reply_markup=keyboard.as_markup())
    else:
        await callback_or_message.answer(title_text, reply_markup=keyboard.as_markup()) 