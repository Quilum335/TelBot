import os
import logging
from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired
import aiosqlite

from config import Config
from database import get_user_db_path
from states import AccountStates

logger = logging.getLogger(__name__)

# Глобальное хранилище клиентов для предотвращения пересоздания сессий
active_clients = {}

async def link_account(callback: types.CallbackQuery, state: FSMContext):
    """Привязка аккаунта"""
    await callback.message.edit_text(
        "📱 Для привязки аккаунта введите номер телефона в международном формате\n"
        "Например: +79123456789"
    )
    await state.set_state(AccountStates.waiting_for_phone)

async def process_phone(message: types.Message, state: FSMContext):
    """Обработка номера телефона"""
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Номер должен начинаться с +")
        return
    user_id = message.from_user.id
    # Закрываем предыдущий клиент если существует
    if user_id in active_clients:
        try:
            await active_clients[user_id].disconnect()
        except Exception:
            logger.exception("Ошибка отключения предыдущего клиента Pyrogram", extra={"user_id": user_id})
        del active_clients[user_id]
    session_name = os.path.join(Config.SESSIONS_DIR, f"user_{user_id}")
    # Удаляем старые файлы сессии
    for ext in ['.session', '.session-journal']:
        session_file = f"{session_name}{ext}"
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
            except Exception:
                logger.exception("Ошибка удаления файла сессии", extra={"file": session_file})
    # Создаем новый клиент и сохраняем его
    client = Client(
        session_name, 
        api_id=Config.API_ID, 
        api_hash=Config.API_HASH,
        in_memory=True  # Используем сессию в памяти
    )
    active_clients[user_id] = client
    try:
        await client.connect()
        sent_code = await client.send_code(phone)
        # Сохраняем данные в состоянии
        await state.update_data(
            phone=phone,
            phone_code_hash=sent_code.phone_code_hash,
            session_name=session_name
        )
        await message.answer(
            "📨 Код подтверждения отправлен на ваш телефон.\n"
            "Введите его в течение 5 минут:"
        )
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
        if user_id in active_clients:
            try:
                await active_clients[user_id].disconnect()
            except Exception:
                logger.exception("Ошибка при отключении клиента после исключения", extra={"user_id": user_id})
            del active_clients[user_id]
        await state.clear()

async def process_code(message: types.Message, state: FSMContext):
    """Обработка кода подтверждения"""
    code = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    # Используем существующий клиент
    if user_id not in active_clients:
        await message.answer(
            "❌ Сессия истекла. Начните процесс заново.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Начать заново", callback_data="link_account")],
                [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_menu")]
            ])
        )
        await state.clear()
        return
    client = active_clients[user_id]
    try:
        # Пробуем войти с кодом
        await client.sign_in(
            phone_number=data['phone'],
            phone_code_hash=data['phone_code_hash'],
            phone_code=code
        )
        # Успешный вход - сохраняем сессию
        session_string = await client.export_session_string()
        username = message.from_user.username or str(user_id)
        db_path = await get_user_db_path(user_id, username)
        async with aiosqlite.connect(db_path) as db:
            # Удаляем старые записи этого номера
            await db.execute(
                "DELETE FROM linked_accounts WHERE phone_number = ?",
                (data['phone'],)
            )
            # Добавляем новую
            await db.execute('''
                INSERT INTO linked_accounts (phone_number, session_string)
                VALUES (?, ?)
            ''', (data['phone'], session_string))
            await db.commit()
        await message.answer(
            "✅ Аккаунт успешно привязан!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_menu")]
            ])
        )
        # Закрываем клиент и удаляем из активных
        await client.disconnect()
        del active_clients[user_id]
        await state.clear()
    except PhoneCodeInvalid:
        await message.answer("❌ Неверный код. Попробуйте еще раз:")
    except PhoneCodeExpired:
        await message.answer(
            "❌ Код истёк. Нажмите кнопку ниже, чтобы запросить новый код.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Запросить новый код", callback_data="resend_code")],
                [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_menu")]
            ])
        )
    except SessionPasswordNeeded:
        await message.answer("🔐 Требуется пароль двухфакторной аутентификации. Введите его:")
        await state.set_state(AccountStates.waiting_for_password)
    except Exception as e:
        error_msg = str(e)
        if "PHONE_CODE_EXPIRED" in error_msg:
            await message.answer(
                "❌ Код истёк. Нажмите кнопку ниже, чтобы запросить новый код.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Запросить новый код", callback_data="resend_code")],
                    [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_menu")]
                ])
            )
        elif "PHONE_NUMBER_UNOCCUPIED" in error_msg:
            await message.answer(
                "❌ Этот номер не зарегистрирован в Telegram.\n"
                "Сначала зарегистрируйте аккаунт в приложении Telegram."
            )
            if user_id in active_clients:
                try:
                    await active_clients[user_id].disconnect()
                except Exception:
                    logger.exception("Ошибка отключения клиента после ошибки PHONE_NUMBER_UNOCCUPIED", extra={"user_id": user_id})
                del active_clients[user_id]
            await state.clear()
        else:
            await message.answer(f"❌ Ошибка: {error_msg}")
            # Не удаляем клиент, чтобы можно было попробовать еще раз

async def resend_code(callback: types.CallbackQuery, state: FSMContext):
    """Повторная отправка кода"""
    data = await state.get_data()
    phone = data.get('phone')
    user_id = callback.from_user.id
    if not phone:
        await callback.message.edit_text(
            "❌ Не найден номер телефона. Начните процесс заново.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Начать заново", callback_data="link_account")],
                [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_menu")]
            ])
        )
        await state.clear()
        return
    # Используем существующий клиент или создаем новый
    if user_id not in active_clients:
        session_name = os.path.join(Config.SESSIONS_DIR, f"user_{user_id}")
        client = Client(
            session_name,
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            in_memory=True
        )
        active_clients[user_id] = client
        await client.connect()
    else:
        client = active_clients[user_id]
    try:
        # Отправляем новый код
        sent_code = await client.resend_code(phone, data.get('phone_code_hash'))
        # Обновляем phone_code_hash
        await state.update_data(phone_code_hash=sent_code.phone_code_hash)
        await callback.message.edit_text(
            "📨 Новый код отправлен на ваш телефон.\n"
            "Введите его:"
        )
        await state.set_state(AccountStates.waiting_for_code)
    except Exception as e:
        await callback.message.edit_text(
            f"❌ Ошибка при отправке кода: {str(e)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Начать заново", callback_data="link_account")],
                [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_menu")]
            ])
        )
        if user_id in active_clients:
            try:
                await active_clients[user_id].disconnect()
            except Exception:
                logger.exception("Ошибка отключения клиента после сбоя resend_code", extra={"user_id": user_id})
            del active_clients[user_id]
        await state.clear()

async def process_password(message: types.Message, state: FSMContext):
    """Обработка пароля двухфакторной аутентификации"""
    password = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    if user_id not in active_clients:
        await message.answer(
            "❌ Сессия истекла. Начните процесс заново.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Начать заново", callback_data="link_account")],
                [InlineKeyboardButton(text="◀️ Отмена", callback_data="back_to_menu")]
            ])
        )
        await state.clear()
        return
    client = active_clients[user_id]
    try:
        # Проверяем пароль
        await client.check_password(password)
        # Сохраняем сессию
        session_string = await client.export_session_string()
        username = message.from_user.username or str(user_id)
        db_path = await get_user_db_path(user_id, username)
        async with aiosqlite.connect(db_path) as db:
            await db.execute('''
                INSERT INTO linked_accounts (phone_number, session_string)
                VALUES (?, ?)
            ''', (data['phone'], session_string))
            await db.commit()
        await message.answer(
            "✅ Аккаунт успешно привязан!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ В меню", callback_data="back_to_menu")]
            ])
        )
        # Закрываем клиент
        await client.disconnect()
        del active_clients[user_id]
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}\nПопробуйте еще раз:")

# Функция очистки неактивных клиентов (вызывать периодически)
async def cleanup_inactive_clients():
    """Очистка неактивных клиентов"""
    for user_id in list(active_clients.keys()):
        try:
            client = active_clients[user_id]
            if not client.is_connected:
                del active_clients[user_id]
        except Exception:
            logger.exception("Ошибка очистки неактивных клиентов (auth_handler)", extra={"user_id": user_id})
            del active_clients[user_id] 