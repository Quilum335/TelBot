# keyboards.py - Клавиатуры

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
import calendar
from datetime import datetime

def get_main_menu_keyboard(license_info: dict = None):
    """Главное меню"""
    keyboard = InlineKeyboardBuilder()
    
    # Добавляем информацию о лицензии, если предоставлена
    if license_info:
        if license_info['has_subscription']:
            if license_info['is_trial']:
                keyboard.row(InlineKeyboardButton(
                    text=f"🆓 Пробный период: {license_info['days_left']} дней", 
                    callback_data="buy_license"
                ))
            else:
                keyboard.row(InlineKeyboardButton(
                    text=f"✅ Лицензия: {license_info['days_left']} дней", 
                    callback_data="buy_license"
                ))
        else:
            keyboard.row(InlineKeyboardButton(
                text="❌ Лицензия истекла", 
                callback_data="buy_license"
            ))
    
    keyboard.row(
        InlineKeyboardButton(text="📝 Создать отложенный пост", callback_data="create_post")
    )
    keyboard.row(
        InlineKeyboardButton(text="🔗 Привязать канал", callback_data="link_channel")
    )
    keyboard.row(
        InlineKeyboardButton(text="👤 Управлять привязкой", callback_data="manage_binding")
    )
    keyboard.row(
        InlineKeyboardButton(text="📋 Список каналов", callback_data="list_channels")
    )
    keyboard.row(
        InlineKeyboardButton(text="📋 Запланированные посты", callback_data="scheduled_posts")
    )
    keyboard.row(
        InlineKeyboardButton(text="💳 Купить лицензию", callback_data="buy_license")
    )
    return keyboard.as_markup()

def get_post_type_keyboard():
    """Выбор типа поста"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="✍️ Ручной режим", callback_data="post_manual"),
        InlineKeyboardButton(text="🔄 Автоматический режим", callback_data="post_auto")
    )
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")
    )
    return keyboard.as_markup()

def get_auto_post_keyboard():
    """Выбор автоматического режима"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🎲 Рандомно", callback_data="auto_random"),
        InlineKeyboardButton(text="🔁 Поток репостов", callback_data="auto_periodic")
    )
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="create_post")
    )
    return keyboard.as_markup()

def get_auto_source_keyboard():
    """Выбор источника постов для автоматического режима"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔗 Из привязанных каналов", callback_data="auto_source_linked"),
        InlineKeyboardButton(text="🌐 Из открытых каналов", callback_data="auto_source_public")
    )
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="post_auto")
    )
    return keyboard.as_markup()

def get_periodic_source_keyboard():
    """Подменю выбора источника для потока репостов"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔗 Из привязанных каналов", callback_data="periodic_source_linked"),
        InlineKeyboardButton(text="🌐 Из публичного канала", callback_data="periodic_source_public")
    )
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="post_auto")
    )
    return keyboard.as_markup()

def get_channel_sort_keyboard():
    """Сортировка каналов"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔤 По алфавиту", callback_data="sort_alpha")
    )
    keyboard.row(
        InlineKeyboardButton(text="📊 По числу постов", callback_data="sort_posts")
    )
    keyboard.row(
        InlineKeyboardButton(text="👥 По подписчикам", callback_data="sort_subscribers")
    )
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")
    )
    return keyboard.as_markup()

def get_admin_menu_keyboard():
    """Админское меню"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Список пользователей", callback_data="admin_users")],
        [InlineKeyboardButton(text="🔗 Привязать основной аккаунт", callback_data="admin_link_main_account")],
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_to_menu")]
    ])

def get_license_duration_keyboard():
    """Выбор длительности лицензии"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="1 день", callback_data="license_1d")
    )
    keyboard.row(
        InlineKeyboardButton(text="7 дней", callback_data="license_7d")
    )
    keyboard.row(
        InlineKeyboardButton(text="2 недели", callback_data="license_14d")
    )
    keyboard.row(
        InlineKeyboardButton(text="1 месяц", callback_data="license_30d")
    )
    keyboard.row(
        InlineKeyboardButton(text="1 год", callback_data="license_365d")
    )
    keyboard.row(
        InlineKeyboardButton(text="♾ Бессрочно", callback_data="license_forever")
    )
    keyboard.row(
        InlineKeyboardButton(text="❌ Удалить лицензию", callback_data="license_delete")
    )
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="admin_licenses")
    )
    return keyboard.as_markup()

def get_license_duration_keyboard_with_username(username: str):
    """Выбор длительности лицензии с передачей username в callback data"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="1 день", callback_data=f"license_1d_{username}")
    )
    keyboard.row(
        InlineKeyboardButton(text="7 дней", callback_data=f"license_7d_{username}")
    )
    keyboard.row(
        InlineKeyboardButton(text="2 недели", callback_data=f"license_14d_{username}")
    )
    keyboard.row(
        InlineKeyboardButton(text="1 месяц", callback_data=f"license_30d_{username}")
    )
    keyboard.row(
        InlineKeyboardButton(text="1 год", callback_data=f"license_365d_{username}")
    )
    keyboard.row(
        InlineKeyboardButton(text="♾ Бессрочно", callback_data=f"license_forever_{username}")
    )
    keyboard.row(
        InlineKeyboardButton(text="❌ Удалить лицензию", callback_data=f"license_delete_{username}")
    )
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="admin_licenses")
    )
    return keyboard.as_markup()

def get_license_status_keyboard(license_info: dict):
    """Клавиатура с информацией о статусе лицензии"""
    keyboard = InlineKeyboardBuilder()
    
    if license_info['has_subscription']:
        if license_info['is_trial']:
            keyboard.row(InlineKeyboardButton(
                text=f"🆓 Пробный период: {license_info['days_left']} дней", 
                callback_data="buy_license"
            ))
        else:
            keyboard.row(InlineKeyboardButton(
                text=f"✅ Лицензия: {license_info['days_left']} дней", 
                callback_data="buy_license"
            ))
    else:
        keyboard.row(InlineKeyboardButton(
            text="❌ Лицензия истекла", 
            callback_data="buy_license"
        ))
    
    return keyboard.as_markup()

# Удалены клавиатуры управления привязкой аккаунтов
def get_manage_binding_keyboard():
    """Главное меню раздела Управлять привязкой"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="👥 Управлять аккаунтами", callback_data="manage_accounts_menu"))
    keyboard.row(InlineKeyboardButton(text="📡 Управлять каналами", callback_data="manage_channels_menu"))
    keyboard.row(InlineKeyboardButton(text="📝 Управлять постами", callback_data="manage_posts_menu"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu"))
    return keyboard.as_markup()

def get_accounts_menu_keyboard():
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="🔗 Привязать аккаунт", callback_data="link_account"))
    keyboard.row(InlineKeyboardButton(text="📋 Список аккаунтов", callback_data="accounts_list"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="manage_binding"))
    return keyboard.as_markup()

def get_manage_accounts_keyboard(accounts: list[tuple[str, bool]]):
    """Клавиатура управления привязанными аккаунтами. Передаётся список пар (phone, is_main)."""
    keyboard = InlineKeyboardBuilder()
    if accounts:
        for (phone, is_main) in accounts:
            main_label = " (основной)" if is_main else ""
            keyboard.row(
                InlineKeyboardButton(text=f"🔗 Сделать основным{main_label}", callback_data=f"set_main_{phone}"),
                InlineKeyboardButton(text=f"🗑 Отвязать {phone}", callback_data=f"unlink_account_{phone}")
            )
    else:
        keyboard.row(InlineKeyboardButton(text="Нет аккаунтов", callback_data="ignore"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="manage_binding"))
    return keyboard.as_markup()

def get_accounts_list_keyboard(accounts: list[tuple[str]]):
    keyboard = InlineKeyboardBuilder()
    if accounts:
        for (phone,) in accounts:
            keyboard.row(InlineKeyboardButton(text=f"❌ Отвязать {phone}", callback_data=f"unlink_account_{phone}"))
    else:
        keyboard.row(InlineKeyboardButton(text="Нет аккаунтов", callback_data="ignore"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="manage_accounts_menu"))
    return keyboard.as_markup()

def get_accounts_for_channels_keyboard(accounts: list[tuple[str]]):
    keyboard = InlineKeyboardBuilder()
    if accounts:
        for (phone,) in accounts:
            keyboard.row(InlineKeyboardButton(text=f"{phone}", callback_data=f"manage_channels_for_{phone}"))
    else:
        keyboard.row(InlineKeyboardButton(text="Нет аккаунтов", callback_data="ignore"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="manage_binding"))
    return keyboard.as_markup()

def get_manage_channels_for_account_keyboard(phone: str):
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="➕ Создать каналы", callback_data=f"create_channels_for_{phone}"))
    keyboard.row(InlineKeyboardButton(text="🗑 Удалить каналы", callback_data=f"delete_channels_for_{phone}"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="manage_channels_menu"))
    return keyboard.as_markup()

def get_channels_list_keyboard(channels: list[tuple[int, str]], phone: str):
    keyboard = InlineKeyboardBuilder()
    if channels:
        for channel_id, channel_title in channels:
            title = channel_title or str(channel_id)
            keyboard.row(InlineKeyboardButton(text=f"🗑 {title}", callback_data=f"delete_channel_{channel_id}_{phone}"))
    else:
        keyboard.row(InlineKeyboardButton(text="Нет каналов", callback_data="ignore"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"manage_channels_for_{phone}"))
    return keyboard.as_markup()

def get_manage_posts_keyboard():
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="📝 Создать пост", callback_data="create_post"))
    keyboard.row(InlineKeyboardButton(text="📋 Запланированные посты", callback_data="scheduled_posts"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="manage_binding"))
    return keyboard.as_markup()

def get_channel_name_method_keyboard():
    """Клавиатура для выбора метода создания названия канала"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Сгенерировать случайное", callback_data="channel_generate_one")],
        [InlineKeyboardButton(text="🎲 Сгенерировать несколько", callback_data="channel_generate_many")],
        [InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="channel_name_input")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="create_channel_start")]
    ])

def get_post_freshness_keyboard():
    """Клавиатура для выбора свежести постов"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" День", callback_data="freshness_1")],
        [InlineKeyboardButton(text=" Неделя", callback_data="freshness_7")],
        [InlineKeyboardButton(text=" 2 Недели", callback_data="freshness_14")],
        [InlineKeyboardButton(text=" Месяц", callback_data="freshness_30")],
        [InlineKeyboardButton(text=" 3 месяца", callback_data="freshness_90")],
        [InlineKeyboardButton(text=" 6 месяцев", callback_data="freshness_180")],
        [InlineKeyboardButton(text=" Год", callback_data="freshness_365")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_previous")]
    ])

def create_calendar(year: int, month: int) -> InlineKeyboardMarkup:
    """Создание календаря"""
    keyboard = InlineKeyboardBuilder()
    
    # Заголовок с месяцем и годом
    month_name = calendar.month_name[month]
    keyboard.row(InlineKeyboardButton(text=f"{month_name} {year}", callback_data="ignore"))
    
    # Дни недели
    weekdays = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    keyboard.row(*[InlineKeyboardButton(text=day, callback_data="ignore") for day in weekdays])
    
    # Получаем календарь
    cal = calendar.monthcalendar(year, month)
    
    for week in cal:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="ignore"))
            else:
                date_str = f"{year:04d}-{month:02d}-{day:02d}"
                row.append(InlineKeyboardButton(text=str(day), callback_data=f"date_{date_str}"))
        keyboard.row(*row)
    
    # Кнопки навигации
    nav_row = []
    if month > 1:
        prev_month = month - 1
        prev_year = year
    else:
        prev_month = 12
        prev_year = year - 1
    
    if month < 12:
        next_month = month + 1
        next_year = year
    else:
        next_month = 1
        next_year = year + 1
    
    nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"month_{prev_year}_{prev_month}"))
    nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"month_{next_year}_{next_month}"))
    keyboard.row(*nav_row)
    
    return keyboard.as_markup()

def get_scheduled_posts_keyboard():
    """Клавиатура для управления запланированными постами"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="📝 Единичные посты", callback_data="scheduled_posts_single"),
        InlineKeyboardButton(text="🔄 Потоки репостов", callback_data="scheduled_posts_streams")
    )
    keyboard.row(
        InlineKeyboardButton(text="🎲 Рандомные посты", callback_data="scheduled_posts_random")
    )
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")
    )
    return keyboard.as_markup()

def get_post_action_keyboard(post_id: int, post_type: str):
    """Клавиатура действий для поста"""
    keyboard = InlineKeyboardBuilder()
    # Текст удаления зависит от типа
    delete_text = "❌ Отменить публикации" if post_type in ("random_stream", "random_stream_config") else "❌ Отменить пост"
    actions_row = [InlineKeyboardButton(text=delete_text, callback_data=f"delete_post_{post_type}_{post_id}")]
    # Кнопка смены донора доступна для всех типов
    actions_row.append(InlineKeyboardButton(text="🔄 Сменить донора", callback_data=f"change_donor_{post_type}_{post_id}"))
    keyboard.row(*actions_row)
    keyboard.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data="scheduled_posts")
    )
    return keyboard.as_markup()

def get_confirm_delete_keyboard(post_id: int, post_type: str):
    """Клавиатура подтверждения удаления"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_{post_type}_{post_id}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"post_action_{post_type}_{post_id}")
    )
    return keyboard.as_markup()

def get_channel_type_keyboard():
    """Клавиатура для выбора типа канала"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="🔓 Открытый канал", callback_data="channel_type_public"),
        InlineKeyboardButton(text="🔒 Закрытый канал", callback_data="channel_type_private")
    )
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="create_channel_start"))
    return keyboard.as_markup()

def get_channel_count_keyboard():
    """Клавиатура для выбора количества каналов"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="1 канал", callback_data="channel_count_1"),
        InlineKeyboardButton(text="5 каналов", callback_data="channel_count_5"),
        InlineKeyboardButton(text="10 каналов", callback_data="channel_count_10")
    )
    keyboard.row(
        InlineKeyboardButton(text="15 каналов", callback_data="channel_count_15"),
        InlineKeyboardButton(text="20 каналов", callback_data="channel_count_20")
    )
    keyboard.row(InlineKeyboardButton(text="Ввести вручную", callback_data="channel_count_custom"))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="create_channel_start"))
    return keyboard.as_markup()

def get_donor_type_keyboard():
    """Клавиатура для выбора типа донора"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="📋 Привязанные каналы", callback_data="donor_type_linked"),
        InlineKeyboardButton(text="🌐 Публичные каналы", callback_data="donor_type_public")
    )
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="auto_random"))
    return keyboard.as_markup()

def get_donor_count_keyboard():
    """Клавиатура выбора количества доноров: один или несколько"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="Один донор", callback_data="donor_count_one"),
        InlineKeyboardButton(text="Несколько доноров", callback_data="donor_count_many"),
    )
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="auto_random"))
    return keyboard.as_markup()

def get_periodic_donor_count_keyboard():
    """Клавиатура выбора количества доноров для потоков (repost streams)"""
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="Один донор", callback_data="periodic_count_one"),
        InlineKeyboardButton(text="Несколько доноров", callback_data="periodic_count_many"),
    )
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data="auto_periodic"))
    return keyboard.as_markup()

def get_donors_confirm_keyboard(done_callback: str, back_callback: str):
    keyboard = InlineKeyboardBuilder()
    keyboard.row(InlineKeyboardButton(text="✅ Готово", callback_data=done_callback))
    keyboard.row(InlineKeyboardButton(text="◀️ Назад", callback_data=back_callback))
    return keyboard.as_markup()