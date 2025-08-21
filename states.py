# states.py - Состояния FSM

from aiogram.fsm.state import State, StatesGroup

class PostStates(StatesGroup):
    waiting_for_time = State()
    waiting_for_date = State()
    waiting_for_channel = State()
    waiting_for_content = State()
    waiting_for_repost = State()
    waiting_for_donor_channel = State()
    waiting_for_period = State()
    waiting_for_once_donor = State()
    waiting_for_once_target = State()
    waiting_for_once_post = State()
    waiting_for_periodic_donor_select = State()
    waiting_for_auto_targets = State()
    waiting_for_auto_source = State()
    waiting_for_public_channel_input = State()
    # Новые состояния для рандомных постов
    waiting_for_random_donors = State()
    waiting_for_random_targets = State()
    waiting_for_random_interval = State()
    waiting_for_random_posts_per_day = State()
    waiting_for_random_freshness = State()
    waiting_for_multiple_donors = State()
    
    # Состояния для выбора свежести постов
    waiting_for_post_freshness = State()
    
    # Состояния для подтверждения создания постов
    waiting_for_confirm_periodic = State()
    waiting_for_confirm_random = State()
    waiting_for_confirm_single = State()

class AccountStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()

class AdminStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_license_duration = State()
    waiting_for_ban_user_id = State()
    waiting_for_search_query = State()
    waiting_for_name_search = State()
    waiting_for_password = State()
    waiting_for_username_by_admin = State()

class ChannelStates(StatesGroup):
    waiting_for_channel_selection = State()

class ChannelCreateStates(StatesGroup):
    waiting_for_account = State()
    waiting_for_channel_type = State()  # Новое: выбор типа канала (открытый/закрытый)
    waiting_for_channel_count = State()  # Новое: количество каналов
    waiting_for_name_method = State()
    waiting_for_channel_name = State()
    waiting_for_generate_count = State()

class ScheduledPostsStates(StatesGroup):
    waiting_for_post_action = State()
    waiting_for_new_donor = State()
    waiting_for_confirm_delete = State()