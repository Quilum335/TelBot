# config.py - Конфигурация с использованием переменных окружения

import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

class Config:
    # Основные настройки
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")
    API_ID = int(os.getenv("API_ID", "0"))
    API_HASH = os.getenv("API_HASH", "")
    
    # Админские настройки
    ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "@admin")
    ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x]
    ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "564654664")
    
    # Настройки базы данных
    DB_DIR = os.getenv("DB_DIR", "databases")
    SESSIONS_DIR = os.getenv("SESSIONS_DIR", "sessions")
    
    # Настройки подписки
    TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "7"))
    
    # Настройки планировщика
    POST_CHECK_INTERVAL = int(os.getenv("POST_CHECK_INTERVAL", "15"))  # секунды (изменено на 60)
    PERIODIC_CHECK_INTERVAL = int(os.getenv("PERIODIC_CHECK_INTERVAL", "15"))  # секунды
    DONOR_CHECK_INTERVAL = int(os.getenv("DONOR_CHECK_INTERVAL", "15"))  # секунды (уменьшено с 60 до 30)
    RANDOM_POST_CHECK_INTERVAL = int(os.getenv("RANDOM_POST_CHECK_INTERVAL", "15"))  # секунды для рандомных постов
    # Безопасные ограничения
    # Минимальный интервал между публикациями в одном канале (в секундах)
    # По умолчанию лимиты между публикациями выключены (0) — дубликаты обрабатываются через дедуп и хеши
    MIN_SECONDS_BETWEEN_POSTS_PER_CHANNEL = int(os.getenv("MIN_SECONDS_BETWEEN_POSTS_PER_CHANNEL", "0"))
    # Жёсткий максимум постов в канал за сутки (0 = выключено)
    MAX_POSTS_PER_CHANNEL_PER_DAY = int(os.getenv("MAX_POSTS_PER_CHANNEL_PER_DAY", "0"))
    
    @classmethod
    def get_session_string(cls):
        """Получение session_string из файла"""
        session_file = os.path.join(cls.SESSIONS_DIR, "session_string.txt")
        if os.path.exists(session_file):
            with open(session_file, "r") as f:
                return f.read().strip()
        return None
    
    @classmethod
    def validate(cls):
        """Проверка обязательных настроек"""
        if not cls.BOT_TOKEN:
            raise ValueError("BOT_TOKEN не установлен!")
        if not cls.API_ID or not cls.API_HASH:
            raise ValueError("API_ID и API_HASH должны быть установлены!")
        if not cls.ADMIN_IDS:
            raise ValueError("ADMIN_IDS не установлен!")
        
        # Создаем необходимые директории
        os.makedirs(cls.DB_DIR, exist_ok=True)
        os.makedirs(cls.SESSIONS_DIR, exist_ok=True)