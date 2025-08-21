# main.py - Главный файл бота

import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ChatMemberUpdated
from aiogram.filters import BaseFilter

from config import Config
# Compatibility shim for tgcrypto: some builds expose ige_decrypt/ige_encrypt
# while pyrogram expects ige256_decrypt/ige256_encrypt. Alias if missing so
# auth/session creation doesn't fail on some environments.
try:
    import tgcrypto
    if not hasattr(tgcrypto, 'ige256_decrypt') and hasattr(tgcrypto, 'ige_decrypt'):
        tgcrypto.ige256_decrypt = tgcrypto.ige_decrypt
    if not hasattr(tgcrypto, 'ige256_encrypt') and hasattr(tgcrypto, 'ige_encrypt'):
        tgcrypto.ige256_encrypt = tgcrypto.ige_encrypt
except Exception:
    # If tgcrypto isn't installed or another error occurs, let pyrogram fall back
    pass

# Import PostScheduler (imports pyrogram internally)
from schu import PostScheduler  # Corrected import path


logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    # Гарантируем существование директории для логов
    os.makedirs('logs', exist_ok=True)
    file_handler = RotatingFileHandler('logs/bot.log', maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8')
    file_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=Config.BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Фильтр для проверки добавления бота в канал
class BotAddedToChannel(BaseFilter):
    async def __call__(self, event: ChatMemberUpdated) -> bool:
        return (
            event.new_chat_member.user.id == bot.id and
            event.new_chat_member.status in ['administrator', 'member']
        )

async def check_license_notifications():
    """Проверка и отправка уведомлений об истечении лицензий"""
    from database import notify_expired_licenses

    while True:
        try:
            await notify_expired_licenses(bot)
            # Проверяем раз в день
            await asyncio.sleep(24 * 60 * 60)
        except Exception as e:
            logging.getLogger(__name__).exception("Ошибка в check_license_notifications")
            await asyncio.sleep(60 * 60)  # Ждем час при ошибке

async def cleanup_clients():
    """Периодическая очистка неактивных клиентов Pyrogram"""
    while True:
        await asyncio.sleep(300)  # каждые 5 минут
        try:
            from handlers.core import cleanup_inactive_clients
            await cleanup_inactive_clients()
        except Exception as e:
            logging.getLogger(__name__).exception("Ошибка очистки клиентов")

async def cleanup_past_posts_periodic():
    """Периодическая очистка прошедших постов"""
    while True:
        await asyncio.sleep(1800)  # каждые 30 минут
        try:
            from database import cleanup_past_posts
            if os.path.exists(Config.DB_DIR):
                for filename in os.listdir(Config.DB_DIR):
                    if filename.endswith(".db"):
                        db_path = os.path.join(Config.DB_DIR, filename)
                        await cleanup_past_posts(db_path)
        except Exception:
            logging.getLogger(__name__).exception("Ошибка очистки прошедших постов")

async def generate_daily_random_posts():
    """Ежедневная генерация новых рандомных постов в 00:00"""
    while True:
        try:
            # Ждем до следующего дня в 00:00
            now = datetime.now()
            tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

            # Вычисляем время до следующего дня в 00:00
            time_until_tomorrow = (tomorrow - now).total_seconds()

            if time_until_tomorrow > 0:
                logging.getLogger(__name__).info(
                    f"Ожидание до следующего дня для генерации рандомных постов: {time_until_tomorrow} секунд"
                )
                await asyncio.sleep(time_until_tomorrow)

            # Генерируем посты на следующий день
            logging.getLogger(__name__).info("Запуск ежедневной генерации рандомных постов...")
            await scheduler.generate_next_day_random_posts()
            logging.getLogger(__name__).info("Рандомные посты на следующий день сгенерированы")

        except Exception:
            logging.getLogger(__name__).exception("Ошибка в generate_daily_random_posts")
            await asyncio.sleep(60 * 60)  # Ждем час при ошибке

async def on_bot_added_to_channel(update: ChatMemberUpdated):
    """Обработчик добавления бота в канал"""
    try:
        chat_id = update.chat.id
        chat_title = update.chat.title or "Неизвестный канал"
        logging.getLogger(__name__).info(f"Бот добавлен в канал: {chat_title} (ID: {chat_id})")
    except Exception:
        logging.getLogger(__name__).exception("Ошибка обработки добавления бота в канал")

# Главная функция
async def main():
    """Запуск бота"""
    # Импортируем handlers здесь, чтобы избежать циклического импорта
    from handlers.core import register_handlers
    from database import migrate_all_databases

    # Выполняем миграцию баз данных (включая исправление/оптимизацию)
    await migrate_all_databases()

    # Регистрируем обработчики
    register_handlers(dp, bot)

    # Регистрируем обработчик добавления бота в канал
    dp.chat_member.register(on_bot_added_to_channel, BotAddedToChannel())

    # Инициализация планировщика из schu.py
    global scheduler
    scheduler = PostScheduler(bot=bot)

    # Запускаем фоновые задачи
    tasks = [
        asyncio.create_task(cleanup_clients()),
        asyncio.create_task(check_license_notifications()),
        # Убираем отдельный полуночный генератор, т.к. сам планировщик
        # делает дозаполнение и генерацию слотов, что предотвращает гонки
        # asyncio.create_task(generate_daily_random_posts()),
        asyncio.create_task(cleanup_past_posts_periodic()),
    ]

    try:
        # Запускаем планировщик (внутри — собственный цикл проверки всех типов постов)
        await scheduler.start()
        # Запускаем бота
        logging.getLogger(__name__).info("Бот запущен!")
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Бот остановлен!")
    finally:
        # Останавливаем все задачи
        for task in tasks:
            task.cancel()

        # Ждем завершения всех задач
        await asyncio.gather(*tasks, return_exceptions=True)

        # Останавливаем планировщик
        await scheduler.stop()

if __name__ == "__main__":
    # Запускаем главную функцию
    asyncio.run(main())