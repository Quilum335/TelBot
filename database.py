# database.py - Функции для работы с базой данных

import os
import aiosqlite
from datetime import datetime, timedelta
from config import Config
import json
from typing import Optional
import logging

logger = logging.getLogger(__name__)

async def apply_performance_pragmas(db: aiosqlite.Connection) -> None:
    """Применяет PRAGMA-настройки для повышения производительности."""
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute("PRAGMA temp_store=MEMORY")
    # Отрицательное значение означает размер в КБ. -20000 ≈ ~20MB кеша страниц
    await db.execute("PRAGMA cache_size=-20000")

async def ensure_db_indexes(db: aiosqlite.Connection) -> None:
    """Создает недостающие индексы для часто используемых запросов."""
    # Индексы для таблицы posts
    await db.execute("CREATE INDEX IF NOT EXISTS idx_posts_sched_pub ON posts(is_published, scheduled_time)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_posts_random_post_id ON posts(random_post_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_posts_content_type ON posts(content_type)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_posts_channel_id ON posts(channel_id)")

    # Индексы для связанных таблиц
    await db.execute("CREATE INDEX IF NOT EXISTS idx_linked_accounts_phone ON linked_accounts(phone_number)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_repost_streams_is_active ON repost_streams(is_active)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_random_posts_is_active ON random_posts(is_active)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_info_telegram_user_id ON info(telegram_user_id)")

    await db.commit()

async def optimize_database(db_path: str) -> None:
    """Применяет PRAGMA и индексы для указанной БД."""
    try:
        async with aiosqlite.connect(db_path) as db:
            await apply_performance_pragmas(db)
            await ensure_db_indexes(db)
    except Exception:
        logger.exception("Ошибка оптимизации базы данных", extra={"db_path": db_path})

async def ensure_user_database(user_id: int, username: str) -> str:
    """Гарантирует существование БД пользователя, возвращает путь к БД."""
    db_path = await get_user_db_path(user_id, username)
    needs_init = False
    if not os.path.exists(db_path):
        needs_init = True
    else:
        # Если файл существует, убеждаемся, что БД инициализирована (есть таблица info)
        try:
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='info'")
                table_exists = await cursor.fetchone()
                if not table_exists:
                    needs_init = True
        except Exception:
            # При ошибке открытия или проверки — переинициализируем
            needs_init = True

    if needs_init:
        await create_user_database(user_id, username)
    return db_path

def safe_json_loads(json_str, default=None):
    """Безопасная загрузка JSON из строки, возвращает default при ошибке."""
    if isinstance(default, list) and isinstance(json_str, (int, float)):
        # Если ожидаем список и получили число, возвращаем пустой список
        return []
    if isinstance(json_str, (int, float)):
        # Если не ожидаем список и получили число, возвращаем число
        return json_str
    if not isinstance(json_str, str):
        # Если вход не строка, и мы ожидаем список, возвращаем пустой список
        if isinstance(default, list):
            return []
        return default
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return default

async def fix_corrupted_json_data(db_path: str):
    """Исправление некорректных JSON данных в базе"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Исправляем repost_streams
            cursor = await db.execute("SELECT id, target_channels FROM repost_streams")
            streams = await cursor.fetchall()
            
            for stream_id, target_channels in streams:
                if target_channels:
                    try:
                        # Пытаемся распарсить JSON
                        json.loads(target_channels)
                    except json.JSONDecodeError:
                        # Если не удается, исправляем
                        fixed_data = safe_json_loads(target_channels, [])
                        await db.execute(
                            "UPDATE repost_streams SET target_channels = ? WHERE id = ?",
                            (json.dumps(fixed_data), stream_id)
                        )
                        logger.info("Исправлен target_channels для потока", extra={"stream_id": stream_id})
            
            # Исправляем random_posts
            cursor = await db.execute("SELECT id, donor_channels, target_channels FROM random_posts")
            posts = await cursor.fetchall()
            
            for post_id, donor_channels, target_channels in posts:
                # Исправляем donor_channels
                if donor_channels:
                    try:
                        json.loads(donor_channels)
                    except json.JSONDecodeError:
                        fixed_data = safe_json_loads(donor_channels, [])
                        await db.execute(
                            "UPDATE random_posts SET donor_channels = ? WHERE id = ?",
                            (json.dumps(fixed_data), post_id)
                        )
                        logger.info("Исправлен donor_channels для поста", extra={"post_id": post_id})
                # Исправляем target_channels
                if target_channels:
                    try:
                        json.loads(target_channels)
                    except json.JSONDecodeError:
                        fixed_data = safe_json_loads(target_channels, [])
                        await db.execute(
                            "UPDATE random_posts SET target_channels = ? WHERE id = ?",
                            (json.dumps(fixed_data), post_id)
                        )
                        logger.info("Исправлен target_channels для поста", extra={"post_id": post_id})
            await db.commit()
    except Exception:
        logger.exception("Ошибка исправления JSON данных", extra={"db_path": db_path})

async def fix_outdated_random_post_times(db_path: str):
    """Исправление устаревших времен в рандомных постах и очистка старых записей"""
    try:
        from datetime import datetime, timedelta
        import random
        
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("SELECT id, posts_per_day, next_post_times_json FROM random_posts WHERE is_active = 1")
            posts = await cursor.fetchall()
            
            now = datetime.now()
            
            for post_id, posts_per_day, next_post_times_json in posts:
                if not next_post_times_json:
                    continue
                    
                try:
                    scheduled_times = json.loads(next_post_times_json)
                    updated_times = []
                    
                    for time_str in scheduled_times:
                        try:
                            scheduled_dt = datetime.fromisoformat(time_str)
                            # Если время в прошлом, заменяем на будущее
                            if scheduled_dt <= now:
                                # Генерируем новое время в ближайшие 2 часа
                                random_minutes = random.randint(5, 120)
                                new_dt = now + timedelta(minutes=random_minutes)
                                updated_times.append(new_dt.isoformat())
                            else:
                                updated_times.append(time_str)
                        except Exception:
                            # Если не удается распарсить время, пропускаем
                            continue
                    
                    # Если все времена были в прошлом, генерируем новые на 7 дней вперед
                    if not updated_times:
                        updated_times = []
                        NUM_DAYS_AHEAD = 7
                        
                        for day_offset in range(NUM_DAYS_AHEAD):
                            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=day_offset)
                            
                            # Для первого дня начинаем с текущего времени
                            if day_offset == 0:
                                start_time = now
                            else:
                                start_time = day_start
                            
                            # Генерируем времена для этого дня
                            day_times = []
                            if posts_per_day <= 1440:  # 24 часа * 60 минут
                                # Выбираем уникальные минуты
                                available_minutes = int((day_start.replace(hour=23, minute=59, second=59) - start_time).total_seconds() / 60)
                                if available_minutes > 0:
                                    picked_minutes = sorted(random.sample(range(available_minutes), min(posts_per_day, available_minutes)))
                                    for m in picked_minutes:
                                        post_time = start_time + timedelta(minutes=m, seconds=random.randint(0, 59))
                                        day_times.append(post_time)
                            else:
                                # Равномерно распределяем
                                step = 1440 / posts_per_day
                                for i in range(posts_per_day):
                                    offset_minutes = int(i * step)
                                    seconds = random.randint(0, 59)
                                    post_time = start_time + timedelta(minutes=offset_minutes, seconds=seconds)
                                    if post_time <= day_start.replace(hour=23, minute=59, second=59):
                                        day_times.append(post_time)
                            
                            updated_times.extend([t.isoformat() for t in day_times])
                        
                        # Ограничиваем общее количество времен
                        updated_times = updated_times[:posts_per_day * NUM_DAYS_AHEAD]
                        updated_times.sort()
                    
                    # Обновляем времена в базе
                    await db.execute(
                        "UPDATE random_posts SET next_post_times_json = ? WHERE id = ?",
                        (json.dumps(updated_times), post_id)
                    )
                    
                    # Очищаем старые записи из таблицы posts для этого random_post_id
                    # Удаляем записи старше 7 дней или уже опубликованные
                    week_ago = now - timedelta(days=7)
                    await db.execute('''
                        DELETE FROM posts 
                        WHERE random_post_id = ? AND (scheduled_time < ? OR is_published = 1)
                    ''', (post_id, week_ago))
                    
                    logger.info("Исправлены времена для рандомного поста", extra={"post_id": post_id})
                    
                except Exception as e:
                    logger.exception("Ошибка исправления времен для поста", extra={"post_id": post_id})
            
            await db.commit()
            logger.info("Исправление устаревших времен завершено для", extra={"db_path": db_path})
            
    except Exception:
        logger.exception("Ошибка исправления устаревших времен для", extra={"db_path": db_path})

async def create_user_database(user_id: int, username: str):
    """Создание базы данных для пользователя"""
    db_name = os.path.join(Config.DB_DIR, f"telegram_{username}_{user_id}.db")
    os.makedirs(Config.DB_DIR, exist_ok=True)
    
    # Проверяем, существует ли уже БД
    db_exists = os.path.exists(db_name)
    logger.debug("Database exists", extra={"db_name": db_name, "db_exists": db_exists})
    
    async with aiosqlite.connect(db_name) as db:
        # Применяем PRAGMA для повышения производительности
        await apply_performance_pragmas(db)
        # Проверяем, есть ли таблица info (индикатор того, что БД инициализирована)
        cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='info'")
        table_exists = await cursor.fetchone()
        
        if table_exists:
            logger.debug("Database already initialized for user", extra={"username": username, "user_id": user_id})
            # Все равно убеждаемся, что индексы на месте
            await ensure_db_indexes(db)
            return db_name
        
        logger.debug("Creating/initializing database for user", extra={"username": username, "user_id": user_id})
        
        # Таблица INFO
        await db.execute('''
            CREATE TABLE IF NOT EXISTS info (
                id INTEGER PRIMARY KEY,
                telegram_username TEXT,
                telegram_user_id INTEGER,
                last_purchase_time TIMESTAMP,
                subscription_duration INTEGER,
                subscription_end TIMESTAMP,
                rights TEXT DEFAULT 'client',
                is_banned BOOLEAN DEFAULT 0
            )
        ''')
        
        # Таблица постов
        await db.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER,
                channel_username TEXT,
                content_type TEXT,
                content TEXT,
                media_id TEXT,
                scheduled_time TIMESTAMP,
                is_periodic BOOLEAN DEFAULT 0,
                period_hours INTEGER,
                is_published BOOLEAN DEFAULT 0,
                last_post_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                random_post_id INTEGER,
                donor_channels_json TEXT,
                target_channels_json TEXT,
                post_freshness INTEGER DEFAULT 1,
                phone_number TEXT,
                is_public_channel BOOLEAN DEFAULT 0
            )
        ''')
        
        # Таблица каналов
        await db.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER UNIQUE,
                channel_username TEXT,
                channel_title TEXT,
                is_donor BOOLEAN DEFAULT 0,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица привязанных аккаунтов (с флагом основного аккаунта is_main)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS linked_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone_number TEXT,
                session_string TEXT,
                is_main BOOLEAN DEFAULT 0,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица потоков авто-репоста
        await db.execute('''
            CREATE TABLE IF NOT EXISTS repost_streams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                donor_channel TEXT,
                target_channels TEXT,
                last_message_id INTEGER DEFAULT 0,
                phone_number TEXT, -- аккаунт, через который читается донор
                is_public_channel BOOLEAN DEFAULT 0, -- флаг для публичных каналов
                post_freshness INTEGER DEFAULT 1, -- свежесть постов в днях (1=день, 7=неделя, 14=2 недели, 30=месяц, 90=3 месяца, 365=год)
                is_active BOOLEAN DEFAULT 1 -- флаг активности потока
            )
        ''')
        
        # Таблица периодических постов
        await db.execute('''
            CREATE TABLE IF NOT EXISTS periodic_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                donor_channel TEXT,
                target_channels TEXT,
                last_post_time TIMESTAMP,
                phone_number TEXT, -- аккаунт для чтения доноров
                is_public_channel BOOLEAN DEFAULT 0, -- флаг для публичных каналов
                is_active BOOLEAN DEFAULT 1, -- флаг активности поста
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица рандомных постов
        await db.execute('''
            CREATE TABLE IF NOT EXISTS random_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                donor_channels TEXT, -- JSON массив каналов-доноров
                target_channels TEXT, -- JSON массив целевых каналов
                min_interval_hours INTEGER DEFAULT 1, -- минимальный интервал между постами
                max_interval_hours INTEGER DEFAULT 24, -- максимальный интервал между постами
                posts_per_day INTEGER DEFAULT 1, -- количество постов в день
                post_freshness INTEGER DEFAULT 1, -- свежесть постов в днях
                is_active BOOLEAN DEFAULT 1,
                last_post_time TIMESTAMP,
                phone_number TEXT, -- аккаунт для чтения доноров
                is_public_channel BOOLEAN DEFAULT 0, -- флаг для публичных каналов
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                next_post_times_json TEXT DEFAULT '[]' -- Новое поле для хранения запланированных постов
            )
        ''')
        
        await db.commit()
        # Убеждаемся, что индексы созданы
        await ensure_db_indexes(db)
        
        # Проверяем, есть ли запись пользователя
        cursor = await db.execute("SELECT * FROM info WHERE telegram_user_id = ?", (user_id,))
        user_info = await cursor.fetchone()
        
        if not user_info:
            # Создаем запись с пробным периодом
            trial_end = datetime.now() + timedelta(days=Config.TRIAL_DAYS)
            await db.execute('''
                INSERT INTO info (telegram_username, telegram_user_id, subscription_end, rights)
                VALUES (?, ?, ?, ?)
            ''', (username, user_id, trial_end, 'client'))
            await db.commit()
            logger.debug("Created trial period for new user", extra={"username": username, "user_id": user_id})
    
    return db_name

async def user_database_exists(user_id: int, username: str) -> bool:
    """Проверка существования БД пользователя"""
    db_path = await get_user_db_path(user_id, username)
    return os.path.exists(db_path)

async def get_user_db_path(user_id: int, username: str) -> str:
    """Получение пути к БД пользователя"""
    return os.path.join(Config.DB_DIR, f"telegram_{username}_{user_id}.db")

async def check_subscription(user_id: int, username: str) -> bool:
    """Проверка активной подписки"""
    # Гарантируем существование БД (без лишних миграций каждый вызов)
    db_path = await ensure_user_database(user_id, username)
    
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            "SELECT subscription_end, is_banned FROM info WHERE telegram_user_id = ?", 
            (user_id,)
        )
        result = await cursor.fetchone()
        
        if result:
            subscription_end = datetime.fromisoformat(result[0])
            is_banned = result[1]
            
            if is_banned:
                return False
                
            return datetime.now() < subscription_end
    
    return False

async def migrate_random_posts_table(db_path: str):
    """Миграция таблицы random_posts для добавления поля posts_per_day"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Проверяем, есть ли поле posts_per_day
            cursor = await db.execute("PRAGMA table_info(random_posts)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if 'posts_per_day' not in column_names:
                # Добавляем поле posts_per_day
                await db.execute('ALTER TABLE random_posts ADD COLUMN posts_per_day INTEGER DEFAULT 1')
                await db.commit()
                logger.info("Миграция выполнена для", extra={"db_path": db_path, "field": "posts_per_day"})
            else:
                logger.debug("Миграция не требуется для", extra={"db_path": db_path, "field": "posts_per_day"})
                
    except Exception:
        logger.exception("Ошибка миграции для", extra={"db_path": db_path})

async def migrate_repost_streams_and_random_posts_tables(db_path: str):
    """Миграция таблиц repost_streams и random_posts для корректного хранения JSON"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Миграция repost_streams: target_channels
            cursor = await db.execute("PRAGMA table_info(repost_streams)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'target_channels' in column_names:
                # Выбираем все записи, где target_channels еще не JSON (простая проверка)
                cursor = await db.execute("SELECT id, target_channels FROM repost_streams WHERE target_channels NOT LIKE '[%]'")
                rows_to_migrate = await cursor.fetchall()

                for row_id, old_target_channels in rows_to_migrate:
                    if old_target_channels:
                        try:
                            # Преобразуем из строки в список и затем в JSON
                            channel_list = [int(cid.strip()) for cid in old_target_channels.split(',') if cid.strip()]
                            new_target_channels = json.dumps(channel_list)
                            await db.execute("UPDATE repost_streams SET target_channels = ? WHERE id = ?", (new_target_channels, row_id))
                            logger.info("Миграция repost_streams для", extra={"db_path": db_path, "row_id": row_id})
                        except Exception as e:
                            logger.exception("Ошибка миграции repost_streams для", extra={"db_path": db_path, "row_id": row_id})
                    else:
                        # Если пусто, сохраняем пустой список JSON
                        await db.execute("UPDATE repost_streams SET target_channels = ? WHERE id = ?", ("[]", row_id))
                        logger.info("Миграция repost_streams для", extra={"db_path": db_path, "row_id": row_id})
                await db.commit()
                
            # Миграция random_posts: donor_channels и target_channels
            cursor = await db.execute("PRAGMA table_info(random_posts)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'donor_channels' in column_names and 'target_channels' in column_names:
                cursor = await db.execute("SELECT id, donor_channels, target_channels FROM random_posts WHERE donor_channels NOT LIKE '[%]' OR target_channels NOT LIKE '[%]'")
                rows_to_migrate = await cursor.fetchall()

                for row_id, old_donor_channels, old_target_channels in rows_to_migrate:
                    # Миграция donor_channels
                    if old_donor_channels and not old_donor_channels.startswith('['): # Проверяем, не JSON ли уже
                        try:
                            donor_list = [int(cid.strip()) if cid.strip().isdigit() else cid.strip() for cid in old_donor_channels.split(',') if cid.strip()]
                            new_donor_channels = json.dumps(donor_list)
                            await db.execute("UPDATE random_posts SET donor_channels = ? WHERE id = ?", (new_donor_channels, row_id))
                            logger.info("Миграция random_posts для", extra={"db_path": db_path, "row_id": row_id})
                        except Exception as e:
                            logger.exception("Ошибка миграции random_posts (donor) для", extra={"db_path": db_path, "row_id": row_id})
                    elif not old_donor_channels:
                        await db.execute("UPDATE random_posts SET donor_channels = ? WHERE id = ?", ("[]", row_id))
                        logger.info("Миграция random_posts для", extra={"db_path": db_path, "row_id": row_id})

                    # Миграция target_channels
                    if old_target_channels and not old_target_channels.startswith('['): # Проверяем, не JSON ли уже
                        try:
                            target_list = [int(cid.strip()) for cid in old_target_channels.split(',') if cid.strip()]
                            new_target_channels = json.dumps(target_list)
                            await db.execute("UPDATE random_posts SET target_channels = ? WHERE id = ?", (new_target_channels, row_id))
                            logger.info("Миграция random_posts для", extra={"db_path": db_path, "row_id": row_id})
                        except Exception as e:
                            logger.exception("Ошибка миграции random_posts (target) для", extra={"db_path": db_path, "row_id": row_id})
                    elif not old_target_channels:
                        await db.execute("UPDATE random_posts SET target_channels = ? WHERE id = ?", ("[]", row_id))
                        logger.info("Миграция random_posts для", extra={"db_path": db_path, "row_id": row_id})

                await db.commit()

            logger.info("Миграция таблиц repost_streams и random_posts для", extra={"db_path": db_path})

    except Exception:
        logger.exception("Общая ошибка миграции для", extra={"db_path": db_path})

async def migrate_random_posts_next_times_table(db_path: str):
    """Миграция таблицы random_posts для добавления поля next_post_times_json"""
    try:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("PRAGMA table_info(random_posts)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'next_post_times_json' not in column_names:
                await db.execute("ALTER TABLE random_posts ADD COLUMN next_post_times_json TEXT DEFAULT '[]'")
                await db.commit()
                logger.info("Миграция выполнена для", extra={"db_path": db_path, "field": "next_post_times_json"})
            else:
                logger.debug("Миграция не требуется для", extra={"db_path": db_path, "field": "next_post_times_json"})

    except Exception:
        logger.exception("Ошибка миграции next_post_times_json для", extra={"db_path": db_path})

async def migrate_posts_last_post_time_table(db_path: str):
    """Миграция таблицы posts для добавления поля last_post_time"""
    try:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute("PRAGMA table_info(posts)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'last_post_time' not in column_names:
                await db.execute("ALTER TABLE posts ADD COLUMN last_post_time TIMESTAMP")
                await db.commit()
                logger.info("Миграция выполнена для", extra={"db_path": db_path, "field": "last_post_time"})
            else:
                logger.debug("Миграция не требуется для", extra={"db_path": db_path, "field": "last_post_time"})

    except Exception:
        logger.exception("Ошибка миграции last_post_time для", extra={"db_path": db_path})

async def migrate_periodic_posts_table(db_path: str):
    """Миграция таблицы periodic_posts для создания таблицы если она не существует"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Проверяем, существует ли таблица periodic_posts
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='periodic_posts'")
            table_exists = await cursor.fetchone()
            
            if not table_exists:
                # Создаем таблицу periodic_posts
                await db.execute('''
                    CREATE TABLE periodic_posts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        donor_channel TEXT,
                        target_channels TEXT,
                        last_post_time TIMESTAMP,
                        phone_number TEXT,
                        is_public_channel BOOLEAN DEFAULT 0,
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                await db.commit()
                logger.info("Миграция выполнена для", extra={"db_path": db_path, "table": "periodic_posts"})
            else:
                logger.debug("Миграция не требуется для", extra={"db_path": db_path, "table": "periodic_posts"})
                
    except Exception:
        logger.exception("Ошибка миграции periodic_posts для", extra={"db_path": db_path})

async def migrate_repost_streams_is_active_table(db_path: str):
    """Миграция таблицы repost_streams для добавления поля is_active"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Проверяем, есть ли поле is_active
            cursor = await db.execute("PRAGMA table_info(repost_streams)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if 'is_active' not in column_names:
                # Добавляем поле is_active
                await db.execute('ALTER TABLE repost_streams ADD COLUMN is_active BOOLEAN DEFAULT 1')
                await db.commit()
                logger.info("Миграция выполнена для", extra={"db_path": db_path, "field": "is_active"})
            else:
                logger.debug("Миграция не требуется для", extra={"db_path": db_path, "field": "is_active"})
                
    except Exception:
        logger.exception("Ошибка миграции repost_streams для", extra={"db_path": db_path})

async def cleanup_bad_random_posts(db_path: str):
    """Очистка проблемных записей рандомных постов"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Удаляем записи с некорректными данными
            cursor = await db.execute('''
                DELETE FROM posts 
                WHERE content_type = 'random' 
                AND (typeof(donor_channels_json) != 'text' OR typeof(target_channels_json) != 'text')
            ''')
            deleted_json = cursor.rowcount
            
            cursor = await db.execute('''
                DELETE FROM posts 
                WHERE content_type = 'random' 
                AND (phone_number IS NULL OR phone_number = '' OR typeof(phone_number) != 'text' OR phone_number NOT LIKE '+%')
            ''')
            deleted_phone = cursor.rowcount
            
            cursor = await db.execute('''
                DELETE FROM posts 
                WHERE content_type = 'random' 
                AND (is_public_channel IS NULL OR typeof(is_public_channel) != 'integer' OR is_public_channel NOT IN (0, 1))
            ''')
            deleted_public = cursor.rowcount
            
            await db.commit()
            
            logger.info("Очищено проблемных записей рандомных постов", extra={"deleted_json": deleted_json, "deleted_phone": deleted_phone, "deleted_public": deleted_public})
            
    except Exception:
        logger.exception("Ошибка очистки проблемных записей")

async def cleanup_past_posts(db_path: str):
    """Очистка постов с прошедшим временем"""
    try:
        async with aiosqlite.connect(db_path) as db:
            now = datetime.now()
            
            # Удаляем посты с прошедшим временем
            cursor = await db.execute('''
                DELETE FROM posts 
                WHERE scheduled_time < ? AND is_published = 0
            ''', (now.isoformat(),))
            deleted_posts = cursor.rowcount
            
            # Обновляем времена в random_posts, убирая прошедшие
            cursor = await db.execute('''
                SELECT id, next_post_times_json FROM random_posts WHERE is_active = 1
            ''')
            random_streams = await cursor.fetchall()
            
            updated_streams = 0
            for stream_id, times_json in random_streams:
                try:
                    scheduled_times = safe_json_loads(times_json, [])
                    future_times = []
                    
                    for time_str in scheduled_times:
                        try:
                            time_dt = datetime.fromisoformat(time_str)
                            if time_dt > now:
                                future_times.append(time_str)
                        except Exception:
                            continue
                    
                    if len(future_times) != len(scheduled_times):
                        await db.execute('''
                            UPDATE random_posts 
                            SET next_post_times_json = ? 
                            WHERE id = ?
                        ''', (json.dumps(future_times), stream_id))
                        updated_streams += 1
                        
                except Exception as e:
                    logger.exception("Ошибка обновления времен для потока", extra={"stream_id": stream_id})
                    continue
            
            await db.commit()
            
            logger.info("Очищено прошедших постов", extra={"deleted_posts": deleted_posts, "updated_streams": updated_streams})
            
    except Exception:
        logger.exception("Ошибка очистки прошедших постов")

async def migrate_all_databases():
    """Миграция всех баз данных"""
    try:
        # Получаем все файлы БД
        if os.path.exists(Config.DB_DIR):
            for filename in os.listdir(Config.DB_DIR):
                if filename.endswith(".db"):
                    db_file = os.path.join(Config.DB_DIR, filename)
                    logger.info("Начинается миграция базы данных", extra={"db_file": db_file})
                    
                    # Выполняем все миграции
                    await migrate_random_posts_table(db_file)
                    await migrate_repost_streams_and_random_posts_tables(db_file)
                    await migrate_random_posts_next_times_table(db_file) # Call new migration function
                    await migrate_posts_last_post_time_table(db_file)
                    await migrate_repost_streams_is_active_table(db_file)  # Добавляем новую миграцию
                    await migrate_periodic_posts_table(db_file) # Добавляем миграцию для periodic_posts
                    await migrate_posts_table_for_random_posts(db_file) # Добавляем миграцию для posts
                    await migrate_published_dedup_table(db_file) # Таблица для дедупликации опубликованных постов
                    
                    # Исправляем некорректные JSON данные
                    await fix_corrupted_json_data(db_file)
                    await fix_outdated_random_post_times(db_file)
                    await cleanup_bad_random_posts(db_file) # Добавляем очистку проблемных записей
                    await cleanup_past_posts(db_file) # Добавляем очистку прошедших постов

                    # Применяем оптимизации (PRAGMA + индексы)
                    await optimize_database(db_file)
                    
        logger.info("Миграция всех баз данных завершена")
        
    except Exception:
        logger.exception("Ошибка миграции баз данных")

async def get_scheduled_posts(user_id: int, username: str):
    """Получение всех запланированных постов пользователя"""
    # Гарантируем существование БД пользователя
    db_path = await ensure_user_database(user_id, username)
    
    async with aiosqlite.connect(db_path) as db:
        now = datetime.now()
        
        # Получаем обычные посты из таблицы posts (только будущие)
        cursor = await db.execute('''
            SELECT p.id, p.channel_id, p.channel_username, p.content_type, p.content, 
                   p.scheduled_time, p.is_periodic, p.period_hours, p.is_published,
                   c.channel_title
            FROM posts p
            LEFT JOIN channels c ON p.channel_id = c.channel_id
            WHERE p.is_published = 0 AND p.content_type != 'random' AND p.scheduled_time > datetime('now','localtime')
            ORDER BY p.scheduled_time ASC
        ''')
        posts = await cursor.fetchall()
        
        # Получаем рандомные посты из таблицы posts (только будущие)
        cursor = await db.execute('''
            SELECT p.id, p.channel_id, p.channel_username, p.content_type, p.content, 
                   p.scheduled_time, p.is_periodic, p.period_hours, p.is_published,
                   c.channel_title, p.donor_channels_json, p.target_channels_json, 
                   p.post_freshness, p.phone_number, p.is_public_channel, p.random_post_id
            FROM posts p
            LEFT JOIN channels c ON p.channel_id = c.channel_id
            WHERE p.is_published = 0 AND p.content_type = 'random' AND p.scheduled_time > datetime('now','localtime')
            ORDER BY p.scheduled_time ASC
        ''')
        random_posts = await cursor.fetchall()
        
        # Получаем потоки репостов
        cursor = await db.execute('''
            SELECT id, donor_channel, target_channels, phone_number, is_public_channel, post_freshness
            FROM repost_streams
        ''')
        repost_streams = await cursor.fetchall()
        
        # Получаем активные рандомные потоки из таблицы random_posts (только с будущими временами)
        cursor = await db.execute('''
            SELECT id, donor_channels, target_channels, min_interval_hours, max_interval_hours,
                   posts_per_day, post_freshness, is_active, last_post_time, phone_number, is_public_channel,
                   next_post_times_json
            FROM random_posts
            WHERE is_active = 1
        ''')
        all_random_posts = await cursor.fetchall()
        
        # Фильтруем только потоки с будущими временами
        old_random_posts = []
        for post in all_random_posts:
            next_times_json = post[11]  # next_post_times_json
            if next_times_json:
                try:
                    next_times = safe_json_loads(next_times_json, [])
                    has_future_times = False
                    
                    for time_str in next_times:
                        try:
                            post_time = datetime.fromisoformat(time_str)
                            if post_time > now:
                                has_future_times = True
                                break
                        except Exception:
                            continue
                    
                    if has_future_times:
                        old_random_posts.append(post)
                except Exception:
                    continue
        
        return {
            'posts': posts,
            'repost_streams': repost_streams,
            'random_posts': random_posts,
            'old_random_posts': old_random_posts
        }

async def delete_scheduled_post(user_id: int, username: str, post_id: int, post_type: str):
    """Удаление запланированного поста"""
    db_path = await get_user_db_path(user_id, username)
    
    async with aiosqlite.connect(db_path) as db:
        if post_type == 'post':
            # Проверяем, является ли это рандомным постом
            cursor = await db.execute('''
                SELECT content_type, random_post_id FROM posts WHERE id = ?
            ''', (post_id,))
            post_info = await cursor.fetchone()
            
            if post_info and post_info[0] == 'random':
                # Это рандомный пост, удаляем все связанные посты
                random_post_id = post_info[1]
                if random_post_id:
                    # Удаляем все посты с этим random_post_id
                    await db.execute('DELETE FROM posts WHERE random_post_id = ?', (random_post_id,))
                    # Удаляем запись из random_posts
                    await db.execute('DELETE FROM random_posts WHERE id = ?', (random_post_id,))
                else:
                    # Удаляем только этот пост
                    await db.execute('DELETE FROM posts WHERE id = ?', (post_id,))
            else:
                # Обычный пост
                await db.execute('DELETE FROM posts WHERE id = ?', (post_id,))
        elif post_type == 'repost_stream':
            await db.execute('DELETE FROM repost_streams WHERE id = ?', (post_id,))
        elif post_type == 'random_post':
            # Удаляем запись потока и все связанные запланированные посты
            try:
                # Удаляем все посты, ссылающиеся на этот поток
                await db.execute('DELETE FROM posts WHERE random_post_id = ?', (post_id,))
            except Exception:
                # Фоллбек: удаляем только запись потока
                pass
            await db.execute('DELETE FROM random_posts WHERE id = ?', (post_id,))
        
        await db.commit()

async def update_post_donor(user_id: int, username: str, post_id: int, post_type: str, new_donor: str):
    """Обновление канала-донора для поста"""
    db_path = await get_user_db_path(user_id, username)
    
    async with aiosqlite.connect(db_path) as db:
        if post_type == 'repost_stream':
            await db.execute('UPDATE repost_streams SET donor_channel = ? WHERE id = ?', (new_donor, post_id))
        elif post_type == 'random_post':
            # Для рандомных постов обновляем donor_channels (JSON массив)
            await db.execute('UPDATE random_posts SET donor_channels = ? WHERE id = ?', (json.dumps([new_donor]), post_id))
        
        await db.commit()

async def get_user_license_info(user_id: int, username: str) -> dict:
    """Получение информации о лицензии пользователя"""
    # Гарантируем существование БД пользователя
    db_path = await ensure_user_database(user_id, username)
    
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
            
            return {
                'has_subscription': days_left > 0 and not is_banned,
                'subscription_end': subscription_end,
                'days_left': days_left,
                'is_banned': is_banned,
                'is_trial': subscription_end == datetime.fromisoformat(
                    (datetime.now() + timedelta(days=Config.TRIAL_DAYS)).isoformat()
                )
            }
    
    return {
        'has_subscription': False,
        'subscription_end': None,
        'days_left': 0,
        'is_banned': False,
        'is_trial': False
    }

async def notify_expired_licenses(bot):
    """Уведомление пользователей об истечении лицензии"""
    import glob
    import os
    
    expired_users = []
    
    # Проходим по всем базам данных пользователей
    for db_file in glob.glob(os.path.join(Config.DB_DIR, "telegram_*.db")):
        try:
            async with aiosqlite.connect(db_file) as db:
                cursor = await db.execute(
                    "SELECT telegram_user_id, telegram_username, subscription_end FROM info LIMIT 1"
                )
                result = await cursor.fetchone()
                
                if result:
                    user_id, username, subscription_end = result
                    
                    if subscription_end:
                        subscription_end_date = datetime.fromisoformat(subscription_end)
                        days_left = (subscription_end_date - datetime.now()).days
                        
                        # Уведомляем за 3 дня до истечения
                        if days_left <= 3 and days_left > 0:
                            expired_users.append({
                                'user_id': user_id,
                                'username': username,
                                'days_left': days_left
                            })
                        # Уведомляем об истечении
                        elif days_left <= 0:
                            expired_users.append({
                                'user_id': user_id,
                                'username': username,
                                'days_left': 0
                            })
                            
        except Exception as e:
            logger.exception("Ошибка при проверке лицензии в", extra={"db_file": db_file})
    
    # Отправляем уведомления
    for user in expired_users:
        try:
            if user['days_left'] > 0:
                text = f"⚠️ Внимание! Ваша лицензия истекает через {user['days_left']} дней.\n\n"
                text += f"💳 Для продления обратитесь к @CEKYHDA"
            else:
                text = f"❌ Ваша лицензия истекла!\n\n"
                text += f"💳 Для продолжения работы купите лицензию у @SEKUNDA"
            
            await bot.send_message(user['user_id'], text)
            
        except Exception as e:
            logger.exception("Ошибка отправки уведомления пользователю", extra={"user_id": user['user_id']})

async def migrate_posts_table_for_random_posts(db_path: str):
    """Миграция таблицы posts для добавления полей для рандомных постов"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Проверяем, есть ли новые поля
            cursor = await db.execute("PRAGMA table_info(posts)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            # Добавляем поля для рандомных постов
            new_fields = [
                ('random_post_id', 'INTEGER'),
                ('donor_channels_json', 'TEXT'),
                ('target_channels_json', 'TEXT'),
                ('post_freshness', 'INTEGER DEFAULT 1'),
                ('phone_number', 'TEXT'),
                ('is_public_channel', 'BOOLEAN DEFAULT 0')
            ]
            
            for field_name, field_type in new_fields:
                if field_name not in column_names:
                    await db.execute(f'ALTER TABLE posts ADD COLUMN {field_name} {field_type}')
                    logger.info("Добавлено поле", extra={"db_path": db_path, "field": field_name})
            
            await db.commit()
            logger.info("Миграция posts таблицы выполнена для", extra={"db_path": db_path})
                
    except Exception:
        logger.exception("Ошибка миграции posts таблицы для", extra={"db_path": db_path})

async def migrate_published_dedup_table(db_path: str):
    """Создает таблицу published_dedup для дедупликации опубликованных постов."""
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS published_dedup (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER NOT NULL,
                    fingerprint TEXT NOT NULL,
                    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_published_dedup_channel_fpr ON published_dedup(channel_id, fingerprint)"
            )
            await db.commit()
            logger.debug("Таблица published_dedup проверена/создана", extra={"db_path": db_path})
    except Exception:
        logger.exception("Ошибка миграции published_dedup для", extra={"db_path": db_path})