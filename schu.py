# scheduler.py - Планировщик постов с поддержкой рандомных постов

import asyncio
import aiosqlite
import logging
from datetime import datetime, timedelta
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import BufferedInputFile
from pyrogram import Client
from pyrogram.errors import FloodWait
import json
import random
import os
from glob import glob
import re
import hashlib
from io import BytesIO
from typing import Dict, Optional

from config import Config
from database import get_user_db_path, safe_json_loads
from utils import clean_post_content

logger = logging.getLogger(__name__)

def clean_telegram_links(text):
    """Удаляет ссылки/упоминания телеграм-каналов из текста, сохраняя исходные абзацы и переносы.
    - Удаляет t.me/telegram.me ссылки
    - Удаляет @username как отдельные токены
    - Не схлопывает пробелы и не меняет переводы строк
    """
    if not text:
        return text
    
    # Удаляем ссылки на Telegram каналы/приглашения в любом месте текста
    text = re.sub(r'(https?://)?t(?:elegram)?\.me/[A-Za-z0-9_+/]+', '', text)
    
    # Удаляем @username, но только когда это отдельный токен (не часть email)
    text = re.sub(r'(?<!\S)@[A-Za-z0-9_]{3,}(?!\S)', '', text)
    
    # Возвращаем как есть, чтобы сохранить форматирование/абзацы
    return text

class PostScheduler:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.running = False
        self.task = None
        self.session_string = Config.get_session_string()
        # Кэш клиентов Pyrogram по session_string
        self._client_cache: Dict[str, Client] = {}
        # Ограничение параллелизма публикаций во избежание FloodWait
        self._publish_semaphore = asyncio.Semaphore(5)
        # Настройка диапазона джиттера для разнесения публикаций по целям (в секундах)
        self.random_min_jitter_sec = 60
        self.random_max_jitter_sec = 300
        # Троттлинг для периодической дозаписи расписаний
        self._last_backfill_time: Optional[datetime] = None
        # Локи по каналам, чтобы избежать гонок публикаций в один канал
        self._channel_locks: Dict[int, asyncio.Lock] = {}
        
    async def _reserve_dedup(self, db: aiosqlite.Connection, channel_id: int, fingerprint: str) -> bool:
        """Пытается зарезервировать публикацию (дедуп). Возвращает True, если публикацию можно делать."""
        try:
            await db.execute(
                "INSERT OR IGNORE INTO published_dedup(channel_id, fingerprint, published_at) VALUES (?, ?, ?)",
                (channel_id, fingerprint, datetime.now().isoformat())
            )
            cur = await db.execute("SELECT changes()")
            row = await cur.fetchone()
            await db.commit()
            changes = int(row[0]) if row and row[0] is not None else 0
            return changes > 0
        except Exception:
            # Если таблица не готова — не блокируем публикацию
            return True
        
    async def _get_client(self, session_string: str, name_hint: str) -> Optional[Client]:
        if not session_string:
            return None
        if session_string in self._client_cache:
            return self._client_cache[session_string]
        client = Client(
            name_hint,
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            session_string=session_string,
            in_memory=True
        )
        await client.start()
        self._client_cache[session_string] = client
        return client

    async def _stop_all_clients(self):
        for client in list(self._client_cache.values()):
            try:
                await client.stop()
            except Exception:
                pass
        self._client_cache.clear()
        
    async def start(self):
        """Запуск планировщика"""
        if self.running:
            return
        self.running = True
        self.task = asyncio.create_task(self.scheduler_loop())
        logger.info("Планировщик запущен")
        
    async def stop(self):
        """Остановка планировщика"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        await self._stop_all_clients()
        logger.info("Планировщик остановлен")
        
    async def scheduler_loop(self):
        """Основной цикл планировщика"""
        # Первый раз — сразу пытаемся дозаполнить расписания на сегодня/завтра
        try:
            await self.generate_next_day_random_posts()
            self._last_backfill_time = datetime.now()
        except Exception:
            pass
        while self.running:
            try:
                await self.check_scheduled_posts()
                await self.check_repost_streams()
                await self.check_random_posts()
                await self.check_periodic_posts()  # Добавляем проверку периодических постов
                # Каждые ~15 минут пытаемся дозаполнить слоты (на случай пропуска полуночи или нового потока)
                try:
                    now = datetime.now()
                    if not self._last_backfill_time or (now - self._last_backfill_time).total_seconds() >= 15 * 60:
                        await self.generate_next_day_random_posts()
                        self._last_backfill_time = now
                except Exception:
                    pass
                await asyncio.sleep(30)  # Проверяем каждые 30 секунд
            except Exception as e:
                logger.error(f"Ошибка в цикле планировщика: {e}")
                await asyncio.sleep(60)
                
    async def check_scheduled_posts(self):
        """Проверка и публикация обычных запланированных постов"""
        for db_file in glob(os.path.join(Config.DB_DIR, '*.db')):
            try:
                async with aiosqlite.connect(db_file) as db:
                    now_iso = datetime.now().isoformat()
                    # Получаем посты, время которых наступило
                    cursor = await db.execute('''
                        SELECT id, channel_id, content_type, content, media_id, scheduled_time
                        FROM posts 
                        WHERE is_published = 0 
                        AND content_type != 'random'  -- Исключаем рандомные посты
                        AND scheduled_time <= ?
                        LIMIT 50
                    ''', (now_iso,))
                    posts = await cursor.fetchall()
                    if posts:
                        logger.info(f"Найдено {len(posts)} запланированных постов для публикации в {db_file}")
                    for post in posts:
                        post_id, channel_id, content_type, content, media_id, scheduled_time = post
                        logger.info(f"Публикация поста {post_id} в {channel_id} (тип={content_type}, время={scheduled_time})")
                        try:
                            cleaned_content = clean_telegram_links(content) if content else ""
                            if content_type == "text":
                                await self.bot.send_message(channel_id, cleaned_content)
                            elif content_type == "photo":
                                await self.bot.send_photo(channel_id, media_id, caption=cleaned_content)
                            elif content_type == "video":
                                await self.bot.send_video(channel_id, media_id, caption=cleaned_content)
                            elif content_type == "repost":
                                parts = content.split("_")
                                if len(parts) >= 3:
                                    source_channel_id = int(parts[1])
                                    source_post_id = int(parts[2])
                                    await self.bot.forward_message(channel_id, source_channel_id, source_post_id)
                            # Отмечаем пост как опубликованный
                            await db.execute(
                                "UPDATE posts SET is_published = 1, last_post_time = ? WHERE id = ?",
                                (datetime.now().isoformat(), post_id)
                            )
                            await db.commit()
                            logger.info(f"Опубликован пост {post_id} в канал {channel_id}")
                        except Exception as e:
                            logger.error(f"Ошибка публикации поста {post_id} в {channel_id}: {e}")
            except Exception as e:
                logger.error(f"Ошибка обработки БД {db_file}: {e}")

    async def check_periodic_posts(self):
        """Проверка и публикация периодических постов"""
        logger.info("Проверяем периодические посты...")
        
        for db_file in glob(os.path.join(Config.DB_DIR, '*.db')):
            try:
                async with aiosqlite.connect(db_file) as db:
                    # Получаем активные периодические посты
                    cursor = await db.execute('''
                        SELECT id, donor_channel, target_channels, last_post_time, 
                               phone_number, is_public_channel
                        FROM periodic_posts 
                        WHERE is_active = 1
                    ''')
                    periodic_posts = await cursor.fetchall()
                    
                    if not periodic_posts:
                        continue
                    
                    current_time = datetime.now()
                    
                    for post in periodic_posts:
                        post_id = post[0]
                        donor_channel = post[1]
                        target_channels_json = post[2]
                        last_post_time_str = post[3]
                        phone_number = post[4]
                        is_public_channel = post[5]
                        
                        # Парсим target_channels
                        target_channels = safe_json_loads(target_channels_json, [])
                        if not target_channels:
                            continue
                        
                        # Проверяем, нужно ли публиковать (каждые 6 часов)
                        should_publish = False
                        if last_post_time_str:
                            last_post_time = datetime.fromisoformat(last_post_time_str)
                            time_since_last = current_time - last_post_time
                            if time_since_last.total_seconds() >= 6 * 3600:  # 6 часов
                                should_publish = True
                        else:
                            should_publish = True
                        
                        if should_publish:
                            logger.info(f"Публикуем периодический пост {post_id}")
                            # Получаем пост из донора
                            post_data = await self.get_random_post_from_donor(
                                donor_channel, 7, is_public_channel, phone_number, db_file
                            )
                            if post_data:
                                for target_channel in target_channels:
                                    # Дедуп: резервируем по содержимому
                                    try:
                                        fingerprint = self._make_post_fingerprint(post_data)
                                        ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                                    except Exception:
                                        ok = True
                                    if not ok:
                                        logger.info(f"Пропуск дубликата в периодическом посте для {target_channel}")
                                        continue
                                    try:
                                        await self.publish_post_to_channel(post_data, target_channel)
                                        logger.info(f"Опубликован периодический пост в канал {target_channel}")
                                    except Exception as e:
                                        logger.error(f"Ошибка публикации в канал {target_channel}: {e}")
                                await db.execute(
                                    "UPDATE periodic_posts SET last_post_time = ? WHERE id = ?",
                                    (current_time.isoformat(), post_id)
                                )
                                await db.commit()
                            else:
                                logger.warning(f"Не удалось получить пост из донора {donor_channel}")
                            
            except Exception as e:
                logger.error(f"Ошибка обработки периодических постов в БД {db_file}: {e}")

    async def check_random_posts(self):
        """Проверка и публикация рандомных постов"""
        logger.info("Проверяем рандомные посты...")
        
        for db_file in glob(os.path.join(Config.DB_DIR, '*.db')):
            try:
                async with aiosqlite.connect(db_file) as db:
                    try:
                        await db.execute("PRAGMA busy_timeout = 5000")
                    except Exception:
                        pass
                    now_iso = datetime.now().isoformat()
                    # Берем готовые расписанные строки из posts (per-target)
                    cursor = await db.execute('''
                        SELECT id, channel_id, donor_channels_json, post_freshness, phone_number, is_public_channel, random_post_id, scheduled_time
                        FROM posts
                        WHERE is_published = 0 AND content_type = 'random' AND scheduled_time <= ?
                        ORDER BY scheduled_time ASC
                        LIMIT 100
                    ''', (now_iso,))
                    due_rows = await cursor.fetchall()
                    if not due_rows:
                        continue
                    for row in due_rows:
                        try:
                            post_id, channel_id, donors_json, freshness, phone, is_public, stream_id, sched_time = row
                            # Защитная проверка: некоторые старые/некорректные значения в БД
                            # могут пройти фильтр SQL, поэтому парсим scheduled_time здесь
                            # и пропускаем запись, если время ещё в будущем.
                            try:
                                sched_dt = datetime.fromisoformat(str(sched_time))
                                if sched_dt > datetime.now():
                                    # ещё не время, пропускаем — возможно формат в БД иной
                                    continue
                            except Exception:
                                # Если не удаётся распарсить — пропускаем, чтобы не публиковать случайно
                                logger.warning(f"Не удалось распарсить scheduled_time для поста {post_id}: {sched_time}")
                                continue
                            # Попробуем атомарно зарезервировать слот (защита от гонок и дублей между процессами)
                            try:
                                await db.execute(
                                    "UPDATE posts SET is_published = -1 WHERE id = ? AND is_published = 0",
                                    (post_id,)
                                )
                                cur_res = await db.execute("SELECT changes()")
                                row_res = await cur_res.fetchone()
                                await db.commit()
                                changes = int(row_res[0]) if row_res and row_res[0] is not None else 0
                                if changes == 0:
                                    # Слот уже взят другим воркером
                                    continue
                            except Exception:
                                # Если не удалось зарезервировать — лучше пропустить, чем задублировать
                                continue

                            donors = safe_json_loads(donors_json, [])
                            if not donors:
                                logger.warning(f"Random post {post_id}: нет доноров")
                                # Снимаем резервацию, чтобы слот мог быть обработан в будущем
                                try:
                                    await db.execute(
                                        "UPDATE posts SET is_published = 0 WHERE id = ? AND is_published = -1",
                                        (post_id,)
                                    )
                                    await db.commit()
                                except Exception:
                                    pass
                                continue

                            # Пытаемся несколько раз подобрать не-дублирующийся пост
                            max_retries = 5
                            publish_ready = False
                            fingerprint = None
                            post_data = None
                            for attempt in range(max_retries):
                                selected_donor = random.choice(donors)
                                post_data = await self.get_random_post_from_donor(selected_donor, freshness, is_public, phone, db_file)
                                if not post_data:
                                    logger.warning(f"Не удалось получить пост из донора {selected_donor} для {channel_id} (попытка {attempt+1}/{max_retries})")
                                    continue
                                # Резервируем дедуп до публикации, чтобы исключить гонки
                                fingerprint = self._make_post_fingerprint(post_data)
                                ok = await self._reserve_dedup(db, int(channel_id), fingerprint)
                                if ok:
                                    publish_ready = True
                                    break
                                else:
                                    logger.info(f"Дубликат для канала {channel_id}, пробуем другого донора/пост (попытка {attempt+1}/{max_retries})")

                            if not publish_ready:
                                # Все попытки не удались — фиксируем пропуск слота без сдвига времени,
                                # чтобы сохранить синхронизацию расписания
                                logger.info(f"Слот {post_id} для канала {channel_id} пропущен из-за дубликатов/ошибок получения контента")
                                await db.execute(
                                    "UPDATE posts SET is_published = 1, last_post_time = ? WHERE id = ?",
                                    (datetime.now().isoformat(), post_id)
                                )
                                await db.commit()
                                continue

                            # (removed scheduled_today_count hard-limit check per user request)

                            # Safety: ensure we do not publish too frequently to the same channel
                            try:
                                from config import Config as _C
                                if _C.MAX_POSTS_PER_CHANNEL_PER_DAY > 0:
                                    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                                    day_end = today_start.replace(hour=23, minute=59, second=59, microsecond=999999)
                                    cur_pub_count = await db.execute("SELECT COUNT(*) FROM published_dedup WHERE channel_id = ? AND published_at >= ? AND published_at <= ?", (int(channel_id), today_start.isoformat(), day_end.isoformat()))
                                    row_count = await cur_pub_count.fetchone()
                                    published_today_count = int(row_count[0]) if row_count and row_count[0] is not None else 0
                                    if published_today_count >= _C.MAX_POSTS_PER_CHANNEL_PER_DAY:
                                        logger.info(f"Пропускаем публикацию в {channel_id}: достигнут дневной лимит {_C.MAX_POSTS_PER_CHANNEL_PER_DAY}")
                                        await db.execute("UPDATE posts SET is_published = 1, last_post_time = ? WHERE id = ?", (datetime.now().isoformat(), post_id))
                                        await db.commit()
                                        continue
                                # минимальный интервал между постами в канале
                                min_sec = getattr(_C, 'MIN_SECONDS_BETWEEN_POSTS_PER_CHANNEL', 60)
                                if min_sec > 0:
                                    cur_last = await db.execute("SELECT MAX(published_at) FROM published_dedup WHERE channel_id = ?", (int(channel_id),))
                                    last_row = await cur_last.fetchone()
                                    last_pub = None
                                    try:
                                        if last_row and last_row[0]:
                                            last_pub = datetime.fromisoformat(last_row[0])
                                    except Exception:
                                        last_pub = None
                                    if last_pub and (datetime.now() - last_pub).total_seconds() < min_sec:
                                        logger.info(f"Пропускаем публикацию в {channel_id}: меньше чем {min_sec}s с последней публикации")
                                        # Снимаем резервацию
                                        await db.execute("UPDATE posts SET is_published = 0 WHERE id = ? AND is_published = -1", (post_id,))
                                        await db.commit()
                                        continue
                            except Exception:
                                # при ошибке проверок — продолжим публикацию (лучше публиковать, чем блокировать)
                                pass

                            # Публикуем подобранный контент
                            await self.publish_post_to_channel(post_data, channel_id)
                            await db.execute(
                                "UPDATE posts SET is_published = 1, last_post_time = ? WHERE id = ?",
                                (datetime.now().isoformat(), post_id)
                            )
                            # Обновим last_post_time потока и next_post_times_json как объединение оставшихся
                            await db.execute(
                                "UPDATE random_posts SET last_post_time = ? WHERE id = ?",
                                (datetime.now().isoformat(), stream_id)
                            )
                            cur = await db.execute(
                                """
                                SELECT scheduled_time FROM posts
                                WHERE random_post_id = ? AND is_published = 0 AND scheduled_time > datetime('now','localtime')
                                ORDER BY scheduled_time ASC
                                """,
                                (stream_id,)
                            )
                            union_rows = await cur.fetchall()
                            union_times = []
                            for r in union_rows:
                                try:
                                    union_times.append(datetime.fromisoformat(str(r[0])))
                                except Exception:
                                    continue
                            await db.execute(
                                "UPDATE random_posts SET next_post_times_json = ? WHERE id = ?",
                                (json.dumps([t.isoformat() for t in union_times]), stream_id)
                            )
                            await db.commit()
                            logger.info(f"Опубликован рандомный пост (stream {stream_id}) в {channel_id}")
                        except Exception as e:
                            # Если публикация не удалась — снимаем резервацию слота, чтобы попробовать позже
                            try:
                                await db.execute(
                                    "UPDATE posts SET is_published = 0 WHERE id = ? AND is_published = -1",
                                    (post_id,)
                                )
                                await db.commit()
                            except Exception:
                                pass
                            # Также снимем резерв дедупа, если он был
                            try:
                                if 'fingerprint' in locals():
                                    await db.execute(
                                        "DELETE FROM published_dedup WHERE channel_id = ? AND fingerprint = ?",
                                        (int(channel_id), fingerprint)
                                    )
                                    await db.commit()
                            except Exception:
                                pass
                            logger.error(f"Ошибка публикации random post row: {e}")
            except Exception as e:
                logger.error(f"Ошибка обработки рандомных постов в БД {db_file}: {e}")

    async def get_random_post_from_donor(self, donor, freshness_days, is_public, phone_number, db_path):
        """Получение случайного поста из канала-донора"""
        try:
            # Определяем session_string
            if is_public:
                session_string = self.session_string
            else:
                async with aiosqlite.connect(db_path) as db:
                    cursor = await db.execute(
                        "SELECT session_string FROM linked_accounts WHERE phone_number = ?",
                        (phone_number,)
                    )
                    result = await cursor.fetchone()
                    if not result:
                        logger.error(f"Не найден session_string для аккаунта {phone_number}")
                        return None
                    session_string = result[0]
            if not session_string:
                logger.error("Нет session_string для получения постов")
                return None
            # Клиент из кэша
            client = await self._get_client(session_string, name_hint=f"random_scheduler")
            if not client:
                logger.error("Не удалось создать клиент Pyrogram")
                return None
            # Определяем ID канала
            if isinstance(donor, str):
                if donor.startswith('@'):
                    channel_id = donor
                elif donor.isdigit():
                    channel_id = int(donor)
                else:
                    channel_id = f"@{donor}"
            else:
                channel_id = int(donor)
            # Получаем посты из канала
            posts = []
            grouped: dict[str, dict] = {}
            min_date = datetime.now() - timedelta(days=freshness_days)
            async for message in client.get_chat_history(channel_id, limit=100):
                if message.date < min_date:
                    break
                has_any = bool(message.text or message.photo or message.video)
                if not has_any:
                    continue
                # Обрабатываем альбомы (media groups): берём один элемент на группу, предпочтительно с подписью
                if getattr(message, 'media_group_id', None):
                    group_id = str(message.media_group_id)
                    group = grouped.setdefault(group_id, {"first": None, "with_caption": None})
                    if group["first"] is None:
                        group["first"] = message
                    if (getattr(message, 'caption', None) or None) and group["with_caption"] is None:
                        group["with_caption"] = message
                    continue
                posts.append(message)
            # Добавим по одному сообщению из каждой группы
            for g in grouped.values():
                picked = g["with_caption"] or g["first"]
                if picked:
                    posts.append(picked)
            if not posts:
                logger.warning(f"Нет подходящих постов в канале {channel_id}")
                return None
            selected_post = random.choice(posts)
            post_data = {
                'type': None,
                'text': None,
                'media': None,
                'caption': None
            }
            if selected_post.text:
                post_data['type'] = 'text'
                post_data['text'] = clean_telegram_links(selected_post.text)
            elif selected_post.photo:
                post_data['type'] = 'photo'
                # Скачиваем в память
                post_data['media'] = await client.download_media(selected_post.photo, in_memory=True)
                post_data['caption'] = clean_telegram_links(getattr(selected_post, 'caption', None) or "")
                # Иногда текст в соседнем сообщении альбома — попытаемся взять text как caption-фолбэк
                if not post_data['caption'] and getattr(selected_post, 'media_group_id', None):
                    # Ищем среди собранной группы с тем же media_group_id
                    group_id = str(selected_post.media_group_id)
                    candidate = grouped.get(group_id, {}).get('with_caption')
                    if candidate and getattr(candidate, 'caption', None):
                        post_data['caption'] = clean_telegram_links(candidate.caption or "")
            elif selected_post.video:
                post_data['type'] = 'video'
                post_data['media'] = await client.download_media(selected_post.video, in_memory=True)
                post_data['caption'] = clean_telegram_links(getattr(selected_post, 'caption', None) or "")
                if not post_data['caption'] and getattr(selected_post, 'media_group_id', None):
                    group_id = str(selected_post.media_group_id)
                    candidate = grouped.get(group_id, {}).get('with_caption')
                    if candidate and getattr(candidate, 'caption', None):
                        post_data['caption'] = clean_telegram_links(candidate.caption or "")
            return post_data
        except Exception as e:
            logger.error(f"Ошибка получения поста из донора {donor}: {e}")
            return None

    async def publish_post_to_channel(self, post_data, channel_id):
        """Публикация поста в канал"""
        async with self._publish_semaphore:
            from io import BytesIO
            original_media = post_data.get('media') if isinstance(post_data, dict) else None
            try:
                if post_data['type'] == 'text':
                    await self.bot.send_message(channel_id, post_data['text'])
                elif post_data['type'] == 'photo':
                    media_obj = post_data['media']
                    # Если это in-memory (BytesIO/bytes), обернём в BufferedInputFile
                    try:
                        if isinstance(media_obj, (bytes, bytearray)):
                            media_input = BufferedInputFile(media_obj, filename="photo.jpg")
                        elif isinstance(media_obj, BytesIO):
                            media_input = BufferedInputFile(media_obj.getvalue(), filename="photo.jpg")
                        else:
                            media_input = media_obj  # уже FSInputFile или поддерживаемый тип
                    except Exception:
                        media_input = media_obj
                    # Фолбэк: если caption пустой, попробуем использовать text (truncate)
                    caption = post_data.get('caption') or (post_data.get('text')[:1024] if post_data.get('text') else None)
                    await self.bot.send_photo(channel_id, media_input, caption=caption)
                elif post_data['type'] == 'video':
                    media_obj = post_data['media']
                    try:
                        if isinstance(media_obj, (bytes, bytearray)):
                            media_input = BufferedInputFile(media_obj, filename="video.mp4")
                        elif isinstance(media_obj, BytesIO):
                            media_input = BufferedInputFile(media_obj.getvalue(), filename="video.mp4")
                        else:
                            media_input = media_obj
                    except Exception:
                        media_input = media_obj
                    caption = post_data.get('caption') or (post_data.get('text')[:1024] if post_data.get('text') else None)
                    await self.bot.send_video(channel_id, media_input, caption=caption)
            except Exception as e:
                logger.error(f"Ошибка публикации в канал {channel_id}: {e}")
                raise
            finally:
                # Очистка буферов/файлов после отправки для экономии памяти/диска
                try:
                    if isinstance(original_media, BytesIO):
                        original_media.close()
                    elif isinstance(original_media, str) and os.path.exists(original_media):
                        os.remove(original_media)
                except Exception:
                    pass

    async def check_repost_streams(self):
        """Проверка потоков репостов"""
        if not self.session_string:
            logger.warning("Нет основной сессии для проверки репостов")
            return
            
        logger.info("Проверяем потоки репостов...")
        streams_count = 0
        
        for db_file in glob(os.path.join(Config.DB_DIR, '*.db')):
            try:
                async with aiosqlite.connect(db_file) as db:
                    cursor = await db.execute('''
                        SELECT id, donor_channel, target_channels, last_message_id, 
                               is_public_channel, phone_number, post_freshness
                        FROM repost_streams
                        WHERE is_active = 1
                    ''')
                    streams = await cursor.fetchall()
                    streams_count += len(streams)
                    
                    for stream in streams:
                        stream_id, donor, targets_str, last_id, is_public, phone, _freshness_unused = stream
                        logger.info(f"Обрабатываем поток репостов {stream_id}: донор={donor}, публичный={is_public}")
                        # Определяем session для использования
                        if is_public or not phone:
                            session_string = self.session_string
                        else:
                            cursor = await db.execute(
                                "SELECT session_string FROM linked_accounts WHERE phone_number = ?",
                                (phone,)
                            )
                            result = await cursor.fetchone()
                            if not result:
                                logger.warning(f"Не найден session_string для телефона {phone}")
                                continue
                            session_string = result[0]
                        if not session_string:
                            logger.warning(f"Нет session_string для потока {stream_id}")
                            continue
                        # Парсим целевые каналы
                        if isinstance(targets_str, str) and targets_str.startswith('['):
                            target_channels = safe_json_loads(targets_str, [])
                        else:
                            target_channels = [int(cid.strip()) for cid in str(targets_str).split(',') if str(cid).strip()]
                        logger.info(f"Целевые каналы для потока {stream_id}: {target_channels}")
                        await self.check_donor_channel(
                            donor, target_channels, last_id or 0, stream_id, 
                            session_string, db_file
                        )
                        
            except Exception as e:
                logger.error(f"Ошибка обработки потоков в БД {db_file}: {e}")
        
        if streams_count == 0:
            logger.info("Активных потоков репостов не найдено")
        else:
            logger.info(f"Обработано {streams_count} потоков репостов")

    async def check_donor_channel(self, donor_channel, target_channels, last_message_id, 
                                 stream_id, session_string, db_path):
        """Проверка новых постов в канале-доноре"""
        try:
            logger.info(f"Проверяем донор {donor_channel} для потока {stream_id}")
            client = await self._get_client(session_string, name_hint=f"stream_{stream_id}")
            if not client:
                logger.warning("Не удалось получить клиент для проверки донора")
                return
            # Определяем ID канала
            if isinstance(donor_channel, str):
                if donor_channel.startswith('@'):
                    channel_id = donor_channel
                elif donor_channel.isdigit():
                    channel_id = int(donor_channel)
                else:
                    channel_id = f"@{donor_channel}"
            else:
                channel_id = int(donor_channel)

            # Если last_message_id ещё не установлен, инициализируем базовую точку: текущий последний пост
            if not last_message_id:
                latest_id = None
                try:
                    async for msg in client.get_chat_history(channel_id, limit=1):
                        latest_id = msg.id
                        break
                except Exception:
                    latest_id = None
                if latest_id:
                    async with aiosqlite.connect(db_path) as db:
                        await db.execute(
                            "UPDATE repost_streams SET last_message_id = ? WHERE id = ?",
                            (latest_id, stream_id)
                        )
                        await db.commit()
                        logger.info(f"Инициализирован baseline для потока {stream_id}: last_message_id={latest_id}")
                # После инициализации выходим: начнём постить только новые сообщения при следующем проходе
                return
            # Получаем новые сообщения
            new_messages = []
            async for message in client.get_chat_history(channel_id, limit=50):
                if message.id <= (last_message_id or 0):
                    break
                # Учитываем любые пользовательские сообщения с содержимым
                if message.photo or message.video or message.document or message.audio or message.voice or message.sticker or message.text:
                    new_messages.append(message)
            logger.info(f"Найдено {len(new_messages)} новых сообщений в {donor_channel}")
            # Публикуем новые сообщения
            for message in reversed(new_messages):
                for target_channel in target_channels:
                    try:
                        # Сначала обрабатываем медиа с подписью, затем чистый текст
                        if message.photo:
                            caption = clean_telegram_links(message.caption or "")
                            # Дедуп резервация
                            try:
                                fingerprint = self._make_post_fingerprint({"type": "photo", "caption": caption, "text": None})
                                ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                            except Exception:
                                ok = True
                            if not ok:
                                logger.info(f"Пропущен дубликат photo для {target_channel}")
                                continue
                            data_obj = None
                            try:
                                data_obj = await client.download_media(message, in_memory=True)
                                from io import BytesIO
                                if isinstance(data_obj, BytesIO):
                                    data = data_obj.getvalue()
                                    await self.bot.send_photo(target_channel, BufferedInputFile(data, filename="photo.jpg"), caption=caption)
                                elif isinstance(data_obj, (bytes, bytearray)):
                                    await self.bot.send_photo(target_channel, BufferedInputFile(data_obj, filename="photo.jpg"), caption=caption)
                                else:
                                    with open(str(data_obj), 'rb') as f:
                                        data = f.read()
                                    await self.bot.send_photo(target_channel, BufferedInputFile(data, filename=os.path.basename(str(data_obj)) or "photo.jpg"), caption=caption)
                            except Exception as e:
                                logger.error(f"Ошибка отправки фото в {target_channel}: {e}")
                            finally:
                                try:
                                    if hasattr(data_obj, 'close'):
                                        data_obj.close()
                                    elif isinstance(data_obj, str) and os.path.exists(data_obj):
                                        os.remove(data_obj)
                                except Exception:
                                    pass
                        elif message.video:
                            caption = clean_telegram_links(message.caption or "")
                            try:
                                fingerprint = self._make_post_fingerprint({"type": "video", "caption": caption, "text": None})
                                ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                            except Exception:
                                ok = True
                            if not ok:
                                logger.info(f"Пропущен дубликат video для {target_channel}")
                                continue
                            data_obj = None
                            try:
                                data_obj = await client.download_media(message, in_memory=True)
                                from io import BytesIO
                                if isinstance(data_obj, BytesIO):
                                    data = data_obj.getvalue()
                                    await self.bot.send_video(target_channel, BufferedInputFile(data, filename="video.mp4"), caption=caption)
                                elif isinstance(data_obj, (bytes, bytearray)):
                                    await self.bot.send_video(target_channel, BufferedInputFile(data_obj, filename="video.mp4"), caption=caption)
                                else:
                                    with open(str(data_obj), 'rb') as f:
                                        data = f.read()
                                    await self.bot.send_video(target_channel, BufferedInputFile(data, filename=os.path.basename(str(data_obj)) or "video.mp4"), caption=caption)
                            except Exception as e:
                                logger.error(f"Ошибка отправки видео в {target_channel}: {e}")
                            finally:
                                try:
                                    if hasattr(data_obj, 'close'):
                                        data_obj.close()
                                    elif isinstance(data_obj, str) and os.path.exists(data_obj):
                                        os.remove(data_obj)
                                except Exception:
                                    pass
                        elif message.document:
                            caption = clean_telegram_links(message.caption or "")
                            try:
                                fingerprint = self._make_post_fingerprint({"type": "document", "caption": caption, "text": None})
                                ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                            except Exception:
                                ok = True
                            if not ok:
                                logger.info(f"Пропущен дубликат document для {target_channel}")
                                continue
                            data_obj = None
                            try:
                                data_obj = await client.download_media(message, in_memory=True)
                                from io import BytesIO
                                if isinstance(data_obj, BytesIO):
                                    data = data_obj.getvalue()
                                    await self.bot.send_document(target_channel, BufferedInputFile(data, filename="document.bin"), caption=caption)
                                elif isinstance(data_obj, (bytes, bytearray)):
                                    await self.bot.send_document(target_channel, BufferedInputFile(data_obj, filename="document.bin"), caption=caption)
                                else:
                                    with open(str(data_obj), 'rb') as f:
                                        data = f.read()
                                    await self.bot.send_document(target_channel, BufferedInputFile(data, filename=os.path.basename(str(data_obj)) or "document.bin"), caption=caption)
                            except Exception as e:
                                logger.error(f"Ошибка отправки документа в {target_channel}: {e}")
                            finally:
                                try:
                                    if hasattr(data_obj, 'close'):
                                        data_obj.close()
                                    elif isinstance(data_obj, str) and os.path.exists(data_obj):
                                        os.remove(data_obj)
                                except Exception:
                                    pass
                        elif message.audio:
                            try:
                                fingerprint = self._make_post_fingerprint({"type": "audio", "caption": clean_telegram_links(message.caption or ""), "text": None})
                                ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                            except Exception:
                                ok = True
                            if not ok:
                                logger.info(f"Пропущен дубликат audio для {target_channel}")
                                continue
                            data_obj = None
                            try:
                                data_obj = await client.download_media(message, in_memory=True)
                                from io import BytesIO
                                if isinstance(data_obj, BytesIO):
                                    data = data_obj.getvalue()
                                    await self.bot.send_audio(target_channel, BufferedInputFile(data, filename="audio.mp3"), caption=clean_telegram_links(message.caption or ""))
                                elif isinstance(data_obj, (bytes, bytearray)):
                                    await self.bot.send_audio(target_channel, BufferedInputFile(data_obj, filename="audio.mp3"), caption=clean_telegram_links(message.caption or ""))
                                else:
                                    with open(str(data_obj), 'rb') as f:
                                        data = f.read()
                                    await self.bot.send_audio(target_channel, BufferedInputFile(data, filename=os.path.basename(str(data_obj)) or "audio.mp3"), caption=clean_telegram_links(message.caption or ""))
                            except Exception as e:
                                logger.error(f"Ошибка отправки аудио в {target_channel}: {e}")
                            finally:
                                try:
                                    if hasattr(data_obj, 'close'):
                                        data_obj.close()
                                    elif isinstance(data_obj, str) and os.path.exists(data_obj):
                                        os.remove(data_obj)
                                except Exception:
                                    pass
                        elif message.voice:
                            try:
                                fingerprint = self._make_post_fingerprint({"type": "voice", "caption": None, "text": None})
                                ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                            except Exception:
                                ok = True
                            if not ok:
                                logger.info(f"Пропущен дубликат voice для {target_channel}")
                                continue
                            data_obj = None
                            try:
                                data_obj = await client.download_media(message, in_memory=True)
                                from io import BytesIO
                                if isinstance(data_obj, BytesIO):
                                    data = data_obj.getvalue()
                                    await self.bot.send_voice(target_channel, BufferedInputFile(data, filename="voice.ogg"))
                                elif isinstance(data_obj, (bytes, bytearray)):
                                    await self.bot.send_voice(target_channel, BufferedInputFile(data_obj, filename="voice.ogg"))
                                else:
                                    with open(str(data_obj), 'rb') as f:
                                        data = f.read()
                                    await self.bot.send_voice(target_channel, BufferedInputFile(data, filename=os.path.basename(str(data_obj)) or "voice.ogg"))
                            except Exception as e:
                                logger.error(f"Ошибка отправки голосового в {target_channel}: {e}")
                            finally:
                                try:
                                    if hasattr(data_obj, 'close'):
                                        data_obj.close()
                                    elif isinstance(data_obj, str) and os.path.exists(data_obj):
                                        os.remove(data_obj)
                                except Exception:
                                    pass
                        elif message.sticker:
                            try:
                                fingerprint = self._make_post_fingerprint({"type": "sticker", "caption": None, "text": None})
                                ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                            except Exception:
                                ok = True
                            if not ok:
                                logger.info(f"Пропущен дубликат sticker для {target_channel}")
                                continue
                            data_obj = None
                            try:
                                data_obj = await client.download_media(message, in_memory=True)
                                from io import BytesIO
                                if isinstance(data_obj, BytesIO):
                                    data = data_obj.getvalue()
                                    await self.bot.send_sticker(target_channel, BufferedInputFile(data, filename="sticker.webp"))
                                elif isinstance(data_obj, (bytes, bytearray)):
                                    await self.bot.send_sticker(target_channel, BufferedInputFile(data_obj, filename="sticker.webp"))
                                else:
                                    with open(str(data_obj), 'rb') as f:
                                        data = f.read()
                                    await self.bot.send_sticker(target_channel, BufferedInputFile(data, filename=os.path.basename(str(data_obj)) or "sticker.webp"))
                            except Exception as e:
                                logger.error(f"Ошибка отправки стикера в {target_channel}: {e}")
                            finally:
                                try:
                                    if hasattr(data_obj, 'close'):
                                        data_obj.close()
                                    elif isinstance(data_obj, str) and os.path.exists(data_obj):
                                        os.remove(data_obj)
                                except Exception:
                                    pass
                        elif message.text:
                            cleaned_text = clean_telegram_links(message.text)
                            try:
                                fingerprint = self._make_post_fingerprint({"type": "text", "caption": None, "text": cleaned_text})
                                ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                            except Exception:
                                ok = True
                            if not ok:
                                logger.info(f"Пропущен дубликат text для {target_channel}")
                                continue
                            await self.bot.send_message(target_channel, cleaned_text)
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Ошибка репоста в {target_channel}: {e}")
            # Обновляем last_message_id
            if new_messages:
                max_id = max(msg.id for msg in new_messages)
                async with aiosqlite.connect(db_path) as db:
                    await db.execute(
                        "UPDATE repost_streams SET last_message_id = ? WHERE id = ?",
                        (max_id, stream_id)
                    )
                    await db.commit()
                    logger.info(f"Обновлен last_message_id для потока {stream_id}: {max_id}")
            else:
                # Доп. логирование для диагностики отсутствия новых сообщений
                try:
                    latest_id = None
                    async for msg in client.get_chat_history(channel_id, limit=1):
                        latest_id = msg.id
                        break
                    logger.info(f"Проверка: last_id={last_message_id}, latest_id={latest_id}")
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Ошибка проверки донора {donor_channel}: {e}")

    async def generate_next_day_random_posts(self):
        """Генерация рандомных постов на следующий день"""
        try:
            if os.path.exists(Config.DB_DIR):
                for filename in os.listdir(Config.DB_DIR):
                    if filename.endswith(".db"):
                        db_path = os.path.join(Config.DB_DIR, filename)
                        await self._generate_next_day_random_posts_for_db(db_path)
        except Exception as e:
            logger.error(f"Ошибка в generate_next_day_random_posts: {e}")

    async def _generate_next_day_random_posts_for_db(self, db_path):
        """Поддержка и пополнение рандомных постов для конкретной БД"""
        try:
            from database import migrate_posts_table_for_random_posts
            await migrate_posts_table_for_random_posts(db_path)
            
            async with aiosqlite.connect(db_path) as db:
                # Смягчаем блокировки
                try:
                    await db.execute("PRAGMA busy_timeout = 5000")
                except Exception:
                    pass
                cursor = await db.execute('''
                    SELECT id, donor_channels, target_channels, posts_per_day, 
                           post_freshness, phone_number, is_public_channel, next_post_times_json
                    FROM random_posts
                    WHERE is_active = 1
                ''')
                random_streams = await cursor.fetchall()
                if not random_streams:
                    return
                for stream in random_streams:
                    stream_id, donor_channels_json, target_channels_json, posts_per_day, post_freshness, phone_number, is_public_channel, next_post_times_json = stream
                    donor_channels = safe_json_loads(donor_channels_json, [])
                    target_channels = safe_json_loads(target_channels_json, [])
                    if not donor_channels or not target_channels:
                        continue
                    now = datetime.now()
                    min_future = now + timedelta(minutes=2)
                    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    day_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
                    tomorrow_start = today_start + timedelta(days=1)
                    tomorrow_end = day_end + timedelta(days=1)

                    # Для каждого целевого канала наполняем недостающее количество слотов на сегодня и (минимум) на завтра
                    for idx_target, target_channel in enumerate((target_channels if isinstance(target_channels, list) else []), start=1):
                        # Перенос по минутам для отличия расписаний между целями: 0, +1, +2, ... минуты
                        per_target_offset = timedelta(minutes=max(0, idx_target - 1))
                        # Сбор существующих времён на сегодня
                        cur = await db.execute(
                            """
                            SELECT scheduled_time FROM posts
                            WHERE random_post_id = ? AND channel_id = ? AND is_published = 0
                              AND scheduled_time >= ? AND scheduled_time <= ?
                            """,
                            (stream_id, target_channel, today_start.isoformat(), day_end.isoformat())
                        )
                        existing_today_rows = await cur.fetchall()
                        existing_today = set()
                        for r in existing_today_rows:
                            try:
                                existing_today.add(datetime.fromisoformat(str(r[0])))
                            except Exception:
                                continue
                        # `posts_per_day` в random_posts хранится как количество
                        # постов В ДЕНЬ НА КАЖДУЮ ЦЕЛЬ (per-target)
                        try:
                            per_target_posts = int(posts_per_day)
                        except Exception:
                            per_target_posts = 0
                        need_today = max(0, int(per_target_posts) - len(existing_today))

                        # Генерация недостающих слотов на сегодня
                        if need_today > 0:
                            remaining_seconds_total = max(0, (day_end - now).total_seconds())
                            remaining_minutes = max(1, int(remaining_seconds_total // 60))
                            generated: list[datetime] = []
                            if need_today <= remaining_minutes:
                                picked = sorted(random.sample(range(remaining_minutes), need_today))
                                for m in picked:
                                    dt = now + timedelta(minutes=m, seconds=random.randint(0, 59)) + per_target_offset
                                    # если попали в последнюю минуту суток, сдвигаем на завтра + небольшой джиттер
                                    if dt.hour == 23 and dt.minute == 59:
                                        dt = tomorrow_start + timedelta(minutes=random.randint(0, 10)) + per_target_offset
                                    if dt < min_future:
                                        dt = min_future
                                    if dt > day_end:
                                        dt = day_end
                                    if dt not in existing_today:
                                        generated.append(dt)
                            else:
                                step = remaining_minutes / need_today
                                for i in range(need_today):
                                    dt = now + timedelta(minutes=int(i * step), seconds=random.randint(0, 59)) + per_target_offset
                                    if dt.hour == 23 and dt.minute == 59:
                                        dt = tomorrow_start + timedelta(minutes=random.randint(0, 10)) + per_target_offset
                                    if dt < min_future:
                                        dt = min_future
                                    if dt > day_end:
                                        dt = day_end
                                    if dt not in existing_today:
                                        generated.append(dt)
                            # Вставка недостающих
                            for dt in generated:
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
                                        json.dumps(donor_channels),
                                        json.dumps(target_channels),
                                        post_freshness,
                                        phone_number,
                                        is_public_channel,
                                    )
                                )
                        # Аналогично — буфер на завтра
                        cur = await db.execute(
                            """
                            SELECT scheduled_time FROM posts
                            WHERE random_post_id = ? AND channel_id = ? AND is_published = 0
                              AND scheduled_time >= ? AND scheduled_time <= ?
                            """,
                            (stream_id, target_channel, tomorrow_start.isoformat(), tomorrow_end.isoformat())
                        )
                        existing_tomorrow_rows = await cur.fetchall()
                        existing_tomorrow = set()
                        for r in existing_tomorrow_rows:
                            try:
                                existing_tomorrow.add(datetime.fromisoformat(str(r[0])))
                            except Exception:
                                continue
                        need_tomorrow = max(0, int(posts_per_day) - len(existing_tomorrow))
                        if need_tomorrow > 0:
                            generated: list[datetime] = []
                            if need_tomorrow <= 1440:
                                picked = sorted(random.sample(range(1440), need_tomorrow))
                                for m in picked:
                                    dt = tomorrow_start + timedelta(minutes=m, seconds=random.randint(0, 59)) + per_target_offset
                                    if dt not in existing_tomorrow:
                                        generated.append(dt)
                            else:
                                step = 1440 / need_tomorrow
                                for i in range(need_tomorrow):
                                    dt = tomorrow_start + timedelta(minutes=int(i * step), seconds=random.randint(0, 59)) + per_target_offset
                                    if dt not in existing_tomorrow:
                                        generated.append(dt)
                            for dt in generated:
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
                                        json.dumps(donor_channels),
                                        json.dumps(target_channels),
                                        post_freshness,
                                        phone_number,
                                        is_public_channel,
                                    )
                                )

                    # Обновляем сводный список времён потока как объединение будущих времён из posts
                    cur = await db.execute(
                        """
                        SELECT scheduled_time FROM posts
                        WHERE random_post_id = ? AND is_published = 0 AND scheduled_time > datetime('now','localtime')
                        ORDER BY scheduled_time ASC
                        """,
                        (stream_id,)
                    )
                    union_rows = await cur.fetchall()
                    union_times = []
                    for r in union_rows:
                        try:
                            union_times.append(datetime.fromisoformat(str(r[0])))
                        except Exception:
                            continue
                    await db.execute(
                        "UPDATE random_posts SET next_post_times_json = ? WHERE id = ?",
                        (json.dumps([t.isoformat() for t in union_times]), stream_id)
                    )

                    await db.commit()
                    logger.info(f"Дополнены рандомные посты для потока {stream_id}: цели={len(target_channels)} по {posts_per_day}/день")
        
        except Exception as e:
            logger.error(f"Ошибка в _generate_next_day_random_posts_for_db для {db_path}: {e}")

    async def _publish_random_to_target_with_jitter(self, donor_channels, freshness, is_public, phone, db_file, target_channel, jitter_sec: int):
        try:
            await asyncio.sleep(max(0, jitter_sec))
            # Safety guard: ensure there's a scheduled 'random' post for this target in DB
            try:
                async with aiosqlite.connect(db_file) as db:
                    cur = await db.execute(
                        """
                        SELECT id, scheduled_time FROM posts
                        WHERE channel_id = ? AND content_type = 'random' AND is_published = 0
                        ORDER BY scheduled_time ASC LIMIT 1
                        """,
                        (target_channel,)
                    )
                    row = await cur.fetchone()
                    if not row:
                        logger.info(f"Skipping ad-hoc random publish for {target_channel}: no scheduled post found")
                        return
                    try:
                        sched_dt = datetime.fromisoformat(str(row[1]))
                    except Exception:
                        sched_dt = None
                    # If the next scheduled post is far in the future, skip ad-hoc publish
                    if sched_dt and sched_dt > datetime.now() + timedelta(seconds=120):
                        logger.info(f"Skipping ad-hoc random publish for {target_channel}: next scheduled at {sched_dt}")
                        return
            except Exception as e:
                # DB check failed — log and proceed (better to publish than to silently drop)
                logger.exception(f"Error checking scheduled post for {target_channel}: {e}")
            selected_donor = random.choice(donor_channels)
            post_data = await self.get_random_post_from_donor(selected_donor, freshness, is_public, phone, db_file)
            if not post_data:
                logger.warning(f"Не удалось получить пост из донора {selected_donor} для {target_channel}")
                return

            # Dedup & channel locking to avoid duplicates from concurrent tasks
            fingerprint = self._make_post_fingerprint(post_data)
            # Acquire per-channel lock
            lock = self._channel_locks.get(int(target_channel))
            if lock is None:
                lock = asyncio.Lock()
                self._channel_locks[int(target_channel)] = lock

            async with lock:
                try:
                    async with aiosqlite.connect(db_file) as db:
                        ok = await self._reserve_dedup(db, int(target_channel), fingerprint)
                        if not ok:
                            logger.info(f"Пропущен дубликат для {target_channel} (донор={selected_donor})")
                            return
                except Exception as e:
                    logger.exception(f"Ошибка дедупа перед публикацией для {target_channel}: {e}")
                    # proceed to publish to avoid silent suppression

                await self.publish_post_to_channel(post_data, target_channel)
                logger.info(f"Опубликован рандомный пост в канал {target_channel} (донор={selected_donor})")
        except Exception as e:
            logger.error(f"Ошибка публикации в канал {target_channel} с джиттером: {e}")

    def _make_post_fingerprint(self, post_data: dict) -> str:
        """Строит короткий отпечаток содержимого для дедупликации."""
        try:
            t = post_data.get('type')
            caption = (post_data.get('caption') or '').strip()
            text = (post_data.get('text') or '').strip()
            # Попробуем включить хеш медиаконтента, если он есть (bytes / BytesIO / path)
            media_hash = ''
            try:
                media_obj = post_data.get('media')
                if media_obj is not None:
                    if isinstance(media_obj, (bytes, bytearray)):
                        media_hash = hashlib.sha256(media_obj).hexdigest()[:32]
                    elif isinstance(media_obj, BytesIO):
                        media_hash = hashlib.sha256(media_obj.getvalue()).hexdigest()[:32]
                    elif isinstance(media_obj, str) and os.path.exists(media_obj):
                        try:
                            with open(media_obj, 'rb') as f:
                                media_hash = hashlib.sha256(f.read()).hexdigest()[:32]
                        except Exception:
                            media_hash = ''
            except Exception:
                media_hash = ''
            # Сократим до 300 символов для стабильности
            base = f"{t}|{caption[:300]}|{text[:300]}|{media_hash}"
            return hashlib.sha256(base.encode('utf-8', errors='ignore')).hexdigest()[:32]
        except Exception:
            # Фолбэк по типу
            return f"{post_data.get('type')}_unknown"