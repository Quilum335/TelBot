# setup_session.py - Оптимизированная настройка сессии Pyrogram

import os
import sys
import asyncio
import logging
from typing import Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
from pyrogram import Client
from pyrogram.errors import (
    SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired,
    PhoneNumberInvalid, PhoneNumberBanned, PhoneNumberUnoccupied,
    FloodWait, BadRequest
)

from config import Config

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SessionSetupResult:
    """Результат настройки сессии"""
    success: bool
    session_string: Optional[str] = None
    error_message: Optional[str] = None
    user_info: Optional[dict] = None

class SessionSetupManager:
    """Менеджер для настройки сессий Pyrogram"""
    
    def __init__(self):
        self.session_dir = Path(Config.SESSIONS_DIR)
        self.session_dir.mkdir(exist_ok=True)
        self.client: Optional[Client] = None
    
    def validate_phone_number(self, phone: str) -> Tuple[bool, str]:
        """Валидация номера телефона"""
        if not phone.startswith("+"):
            return False, "Номер должен начинаться с +"
        
        # Убираем все символы кроме цифр и +
        clean_phone = ''.join(c for c in phone if c.isdigit() or c == '+')
        
        if len(clean_phone) < 10:
            return False, "Номер слишком короткий"
        
        if len(clean_phone) > 15:
            return False, "Номер слишком длинный"
        
        return True, clean_phone
    
    def validate_code(self, code: str) -> Tuple[bool, str]:
        """Валидация кода подтверждения"""
        if not code.isdigit():
            return False, "Код должен содержать только цифры"
        
        if len(code) < 4 or len(code) > 6:
            return False, "Код должен содержать от 4 до 6 цифр"
        
        return True, code
    
    async def create_client(self, phone: str) -> Client:
        """Создание клиента Pyrogram"""
        session_name = self.session_dir / "main_session"
        
        # Удаляем старые файлы сессии
        for ext in ['.session', '.session-journal']:
            old_file = session_name.with_suffix(ext)
            if old_file.exists():
                old_file.unlink()
        
        self.client = Client(
            str(session_name),
            api_id=Config.API_ID,
            api_hash=Config.API_HASH
        )
        
        return self.client
    
    async def send_code(self, phone: str) -> Tuple[bool, str, Optional[str]]:
        """Отправка кода подтверждения"""
        try:
            await self.client.connect()
            sent_code = await self.client.send_code(phone)
            return True, "Код отправлен успешно", sent_code.phone_code_hash
            
        except PhoneNumberInvalid:
            return False, "Неверный формат номера телефона", None
        except PhoneNumberBanned:
            return False, "Номер заблокирован в Telegram", None
        except PhoneNumberUnoccupied:
            return False, "Номер не зарегистрирован в Telegram", None
        except FloodWait as e:
            return False, f"Слишком много попыток. Подождите {e.value} секунд", None
        except Exception as e:
            return False, f"Ошибка отправки кода: {str(e)}", None
    
    async def sign_in(self, phone: str, code: str, phone_code_hash: str) -> Tuple[bool, str]:
        """Вход в аккаунт с кодом"""
        try:
            await self.client.sign_in(phone, phone_code_hash, code)
            return True, "Вход выполнен успешно"
            
        except PhoneCodeInvalid:
            return False, "Неверный код подтверждения"
        except PhoneCodeExpired:
            return False, "Код подтверждения истек"
        except SessionPasswordNeeded:
            return False, "NEED_PASSWORD"  # Специальный флаг для 2FA
        except Exception as e:
            return False, f"Ошибка входа: {str(e)}"
    
    async def check_password(self, password: str) -> Tuple[bool, str]:
        """Проверка пароля 2FA"""
        try:
            await self.client.check_password(password)
            return True, "Пароль принят"
        except Exception as e:
            return False, f"Неверный пароль: {str(e)}"
    
    async def get_user_info(self) -> Optional[dict]:
        """Получение информации о пользователе"""
        try:
            me = await self.client.get_me()
            return {
                'id': me.id,
                'first_name': me.first_name,
                'last_name': me.last_name,
                'username': me.username,
                'phone_number': me.phone_number
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о пользователе: {e}")
            return None
    
    async def export_session_string(self) -> Optional[str]:
        """Экспорт session string"""
        try:
            return await self.client.export_session_string()
        except Exception as e:
            logger.error(f"Ошибка экспорта session string: {e}")
            return None
    
    def save_session_string(self, session_string: str) -> bool:
        """Сохранение session string в файл"""
        try:
            session_file = self.session_dir / "session_string.txt"
            session_file.write_text(session_string)
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения session string: {e}")
            return False
    
    async def cleanup(self):
        """Очистка ресурсов"""
        if self.client and self.client.is_connected:
            await self.client.disconnect()

class InteractiveSessionSetup:
    """Интерактивная настройка сессии"""
    
    def __init__(self):
        self.manager = SessionSetupManager()
    
    def print_banner(self):
        """Вывод баннера"""
        print("=" * 60)
        print("🔧 НАСТРОЙКА СЕССИИ PYROGRAM")
        print("=" * 60)
        print("Этот скрипт поможет настроить основной аккаунт для работы бота")
        print("с публичными каналами Telegram.")
        print()
    
    def get_phone_number(self) -> Optional[str]:
        """Получение номера телефона от пользователя"""
        while True:
            phone = input("📱 Введите номер телефона (+7xxxxxxxxx): ").strip()
            
            if phone.lower() == 'exit':
                return None
            
            is_valid, clean_phone = self.manager.validate_phone_number(phone)
            if is_valid:
                return clean_phone
            else:
                print(f"❌ {clean_phone}")
                print("Попробуйте еще раз или введите 'exit' для выхода")
    
    def get_code(self) -> Optional[str]:
        """Получение кода подтверждения от пользователя"""
        while True:
            code = input("🔢 Введите код подтверждения: ").strip()
            
            if code.lower() == 'exit':
                return None
            
            is_valid, clean_code = self.manager.validate_code(code)
            if is_valid:
                return clean_code
            else:
                print(f"❌ {clean_code}")
                print("Попробуйте еще раз или введите 'exit' для выхода")
    
    def get_password(self) -> Optional[str]:
        """Получение пароля 2FA от пользователя"""
        while True:
            password = input("🔐 Введите пароль двухфакторной аутентификации: ").strip()
            
            if password.lower() == 'exit':
                return None
            
            if len(password) < 4:
                print("❌ Пароль слишком короткий")
                continue
            
            return password
    
    async def setup_session(self) -> SessionSetupResult:
        """Основной процесс настройки сессии"""
        try:
            self.print_banner()
            
            # Получение номера телефона
            phone = self.get_phone_number()
            if not phone:
                return SessionSetupResult(False, error_message="Пользователь отменил операцию")
            
            # Создание клиента
            print("🔗 Подключение к Telegram...")
            client = await self.manager.create_client(phone)
            
            # Отправка кода
            print("📨 Отправка кода подтверждения...")
            success, message, phone_code_hash = await self.manager.send_code(phone)
            
            if not success:
                return SessionSetupResult(False, error_message=message)
            
            print("✅ Код подтверждения отправлен на ваш телефон")
            print("⚠️ ВАЖНО: Введите код в этот терминал, НЕ в Telegram!")
            print()
            
            # Получение кода
            code = self.get_code()
            if not code:
                return SessionSetupResult(False, error_message="Пользователь отменил операцию")
            
            # Вход в аккаунт
            print("🔐 Вход в аккаунт...")
            success, message = await self.manager.sign_in(phone, code, phone_code_hash)
            
            if not success:
                if message == "NEED_PASSWORD":
                    # Требуется 2FA
                    password = self.get_password()
                    if not password:
                        return SessionSetupResult(False, error_message="Пользователь отменил операцию")
                    
                    success, message = await self.manager.check_password(password)
                    if not success:
                        return SessionSetupResult(False, error_message=message)
                else:
                    return SessionSetupResult(False, error_message=message)
            
            # Получение информации о пользователе
            user_info = await self.manager.get_user_info()
            if not user_info:
                return SessionSetupResult(False, error_message="Не удалось получить информацию о пользователе")
            
            print(f"✅ Успешный вход! Аккаунт: {user_info['first_name']} (@{user_info['username']})")
            
            # Экспорт session string
            session_string = await self.manager.export_session_string()
            if not session_string:
                return SessionSetupResult(False, error_message="Не удалось экспортировать session string")
            
            # Сохранение в файл
            if self.manager.save_session_string(session_string):
                print(f"💾 Session string сохранен в: {self.manager.session_dir / 'session_string.txt'}")
            else:
                print("⚠️ Не удалось сохранить session string в файл")
            
            print("✅ Настройка завершена!")
            return SessionSetupResult(True, session_string=session_string, user_info=user_info)
            
        except KeyboardInterrupt:
            print("\n❌ Операция отменена пользователем")
            return SessionSetupResult(False, error_message="Операция отменена")
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            return SessionSetupResult(False, error_message=f"Неожиданная ошибка: {str(e)}")
        finally:
            await self.manager.cleanup()

def setup_session():
    """Ручная настройка сессии Pyrogram (синхронная версия)"""
    setup = InteractiveSessionSetup()
    result = asyncio.run(setup.setup_session())
    
    if result.success:
        print("\n🎉 Настройка успешно завершена!")
        print(f"👤 Пользователь: {result.user_info['first_name']} (@{result.user_info['username']})")
        print("🚀 Теперь бот может работать с публичными каналами")
    else:
        print(f"\n❌ Ошибка настройки: {result.error_message}")
        sys.exit(1)

if __name__ == "__main__":
    setup_session() 