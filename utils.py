# utils.py - Утилиты для работы с контентом

import re

def clean_post_content(text, donor_channel=None):
    """
    Очищает текст поста от ссылок на исходный канал, сохраняя структуру
    
    Args:
        text (str): Исходный текст поста
        donor_channel (str): Название канала-донора (например, '@ifinvest')
    
    Returns:
        str: Очищенный текст
    """
    if not text:
        return text
    
    # Убираем упоминания канала-донора (более аккуратно)
    if donor_channel:
        donor_name = donor_channel.replace("@", "")
        
        # Убираем @username в конце строки (только если это отдельная строка)
        text = re.sub(rf'\n\s*@{donor_name}\s*$', '', text, flags=re.IGNORECASE)
        text = re.sub(rf'^\s*@{donor_name}\s*\n', '\n', text, flags=re.IGNORECASE)
        
        # Убираем @username в начале текста (только если это первая строка)
        text = re.sub(rf'^\s*@{donor_name}\s+', '', text, flags=re.IGNORECASE)
        
        # Убираем @username в конце текста (только если это последняя строка)
        text = re.sub(rf'\s+@{donor_name}\s*$', '', text, flags=re.IGNORECASE)
        
        # Убираем отдельные строки с @username
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if line and not re.match(rf'^@{donor_name}$', line, flags=re.IGNORECASE):
                cleaned_lines.append(line)
        text = '\n'.join(cleaned_lines)
    
    # Убираем ссылки на Telegram каналы (более аккуратно)
    # Убираем ссылки вида https://t.me/channel_name (только отдельные строки)
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        if line:
            # Убираем ссылки на t.me только если это отдельная строка
            if not re.match(r'^https://t\.me/[a-zA-Z0-9_]+$', line):
                if not re.match(r'^t\.me/[a-zA-Z0-9_]+$', line):
                    cleaned_lines.append(line)
    text = '\n'.join(cleaned_lines)
    
    # Убираем множественные пустые строки, но сохраняем структуру
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    
    # Убираем пробелы в начале и конце
    text = text.strip()
    
    return text 