# utils.py - Утилиты для работы с контентом

import re

def clean_post_content(text, donor_channel=None):
    """
    Очищает текст поста от ссылок/упоминаний телеграм-каналов, сохраняя структуру.
    - Удаляет t.me/telegram.me ссылки в любом месте текста
    - Удаляет @username токены (отдельные слова)
    - Дополнительно удаляет явные упоминания канала-донора (первую/последнюю строку и отдельные строки)
    """
    if not text:
        return text

    # Глобально убираем t.me/telegram.me ссылки где угодно в тексте
    text = re.sub(r'(https?://)?t(?:elegram)?\.me/[A-Za-z0-9_+/]+', '', text)

    # Удаляем любые отдельные токены @username (не часть email/слова)
    text = re.sub(r'(?<!\S)@[A-Za-z0-9_]{3,}(?!\S)', '', text)

    # Точечная зачистка упоминаний конкретного донора (если задан)
    if donor_channel:
        donor_name = donor_channel.replace("@", "")
        # Варианты расположения имени донора
        text = re.sub(rf'\n\s*@{donor_name}\s*$', '', text, flags=re.IGNORECASE)
        text = re.sub(rf'^\s*@{donor_name}\s*\n', '\n', text, flags=re.IGNORECASE)
        text = re.sub(rf'^\s*@{donor_name}\s+', '', text, flags=re.IGNORECASE)
        text = re.sub(rf'\s+@{donor_name}\s*$', '', text, flags=re.IGNORECASE)

        # Удаляем отдельные строки, состоящие только из @donor_name
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            line_stripped = line.strip()
            if line_stripped and not re.match(rf'^@{donor_name}$', line_stripped, flags=re.IGNORECASE):
                cleaned_lines.append(line_stripped)
        text = '\n'.join(cleaned_lines)

    # Убираем множественные пустые строки, но сохраняем структуру абзацев
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)

    return text.strip()