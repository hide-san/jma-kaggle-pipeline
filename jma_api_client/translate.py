"""Japanese-to-English translation utilities using deep_translator."""

from functools import lru_cache
from deep_translator import GoogleTranslator

_translator = GoogleTranslator(source='ja', target='en')
_translation_cache = {}


@lru_cache(maxsize=2048)
def translate_ja_to_en(text: str) -> str:
    """Translate Japanese text to English.

    Uses deep_translator with Google Translate backend (free).
    Results are cached to avoid repeated translations.

    Args:
        text: Japanese text to translate

    Returns:
        Translated English text, or original text if translation fails
    """
    if not text:
        return text

    # Check cache first
    if text in _translation_cache:
        return _translation_cache[text]

    try:
        translated = _translator.translate(text)
        _translation_cache[text] = translated
        return translated
    except Exception:
        # On error, return original text
        return text


def translate_fields(data: dict, fields: list) -> dict:
    """Translate specified fields in a dictionary from Japanese to English.

    Args:
        data: Dictionary with potentially Japanese text values
        fields: List of field names to translate

    Returns:
        Dictionary with translated fields
    """
    result = data.copy()
    for field in fields:
        if field in result and result[field]:
            result[field] = translate_ja_to_en(str(result[field]))
    return result
