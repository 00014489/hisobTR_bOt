from aiogram import Router, F, Bot
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, ContentType
from aiogram.exceptions import TelegramRetryAfter, TelegramAPIError, TelegramBadRequest
import asyncio
import re
import unicodedata
from aiogram.fsm.context import FSMContext
import logging
from typing import List
import json


with open("languages.json", "r", encoding="utf-8") as f:
    translations = json.load(f)


def get_all_values_by_key(translations: dict, key: str) -> list:
    
    return [
        lang_dict.get(key, f"[missing: {key}]")
        for lang_dict in translations.values()
    ]

async def get_lang_code_by_text_async(translations: dict, key: str, text: str) -> str | None:
    
    for lang_code, lang_dict in translations.items():
        if lang_dict.get(key) == text:
            return lang_code
    return None

def get_all_language_codes(translations: dict) -> list[str]:
    """
    Returns a list of all language codes from the translations dictionary.

    :param translations: Dictionary of translations with language codes as top-level keys
    :return: List of language code strings (e.g., ['uz', 'en', 'ru'])
    """
    return list(translations.keys())

async def get_text(lang: str, key: str) -> str:
    """
    Asynchronously return a translated phrase based on language and key.

    Args:
        lang (str): Language code ('en', 'ru', 'uz')
        key (str): Phrase key ('greeting', 'farewell', etc.)

    Returns:
        str: Translated phrase or fallback message
    """
    try:
        return translations[lang][key]
    except KeyError:
        return f"[{key} not found in {lang}]"
    

async def smart_sleep(bot_method, *args, **kwargs):
    """
    A wrapper function to handle Telegram's FloodWait exceptions gracefully.
    
    Args:
        bot_method: The bot's method to execute (e.g., bot.send_message).
        *args: Positional arguments for the method.
        **kwargs: Keyword arguments for the method.
    
    Returns:
        The result of the bot method if successful.
    """
    while True:
        try:
            # Attempt the bot method
            return await bot_method(*args, **kwargs)
        except TelegramRetryAfter as e:
            # Handle FloodWait by sleeping for the recommended timeout
            logging.warning(f"FloodWait detected. Retrying after {e.retry_after} seconds...")
            await asyncio.sleep(e.retry_after)
        except TelegramAPIError as ex:
            # Log other Telegram API-related exceptions
            logging.error(f"from smart_sleep Telegram API Error: {ex}")
            return
            # raise
        except Exception as ex:
            # Log other unexpected exceptions
            logging.error(f"from smart_sleep Unexpected error: {ex}")
            # raise
            return