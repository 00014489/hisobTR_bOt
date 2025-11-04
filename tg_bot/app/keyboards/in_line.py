from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import app.cmn.transtalor as translator
from aiogram.utils.keyboard import InlineKeyboardBuilder
import app.data.dbContext as db


async def languages_keyboard(input_lng_code: str) -> InlineKeyboardMarkup:
    """
    Build an inline keyboard for language selection.
    Button text = full_name (e.g. English, Русский, O'zbek)
    Callback data = lang code (e.g. 'en', 'ru', 'uz')
    """
    keyboard = InlineKeyboardBuilder()


    for lang_code, phrases in translator.translations.items():
        full_name = phrases.get("full_name", lang_code)
        keyboard.add(
            InlineKeyboardButton(
                text=full_name,
                callback_data=f"lang_{lang_code}"
            )
        )
    if input_lng_code:
        keyboard.add(
            InlineKeyboardButton(
                text=f"⬅️ {await translator.get_text(input_lng_code, 'back')}",
                callback_data=f"backToSettings"
            )
        )

    return keyboard.adjust(1).as_markup()