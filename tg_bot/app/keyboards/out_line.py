from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
import app.cmn.transtalor as translator
import app.data.dbContext as db




async def main_menu(lang_code: str) -> ReplyKeyboardMarkup:
    """
    Build a main menu keyboard with localized button labels.

    Args:
        lang_code (str): The language code (e.g., 'en', 'ru', 'uz')

    Returns:
        ReplyKeyboardMarkup: The localized reply keyboard
    """
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text=f"{await translator.get_text(lang_code, 'rashod')}"),
                KeyboardButton(text=f"{await translator.get_text(lang_code, 'income')}")
            ],
            [
                KeyboardButton(text=f"{await translator.get_text(lang_code, 'account')}")
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder=f"{await translator.get_text(lang_code, 'chooseOption')} . . .",
    )
    return keyboard


async def times_currencies(lang_code: str, is_time: bool = True) -> ReplyKeyboardMarkup:
    """
    Build a reply keyboard with dynamic amount buttons and a cancel button.

    Args:
        user_id (int): User's Telegram ID (can be used for user-specific logic)
        lang_code (str): Language code ('en', 'ru', 'uz', etc.)
        amounts (list[int] | None): Optional list of numeric amount options

    Returns:
        ReplyKeyboardMarkup: A dynamic keyboard with amounts and cancel option
    """
    if is_time:
        amounts = await db.get_last_times()
    else:
        amounts = await db.get_last_currencies()

    # Validate the result
    
    rows = []

    # Add amount buttons if provided
    if amounts:
        for i in range(0, len(amounts), 3):
            row = [KeyboardButton(text=str(amount)) for amount in amounts[i:i+3]]
            rows.append(row)

    keyboard = ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        input_field_placeholder=f"{await translator.get_text(lang_code, 'chooseOrEnter')} . . ."
    )

    return keyboard