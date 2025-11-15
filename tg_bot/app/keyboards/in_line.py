from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

import app.cmn.transtalor as translator
import app.data.dbContext as db


async def get_categories(user_id: int, lng_code: str, is_ex: bool = True, for_delete: bool = False) -> InlineKeyboardMarkup:
    """
    Returns an inline keyboard with user's active categories.
    If there are no categories, only 'Add' and 'Delete' buttons are shown.
    """
    # Fetch categories by type
    categories, max_count = await db.get_active_categories_by_type(user_id, is_ex)

    # Define callback prefixes
    if for_delete:
        callback_text = "de_category"
    elif is_ex:
        callback_text = "ex_category"
        add_text = "ex_add"
        delete_text = "ex_delete"
    else:
        callback_text = "in_category"
        add_text = "in_add"
        delete_text= "in_delete"

    buttons = []

    
    if categories:
        other_titles = translator.get_all_values_by_key(translator.translations, "other")
        categories_sorted = sorted(
            categories,
            key=lambda x: x[1] in other_titles
        )
        buttons = [
            [InlineKeyboardButton(text=title, callback_data=f"{callback_text}_{cat_id}:{lng_code}:{int(is_ex)}")]
            for cat_id, title in categories_sorted
        ]

    if for_delete:
        buttons.append([
            InlineKeyboardButton(
                text=await translator.get_text(lng_code, 'cancel'),
                callback_data=f'de_cancel_{lng_code}'
            )
        ])
    else:
        buttons.append([
            InlineKeyboardButton(
                text=await translator.get_text(lng_code, 'create'),
                callback_data=f"{add_text}_{lng_code}:{max_count}:{len(categories)}"
            ),
            InlineKeyboardButton(
                text=await translator.get_text(lng_code, 'delete'),
                callback_data=f"{delete_text}_{lng_code}:{len(categories)}"
            )
        ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def add_comment(amount_id: int, lang_code: str) -> InlineKeyboardMarkup:
    """
    Build an inline keyboard for language selection.
    Button text = full_name (e.g. English, Русский, O'zbek)
    Callback data = lang code (e.g. 'en', 'ru', 'uz')
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.add(
        InlineKeyboardButton(
            text=f"📝 {await translator.get_text(lang_code, 'addCommentBtn')}",
            callback_data=f"addComment_{amount_id}:{lang_code}"
        )
    )

    return keyboard.adjust(1).as_markup()

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

async def premium_and_settings(lang_code: str) -> InlineKeyboardMarkup:
    """
    Build an inline keyboard with Premium and Settings buttons in one row.
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.add(
        InlineKeyboardButton(
            text=await translator.get_text(lang_code, 'premium'),
            callback_data=f"premium_{lang_code}"
        ),
        InlineKeyboardButton(
            text=await translator.get_text(lang_code, 'settings'),
            callback_data=f"settings_{lang_code}"
        )
    )

    return keyboard.adjust(2).as_markup()