from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from datetime import datetime

import re
import logging

import app.cmn.transtalor as translator
import app.data.dbContext as db
import app.keyboards.in_line as inKb
import app.keyboards.out_line as outKb

from app.models.models import Category, Dengies, Comment

router = Router()



async def generate_profile_message_html(
    user_first_name: str,
    balance: float,
    currency: str,
    lang_code: str,
    is_premium: bool,
    premium_until: str | None,
    monthly_expenses: float,
    monthly_income: float
) -> str:
    """
    Returns a formatted profile message (HTML parse_mode).
    Balance is hidden as a spoiler.
    """

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    status = "Premium" if is_premium else "Standard"

    premium_line = ""
    if is_premium and premium_until:
        premium_label = await translator.get_text(lang_code, "deadlinePremium")
        premium_line = f"\n<i>{premium_label} {premium_until}</i>"

    text = (
        f"👤 <b>{user_first_name}</b>\n\n"
        f"───────\n"
        f"{await translator.get_text(lang_code, 'balanseTxt')} <span class=\"tg-spoiler\">{balance}</span>\n"
        f"{await translator.get_text(lang_code, 'currencyTxt')} {currency}\n"
        f"{await translator.get_text(lang_code, 'languageIs')} {lang_code}\n"
        f"{await translator.get_text(lang_code, 'dateTime')} {now}\n\n"
        f"{await translator.get_text(lang_code, 'typeUser')} {status}{premium_line}\n\n"
        f"{await translator.get_text(lang_code, 'thisMonth')}\n"
        f"   <i>{await translator.get_text(lang_code, 'total_ex')} {monthly_expenses}</i>\n"
        f"   <i>{await translator.get_text(lang_code, 'total_in')} {monthly_income}</i>\n"
        f"───────"
    )

    return text





@router.message(F.text.in_(translator.get_all_values_by_key(translator.translations, "account")))
async def get_profile(message: Message):
    user_id = message.from_user.id
    # Example user from DB
    user = await db.infos_get_user(user_id)
    if user:
        text = await generate_profile_message_html(
            user_first_name=message.from_user.first_name,
            balance=user["balance"],
            currency=user["currency"],
            lang_code=user["lang_code"],
            is_premium=user["is_premium"],
            premium_until=user["premium_date"],
            monthly_expenses=user["monthly_expenses"],
            monthly_income=user["monthly_income"],
        )

        await message.answer(
            text=text,
            reply_markup=await inKb.premium_and_settings(user["lang_code"]),
            parse_mode="HTML"
        )
    else:
        logging.info(f"User {user_id} is not registered")

