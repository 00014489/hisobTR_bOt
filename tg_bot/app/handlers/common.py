from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
import logging
import app.cmn.transtalor as translator
import app.data.dbContext as db
import app.keyboards.in_line as inKb
import app.keyboards.out_line as outKb
from datetime import datetime, timedelta
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from datetime import datetime
from app.models.models import User

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id
    lang_code = message.from_user.language_code
    supported_lngs = translator.get_all_language_codes(translator.translations)
    if lang_code in supported_lngs:
        await db.insert_or_update_user(user_id, message.from_user.first_name, message.from_user.username or 'no_username', lang_code)
        await get_time(bot, user_id, lang_code)
        await state.update_data(lang_code = lang_code)
        await state.set_state(User.datetime_time)
    else:
        await choosing_lang(user_id, bot)



@router.message(User.datetime_time)
async def get_time2(message: Message, state: FSMContext, bot: Bot):
    date_time = message.text.strip()
    data = await state.get_data()
    lang_code = data.get("lang_code")

    try:
        # Try parsing user input
        user_local_dt = datetime.strptime(date_time, "%Y-%m-%d %H:%M")
    except ValueError:
        await message.reply(text=await translator.get_text(lang_code, 'askTime'))
        return

    now_utc = datetime.utcnow()
    time_offset = user_local_dt - now_utc
    rounded_offset = timedelta(hours=round(time_offset.total_seconds() / 3600))

    # ✅ Validate min/max UTC offset
    min_offset = timedelta(hours=-12)
    max_offset = timedelta(hours=14)

    if rounded_offset < min_offset or rounded_offset > max_offset:
        await message.answer(await translator.get_text(lang_code, 'invalidUtcOffset'))  # You should define this key in your texts
        return

    await state.update_data(datetime_time = rounded_offset)

    await message.reply(
        text=f"✅ {await translator.get_text(lang_code, 'timeUpdated')}",
        reply_markup=await outKb.main_menu(lang_code)
    )
    await get_curriencies(bot, message.from_user.id, lang_code)
    # await message.answer(f"{await translator.get_text(lang_code, "currenciesTxt")}", parse_mode="Markdown")
    await state.set_state(User.currency)


@router.message(User.currency)
async def get_currency(message: Message, state: FSMContext):
    # Normalize input (capitalize)
    currency = message.text.strip().upper()
    user_id = message.from_user.id

    data = await state.get_data()
    lang_code = data.get("lang_code")

    currencies = [
        "UZS",
        "USD",
        "EUR",
        "RUB",
        "GBP",
        "KZT",
        "TRY",
        "AED",
        "CNY",
        "INR",
        "JPY",
        "KRW",
        "CHF",
        "CAD",
        "AUD"
    ]


    # Validate currency (3 letters)
    if len(currency) != 3 or not currency.isalpha():
        logging.info(f"Error {user_id} entered incorect data: {currency}")
        await message.reply(f"{await translator.get_text(lang_code, "currencyError")}")
        return

    if currency not in currencies:
        logging.info(f"Error {user_id} tried to enter {currency} which is not in currencies")
        await message.reply(f"{await translator.get_text(lang_code, "errorCurency")}")
        return

    # Save currency
    await state.update_data(currency=currency)
    
    # Ask for balance
    await message.answer(text=f"{await translator.get_text(lang_code, 'balance')}", reply_markup=ReplyKeyboardRemove())
    await state.set_state(User.balans)


@router.message(User.balans)
async def get_balance(message: Message, state: FSMContext, bot: Bot):
    data_text = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    lang_code = data.get("lang_code")
    currency = data.get("currency")
    rouded_offset = data.get("datetime_time")
    
    if data_text == "/skip":
        #way with skipping the balance update
        await db.update_user_info(user_id=user_id, rounded_offset=rouded_offset, currency=currency)
    else:
        balans_text = data_text.replace(",", ".")
        MAX_BALANCE = 9999999999.99
        formatted_msg = f"{MAX_BALANCE:,.2f}"
        # Validate balance
        try:
            balans = float(balans_text)

            if balans < 0:
                raise ValueError("negative")

            if balans > MAX_BALANCE:
                raise ValueError("too_large")

        except ValueError as e:
            if str(e) == "negative":
                await message.reply(f"{await translator.get_text(lang_code, 'errorBalanceNegative')}")
            elif str(e) == "too_large":
                await message.reply(
                    f"{await translator.get_text(lang_code, 'errorBalanceTooLarge')} {formatted_msg}"
                )
            else:
                await message.answer(f"{await translator.get_text(lang_code, 'errorBalance')}")
            return

        # Save balance
        
        await db.update_user_info(user_id=user_id, rounded_offset=rouded_offset, currency=currency, balans=balans)
    
    # 💬 Copy another message
    try:
        await bot.copy_message(
            chat_id=user_id,  # send to the same user
            from_chat_id=1081599122,         # source chat ID
            message_id=2,            # message ID in that chat
            reply_markup= await outKb.main_menu(lang_code)
        )
    except Exception as e:
        logging.error(f"Failed to copy message: {e}")

    # Finish FSM
    await state.clear()




@router.callback_query(F.data.startswith("lang_"))
async def Lang(callback: CallbackQuery, bot: Bot, state: FSMContext):
    lang_code = callback.data.replace("lang_", "")
    user_id = callback.from_user.id
    await db.insert_or_update_user(
        user_id=user_id,
        first_name=callback.from_user.first_name,
        user_name=callback.from_user.username or "no_usernama",
        language_is=lang_code
    )
    await callback.message.delete()
    await get_time(bot=bot, user_id=user_id, lang_code=lang_code)
    await state.update_data(lang_code = lang_code)
    await state.set_state(User.datetime_time)




async def choosing_lang(user_id: int, bot: Bot, lang_code: str = None):
    await bot.send_message(
        chat_id=user_id,
        text=(
            "Iltimos, kerakli tilni tanlang.\n"
            "* * *\n"
            "Please choose your language.\n"
            "* * *\n"
            "Пожалуйста, выберите язык."
        ),
        reply_markup=await inKb.languages_keyboard(lang_code),
        parse_mode="HTML"  # or "Markdown" if you want formatting
    )


async def get_time(
    bot: Bot,
    user_id: int,
    lang_code: str,
):
    """
    Asynchronously get the current date and time from the user.
    
    Args:
        message (Message): The incoming message from the user.
        bot (Bot): The bot instance.
        user_id (int): The user's ID.
        lang_code (str): Language code for translations.

    Returns:
        bool: True if successful, False otherwise.
    """
    await bot.send_message(
        chat_id=user_id,
        text=await translator.get_text(lang_code, "askTime"),
        reply_markup=await outKb.times_currencies(lang_code = lang_code)
    )

async def get_curriencies(
    bot: Bot,
    user_id: int,
    lang_code: str,
):
    
    await bot.send_message(
        chat_id=user_id,
        text=await translator.get_text(lang_code, "currenciesTxt"),
        parse_mode="Markdown",
        reply_markup=await outKb.times_currencies(lang_code = lang_code, is_time=False)
    )