from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext

import re
import logging

import app.cmn.transtalor as translator
import app.data.dbContext as db
import app.keyboards.in_line as inKb
import app.keyboards.out_line as outKb

from app.models.models import Category, Dengies, Comment

router = Router()



@router.callback_query(F.data.startswith("in_category"))
async def add_amount1(callback: CallbackQuery, state: FSMContext):
    data = callback.data.replace("in_category_", "")
    cat_id, lng_code, is_ex = data.split(":")
    await state.update_data(some_info = f"{lng_code}:{cat_id}:{is_ex}")
    await translator.smart_sleep(callback.message.delete)
    # Ask the user to enter the amount
    await translator.smart_sleep(
        callback.message.answer,
        text=await translator.get_text(lng_code, "enterAmountIn"),
        reply_markup=await outKb.amounts(callback.from_user.id, lng_code, int(cat_id))
    )
    await state.set_state(Dengies.amount)



@router.message(F.text.in_(translator.get_all_values_by_key(translator.translations, "income")))
async def get_all_categories(message: Message):
    user_id = message.from_user.id
    lng_code = await translator.get_lang_code_by_text_async(translator.translations, 'income', message.text)
    await translator.smart_sleep(
        message.reply,
        text=await translator.get_text(lng_code, 'category'),
        reply_markup=await inKb.get_categories(user_id=user_id, lng_code=lng_code, is_ex=False)
    )


@router.callback_query(F.data.startswith("in_add"))
async def add_category1(callback: CallbackQuery, state: FSMContext):
    data = callback.data.replace("in_add_", "")
    lng_code, max_str, count_str = data.split(":")
    max_count = int(max_str)
    len_count = int(count_str)
    if len_count < max_count:
        # Delete the callback message safely
        await translator.smart_sleep(callback.message.delete)

        # Ask the user to type the category name
        await translator.smart_sleep(
            callback.message.answer,
            text=await translator.get_text(lng_code, 'typeCategoryName'),
            reply_markup=ReplyKeyboardRemove()
        )

        # Answer the callback
        await translator.smart_sleep(
            callback.answer,
            text=await translator.get_text(lng_code, 'giveName')
        )

        # Update state
        await state.update_data(lang_code=f"{lng_code}:0")
        await state.set_state(Category.title)

    else:
        # Delete the callback message safely
        await translator.smart_sleep(callback.message.delete)

        # Inform user about the category limit
        await translator.smart_sleep(
            callback.message.answer,
            text=await translator.get_text(lng_code, "limitCategory")
        )

@router.message(Category.title)
async def add_category2(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not message.text:
        logging.info(f"The user: {user_id} send not text")
        return
    title = message.text.strip()
    data = await state.get_data()
    lng_code, ct_type = str(data.get("lang_code")).split(":")
    bool_type = bool(int(ct_type))
    if not title or re.search(r'[:;"\'\\]<>', title):
        await translator.smart_sleep(
            message.answer,
            text=f"{await translator.get_text(lng_code, 'nameValid1')}\n* * *\n\n':', ';', '<>''\"', '\\'."
        )
        return

    if len(title) > 10:
        await translator.smart_sleep(
            message.answer,
            text=await translator.get_text(lng_code, 'errorName')
        )
        return
    is_exist = await db.is_exist_title(user_id, title, bool_type)
    
    if is_exist is True:
        await translator.smart_sleep(
            message.answer,
            text=await translator.get_text(lng_code, "catExist")
        )
        return
    elif is_exist is False:
        logging.info(f"Reactivated category from user {user_id}")
    else:
        await db.create_category(user_id, title, bool_type)
    
    await translator.smart_sleep(
        message.reply,
        text=await translator.get_text(lng_code, 'categoryCreated'),
        reply_markup=await outKb.main_menu(lng_code)
    )

    await state.clear()

    await translator.smart_sleep(
        message.answer,
        text=await translator.get_text(lng_code, 'category'),
        reply_markup=await inKb.get_categories(user_id=user_id, lng_code=lng_code, is_ex=bool_type)
    )




@router.callback_query(F.data.startswith("in_delete"))
async def delete_cat(callback: CallbackQuery):
    data = callback.data.replace("in_delete_", "")
    lng_code, count_str = data.split(":")
    user_id = callback.from_user.id
    if int(count_str) > 0:
        await translator.smart_sleep(
            callback.message.edit_text,
            text=await translator.get_text(lng_code, 'deleteCategory'),
            reply_markup=await inKb.get_categories(user_id=user_id, lng_code=lng_code, is_ex=False, for_delete=True)
        )
    else:
        await translator.smart_sleep(
            callback.message.edit_text,
            text=await translator.get_text(lng_code, 'noCategories')
        )

