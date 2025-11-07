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



@router.callback_query(F.data.startswith("ex_category"))
async def add_amount1(callback: CallbackQuery, state: FSMContext):
    data = callback.data.replace("ex_category_", "")
    cat_id, lng_code, is_ex = data.split(":")
    await state.update_data(some_info = f"{lng_code}:{cat_id}")
    await callback.message.delete()
    await callback.message.answer(text=await translator.get_text(lng_code, "enterAmount"), reply_markup=await outKb.amounts(callback.from_user.id, lng_code, int(cat_id)))
    await state.set_state(Dengies.amount)


@router.message(Dengies.amount)
async def add_amount2(message: Message, state: FSMContext):
    amount_str = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    lng_code, cat_id = str(data.get("some_info")).split(":")

    if amount_str in translator.get_all_values_by_key(translator.translations, 'cancel'):
        await message.answer(
            text=await translator.get_text(lng_code, 'delCancel'),
            reply_markup=await outKb.main_menu(lng_code)
        )
        logging.info(f"Cancelled the amount for user {user_id}")
        await state.clear()
        return

    try:
        amount = float(amount_str.replace(" ", "").replace(",", ".") )
        if not (1 <= amount <= 1_000_000_000):
            raise ValueError
    except ValueError:
        logging.info(f"Invalid amount for user {user_id} - {amount_str}")
        await message.answer(await translator.get_text(lng_code, 'invalidAmount'))
        return
    
    
    new_balance = await db.minus_user_balance(user_id, amount)
    if new_balance is None:
        await message.reply(text=await translator.get_text(lng_code, "minusVal"))
        return
    else:
        amount_id = await db.insert_dengies(amount, cat_id, user_id)
        await message.reply(
            text=f"<b>{await db.get_category_name(int(cat_id))}:</b> <i>{amount}</i>\n{await translator.get_text(lng_code, 'rashxodSaved')}\n\n<b>{await translator.get_text(lng_code, "currentBalance")}</b> <span class='tg-spoiler'>{new_balance}</span>",
            parse_mode='HTML',
            reply_markup=await outKb.main_menu(lng_code)
        )

        await message.answer(
            text=await translator.get_text(lng_code, 'addComment'),
            reply_markup=await inKb.add_comment(amount_id, lng_code)
        )

        await state.clear()

@router.message(F.text.in_(translator.get_all_values_by_key(translator.translations, "income")))
async def get_all_categories(message: Message):
    user_id = message.from_user.id
    lng_code = await translator.get_lang_code_by_text_async(translator.translations, 'income', message.text)
    await message.reply(
        text=f"{await translator.get_text(lng_code, 'category')}",
        reply_markup=await inKb.get_categories(user_id=user_id, lng_code=lng_code, is_ex=False)
    )

@router.callback_query(F.data.startswith("ex_add"))
async def add_category1(callback: CallbackQuery, state: FSMContext):
    data = callback.data.replace("ex_add_", "")
    lng_code, max_str, count_str = data.split(":")
    max_count = int(max_str)
    len_count = int(count_str)
    if len_count < max_count:
        await callback.message.delete()
        await callback.message.answer(await translator.get_text(lng_code, 'typeCategoryName'), reply_markup=ReplyKeyboardRemove())
        await callback.answer(await translator.get_text(lng_code, 'giveName'))
        await state.update_data(lang_code = f"{lng_code}:1")
        await state.set_state(Category.title)
    else:
        await callback.message.delete()
        await callback.message.answer(text=await translator.get_text(lng_code, "limitCategory"))


@router.message(Category.title)
async def add_category2(message: Message, state: FSMContext):
    title = message.text.strip()
    user_id = message.from_user.id
    data = await state.get_data()
    lng_code, ct_type = str(data.get("lang_code")).split(":")
    bool_type = bool(int(ct_type))
    if not title or re.search(r'[:;"\'\\]<>', title):
        await message.answer(f"{await translator.get_text(lng_code, 'nameValid1')}\n* * *\n\n':', ';', '<>''\"', '\\'.")
        return

    if len(title) > 10:
        await message.answer(await translator.get_text(lng_code, 'errorName'))
        return
    is_exist = await db.is_exist_title(user_id, title, bool_type)
    
    if is_exist is True:
        await message.answer(text=await translator.get_text(lng_code, "catExist"))
        return
    elif is_exist is False:
        logging.info(f"Reactivated category from user {user_id}")
    else:
        await db.create_category(user_id, title, bool_type)
    
    await message.reply(await translator.get_text(lng_code, 'categoryCreated'), reply_markup=await outKb.main_menu(lng_code))
    await state.clear()

    await message.answer(
        text=f"{await translator.get_text(lng_code, 'category')}",
        reply_markup=await inKb.get_categories(user_id=user_id, lng_code=lng_code, is_ex=bool_type)
    )




@router.callback_query(F.data.startswith("ex_delete"))
async def delete_cat(callback: CallbackQuery):
    data = callback.data.replace("ex_delete_", "")
    lng_code, count_str = data.split(":")
    user_id = callback.from_user.id
    if int(count_str) > 0:
        await callback.message.edit_text(text=await translator.get_text(lng_code, 'deleteCategory'), reply_markup=await inKb.get_categories(user_id=user_id, lng_code=lng_code, for_delete=True))
    else:
        await callback.message.edit_text(await translator.get_text(lng_code, 'noCategories'))


@router.callback_query(F.data.startswith("de_cancel"))
async def del_canceled(callback: CallbackQuery):
    lng_code = callback.data.replace("de_cancel_", "")
    # await callback.message.delete()
    await callback.message.edit_text(text=await translator.get_text(lng_code, "delCancel"))




@router.callback_query(F.data.startswith("de_category"))
async def del_category(callback: CallbackQuery):
    data = callback.data.replace("de_category_", "")
    category_id, lng_code, cat_type = data.split(":")
    user_id = callback.from_user.id
    result = await db.deactivate_category(int(category_id))
    if result == "deactivated":
        await callback.message.edit_text(
            text=f"{await translator.get_text(lng_code, 'category')}",
            reply_markup=await inKb.get_categories(user_id=user_id, lng_code=lng_code, is_ex=bool(int(cat_type))))
        await callback.answer(text=await translator.get_text(lng_code, "deletedMsg"))
    else:
        await callback.message.delete()
        await callback.answer(text=await translator.get_text(lng_code, "invalidDel"))


@router.callback_query(F.data.startswith("addComment_"))
async def add_comment1(callback: CallbackQuery, state: FSMContext):
    data = callback.data.replace("addComment_", "")
    dengies_id, lng_code = data.split(":")
    await callback.message.delete()
    await callback.message.answer(text = await translator.get_text(lng_code, "commentTxt"), reply_markup=ReplyKeyboardRemove())
    await state.update_data(dengies_id_ln_code = f"{dengies_id}:{lng_code}")
    await state.set_state(Comment.comment_text)

@router.message(Comment.comment_text)
async def add_comment2(message: Message, state: FSMContext):
    comment_text = message.text.strip()

    if not comment_text or re.search(r'[:;"\'\\]<>', comment_text):
        await message.answer(f"{await translator.get_text(lng_code, 'commentValid1')}\n* * *\n\n':', ';', '<>''\"', '\\'.")
        return

    data = await state.get_data()
    dengies_id, lng_code = str(data.get("dengies_id_ln_code")).split(":")
    if len(comment_text) > 30:
        await message.answer(await translator.get_text(lng_code, 'errorComment'))
        return
    
    result = await db.update_comment_text(int(dengies_id), comment_text)
    
    if result:
        await message.reply(text = await translator.get_text(lng_code, "succesComment"), reply_markup=await outKb.main_menu(lng_code))
    
    await state.clear()
