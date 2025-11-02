from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
import logging
from datetime import datetime, timedelta
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from datetime import datetime


router = Router()


@router.message(CommandStart())
async def CmdStart(message: Message):
    await message.answer("hello from here")