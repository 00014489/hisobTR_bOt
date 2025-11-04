from aiogram.fsm.state import StatesGroup, State

class User(StatesGroup):
    lang_code = State()
    datetime_time = State()
    currency = State()
    balans = State()