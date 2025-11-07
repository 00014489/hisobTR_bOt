from aiogram.fsm.state import StatesGroup, State

class User(StatesGroup):
    datetime_time = State()
    currency = State()
    balans = State()

class Category(StatesGroup):
    title = State()

class Dengies(StatesGroup):
    amount = State()

class Comment(StatesGroup):
    comment_text = State()