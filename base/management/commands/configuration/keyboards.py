from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def get_main_menu_kb() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text='Search by address')],
        [KeyboardButton(text='Search by phone')],
    ]
    keyboard = ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder='Select the search mode'
    )
    return keyboard


def get_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='/cancel')]], resize_keyboard=True)
