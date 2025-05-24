from aiogram.fsm.state import StatesGroup, State


class AddressSearchStatesGroup(StatesGroup):
    person = State()
    result = State()


class PhoneSearchStatesGroup(StatesGroup):
    persons = State()
    person = State()
    result = State()
