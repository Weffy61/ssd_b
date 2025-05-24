import textwrap

from aiogram import types, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery

from base.management.commands.configuration.keyboards import get_cancel
from base.management.commands.configuration.states import PhoneSearchStatesGroup
from base.tasks import search_by_phone
from base.tasks.search_address import get_personal_data, search_by_address

router = Router()


@router.message(F.text.lower() == 'search by phone')
async def start_search_by_phone(message: types.Message, state: FSMContext) -> None:
    msg = textwrap.dedent('''
       Format for search:

       phone_number

       Example for Reference:

       9072744816
       ''')
    await state.set_state(PhoneSearchStatesGroup.persons)
    await message.answer(msg, reply_markup=get_cancel())


@router.message(PhoneSearchStatesGroup.persons)
async def search_by_phone_handler(message: types.Message, state: FSMContext) -> None:
    phone = str(message.text).strip()
    if len(phone) != 10:
        error_msg = 'You entered something incorrectly, phone length must be 10 symbols, please try again.'
        await message.answer(error_msg, reply_markup=get_cancel())
        return
    search_by_phone.delay(phone, message.chat.id)


@router.callback_query(lambda p: p.data.startswith('person_id|'))
async def handle_person(callback: CallbackQuery):
    _, person_id = callback.data.split('|')
    await callback.answer()
    search_by_address.delay(callback.message.chat.id, person_id=person_id)


@router.callback_query(lambda p: p.data.startswith('separator'))
async def handle_separator(callback: CallbackQuery):
    await callback.answer()

    # await state.set_state(AddressSearchStatesGroup.result)
    # msg_text, markup, success = get_personal_data(person_id)
