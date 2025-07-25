import textwrap
from dataclasses import asdict

from aiogram import types, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from asgiref.sync import sync_to_async

from base.management.commands.configuration.keyboards import get_cancel, get_main_menu_kb
from base.management.commands.configuration.states import AddressSearchStatesGroup
from base.models import Person
from base.search_dc import PersonSearch
from base.tasks import search_by_address

router = Router()


@router.message(F.text.lower() == 'search by address')
async def start_search_by_address(message: types.Message, state: FSMContext) -> None:
    msg = textwrap.dedent('''
       Format for search:

       Fname Lname
       Address
       City
       State
       Zip

       Example for Reference:

       John Connor
       4309 N Montana Ave
       Portland
       OR
       97217
       ''')
    await state.set_state(AddressSearchStatesGroup.person)
    await message.answer(msg, reply_markup=get_cancel())


@router.message(AddressSearchStatesGroup.person)
async def search_by_address_handler(message: types.Message, state: FSMContext) -> None:
    parsed_msg = message.text.split('\n')
    if len(parsed_msg) != 5:
        error_msg = 'You entered something incorrectly, please try again.'
        await message.answer(error_msg, reply_markup=get_cancel())
        return
    full_name, address, city, state_add, zip_code = parsed_msg
    first_name, last_name = full_name.split(' ')
    person = PersonSearch(
        first_name, last_name, address, city, state_add, zip_code
    )
    search_by_address.delay(message.chat.id, person_info=asdict(person))
    await state.set_state(AddressSearchStatesGroup.result)


@router.callback_query(F.data == 'unseen')
async def handle_hide_callback(callback: CallbackQuery) -> None:
    await callback.message.delete()
    await callback.answer()


@router.callback_query(F.data == 'main')
async def handle_main_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer(text='Back to main')
    await callback.message.answer(text='Main menu', reply_markup=get_main_menu_kb())
    await state.clear()


@router.callback_query(lambda p: p.data.startswith('pagination|'))
async def handle_pagination(callback: CallbackQuery):
    try:
        _, page_str, count_str, person_id = callback.data.split('|')
        person = await Person.objects.aget(pk=int(person_id))
        dob_list = await sync_to_async(lambda: list({
            f'{pd.dob.month}/{pd.dob.day}/{pd.dob.year}'
            for pd in person.personal_datas.all() if pd.dob
        }))()

        dobs = '\n'.join(dob_list)

        alt1_dob = await sync_to_async(lambda: list({
            f'{pd.alt1_dob.month}/{pd.alt1_dob.day}/{pd.alt1_dob.year}'
            for pd in person.personal_datas.all() if pd.alt1_dob
        }))()

        alt2_dob = await sync_to_async(lambda: list({
            f'{pd.alt2_dob.month}/{pd.alt2_dob.day}/{pd.alt2_dob.year}'
            for pd in person.personal_datas.all() if pd.alt2_dob
        }))()

        alt3_dob = await sync_to_async(lambda: list({
            f'{pd.alt3_dob.month}/{pd.alt3_dob.day}/{pd.alt3_dob.year}'
            for pd in person.personal_datas.all() if pd.alt3_dob
        }))()
        alt_dobs = set(alt1_dob + alt2_dob + alt3_dob)
        all_alt_dobs = '\nAlt DOBs:\n' + '\n'.join(alt_dobs) if any(alt_dobs) else ''

        aka1_fullname = await sync_to_async(lambda: list({
            pd.aka1_fullname for pd in person.personal_datas.all() if pd.aka1_fullname
        }))()

        aka2_fullname = await sync_to_async(lambda: list({
            pd.aka2_fullname for pd in person.personal_datas.all() if pd.aka2_fullname
        }))()

        aka3_fullname = await sync_to_async(lambda: list({
            pd.aka3_fullname for pd in person.personal_datas.all() if pd.aka3_fullname
        }))()

        aka_full_names = aka1_fullname + aka2_fullname + aka3_fullname
        all_aka_full_names = f'({", ".join(aka_full_names) if any(aka_full_names) else "Has no other names"})'

        found_addresses = await sync_to_async(list)(person.home_addresses.all())
        page = int(page_str)
        count = int(count_str)

        middle_name = person.middle_name if person.middle_name else ''

        if page == 1:
            callback_data = f'pagination|{page + 1}|{count}|{person.pk}'
            buttons = [
                [InlineKeyboardButton(text='Hide', callback_data='unseen')],
                [
                    InlineKeyboardButton(text=f'{page}/{count}', callback_data=f' '),
                    InlineKeyboardButton(text=f'Next --->', callback_data=callback_data)
                ],
                [InlineKeyboardButton(text='Main menu', callback_data='main')],
            ]

        elif page == count:
            callback_data = f'pagination|{page - 1}|{count}|{person.pk}'
            buttons = [
                [InlineKeyboardButton(text='Hide', callback_data='unseen')],
                [
                    InlineKeyboardButton(text=f'<--- Previous', callback_data=callback_data),
                    InlineKeyboardButton(text=f'{page}/{count}', callback_data=f' '),

                ],
                [InlineKeyboardButton(text='Main menu', callback_data='main')],
            ]
        else:
            callback_data_prev = f'pagination|{page - 1}|{count}|{person.pk}'
            callback_data_next = f'pagination|{page + 1}|{count}|{person.pk}'

            buttons = [
                [InlineKeyboardButton(text='Hide', callback_data='unseen')],
                [
                    InlineKeyboardButton(text=f'<--- Previous', callback_data=callback_data_prev),
                    InlineKeyboardButton(text=f'{page}/{count}', callback_data=f' '),
                    InlineKeyboardButton(text=f'Next --->', callback_data=callback_data_next)
                ],
                [InlineKeyboardButton(text='Main menu', callback_data='main')],
            ]
        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        msg_text = textwrap.dedent(f''' 
<b>{person.first_name} {middle_name} {person.last_name}</b>
{all_aka_full_names}\n\n
Full Address: \n{found_addresses[page - 1].address}, 
{found_addresses[page - 1].city}, 
{found_addresses[page - 1].state}, 
{found_addresses[page - 1].zip_code}
{found_addresses[page - 1].phone if found_addresses[page - 1].phone else ""}\n
SSN: \n{person.ssn if person.ssn else 'ssn not found'}\n
DOBs: \n{dobs}
{all_alt_dobs}
                    ''')
        await callback.answer(text=f'Page {page}')
        await callback.message.edit_text(text=msg_text, reply_markup=markup, parse_mode='HTML')
    except IndexError:
        await callback.answer(text=f'Only 1 page')