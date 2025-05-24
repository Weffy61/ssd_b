import textwrap

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from celery import shared_task

from base.management.commands.configuration.utils import extract_house_number
from base.models import Person
from base.search_dc import PersonSearch
from base.tasks.utils import run_async, send_result


@shared_task
def search_by_address(chat_id, person_info=None, person_id=None):
    markup = None
    # person = None
    try:
        if person_id:
            msg_text, markup, success = get_personal_data(person_id)
            # person = True
        else:
            person = None
            person_info = PersonSearch(**person_info)
            persons = Person.objects.filter(
                first_name__icontains=person_info.first_name.strip(),
                last_name__icontains=person_info.last_name.strip()
            ).prefetch_related('home_addresses')

            house_number = extract_house_number(person_info.address)
            for some_person in persons:
                for addr in some_person.home_addresses.all():
                    if (
                            house_number in (addr.address or '')
                            and person_info.city.strip().lower() in (addr.city or '').lower()
                            and person_info.state.strip().lower() == (addr.state or '').lower()
                            and person_info.zip_code.strip() in (addr.zip_code or '')
                    ):

                        person = some_person
                        person_address = addr
                        break
                else:
                    continue
                break

            if person:
                msg_text, markup, success = get_personal_data(person.pk)

            else:
                msg_text = f'No person found with that data'
                success = False
    except Exception as e:
        msg_text = f'Error during search: {str(e)}'
        success = False
    try:
        run_async(send_result, chat_id, msg_text, markup, success)
    except Exception as e:
        print(e)


def get_personal_data(person_id):
    person = Person.objects.get(pk=person_id)
    middle_name = person.middle_name if person.middle_name else ''
    found_addresses = [address for address in person.home_addresses.all()]
    dobs = '\n'.join(set(
        f'{pd.dob.month}/{pd.dob.day}/{pd.dob.year}'
        for pd in person.personal_datas.all() if pd.dob
    ))

    alt1_dob = (
        list(
            set(f'{pd.alt1_dob.month}/{pd.alt1_dob.day}/{pd.alt1_dob.year}'
                for pd in person.personal_datas.all() if pd.alt1_dob)
        )
    )
    alt2_dob = (
        list(
            set(f'{pd.alt2_dob.month}/{pd.alt2_dob.day}/{pd.alt2_dob.year}'
                for pd in person.personal_datas.all() if pd.alt2_dob)
        )
    )
    alt3_dob = (
        list(
            set(f'{pd.alt3_dob.month}/{pd.alt3_dob.day}/{pd.alt3_dob.year}'
                for pd in person.personal_datas.all() if pd.alt3_dob)
        )
    )
    alt_dobs = set(alt1_dob + alt2_dob + alt3_dob)

    all_alt_dobs = '\nAlt DOBs:\n' + '\n'.join(alt_dobs) if any(alt_dobs) else ''

    aka1_fullname = (
        list(set(pd.aka1_fullname for pd in person.personal_datas.all() if pd.aka1_fullname))
    )
    aka2_fullname = (
        list(set(pd.aka2_fullname for pd in person.personal_datas.all() if pd.aka2_fullname))
    )
    aka3_fullname = (
        list(set(pd.aka3_fullname for pd in person.personal_datas.all() if pd.aka3_fullname))
    )
    aka_full_names = aka1_fullname + aka2_fullname + aka3_fullname
    all_aka_full_names = f'({", ".join(aka_full_names) if any(aka_full_names) else "Has no other names"})'

    page = 1
    count = len(found_addresses)

    callback_data = f'pagination|{page + 1}|{count}|{person.pk}'
    buttons = [
        [InlineKeyboardButton(text='Hide', callback_data='unseen')],
        [
            InlineKeyboardButton(text=f'{page}/{count}', callback_data=f' '),
            InlineKeyboardButton(text=f'Next --->', callback_data=callback_data)
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
SSN: \n{person.ssn}\n
DOBs: \n{dobs}
{all_alt_dobs}
                ''')
    success = True
    return msg_text, markup, success
