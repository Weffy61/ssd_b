from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from celery import shared_task

from base.models import Person, PersonAddress
from base.tasks.utils import run_async, send_result


@shared_task
def search_by_phone(phone_number, chat_id):
    markup = None
    success = False
    try:
        persons_addresses = PersonAddress.objects.filter(phone=phone_number).prefetch_related('persons')
        # persons_addresses = PersonAddress.objects.filter(phone=phone_number).select_related('persons')
        persons = []
        for person_address in persons_addresses:
            for person in person_address.persons.all():
                if person not in persons:
                    persons.append(person)
        if persons:
            page = 1
            count = 1 if len(persons) <= 5 else len(persons) // 5
            # callback_data = f'persons|{page + 1}|{count}|{person.pk}'
            persons_count = len(persons)
            buttons = [
                [InlineKeyboardButton(
                    text=f'{person.first_name} {person.last_name}',
                    callback_data=f'person_id|{person.pk}') for person in persons if person],
                [InlineKeyboardButton(text='\u200B', callback_data='separator')],
                [InlineKeyboardButton(text='Hide', callback_data='unseen')],
                [InlineKeyboardButton(text='Main menu', callback_data='main')],
            ]
            markup = InlineKeyboardMarkup(inline_keyboard=buttons)
            msg_text = 'Please select the person'
            success = True

        else:
            msg_text = 'No person found with that data'
    except Exception as ex:
        msg_text = f'Error during search: {str(ex)}'
    try:
        run_async(send_result, chat_id, msg_text, markup, success)
    except Exception as e:
        print(e)
