from aiogram import Router, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext

from base.management.commands.configuration.keyboards import get_main_menu_kb

router = Router()


@router.message(Command('start'))
async def start_command(message: types.Message) -> None:
    welcome_message = f'Hello, {message.from_user.username}!'
    await message.answer(welcome_message,
                         reply_markup=get_main_menu_kb())


@router.message(Command('cancel'), StateFilter('*'))
async def start_command(message: types.Message, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state is None:
        return
    await message.reply('Canceled',
                        reply_markup=get_main_menu_kb())
    await state.clear()
