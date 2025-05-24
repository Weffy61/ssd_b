import asyncio

from ..management.commands.configuration.bot_instance import bot
from ..management.commands.configuration.keyboards import get_main_menu_kb


def run_async(func, *args, **kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(func(*args, **kwargs))


async def send_result(chat_id, text, markup, success):
    if success:
        await bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
    else:
        markup = get_main_menu_kb()
        await bot.send_message(chat_id, text, reply_markup=markup)
    await bot.session.close()
