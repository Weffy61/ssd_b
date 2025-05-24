import asyncio
import logging

from django.core.management import BaseCommand

from .configuration.bot_instance import bot, dp
from .configuration.handlers import commands_router
from .configuration.handlers import search_address_router
from .configuration.handlers import search_phone_router

logging.basicConfig(level=logging.INFO)

dp.include_router(commands_router)
dp.include_router(search_address_router)
dp.include_router(search_phone_router)


async def main():
    await dp.start_polling(bot)


class Command(BaseCommand):
    help = 'Telegram-bot for search data'

    def handle(self, *args, **options):
        try:
            asyncio.run(main())
        except Exception as error:
            logging.error(error)
