from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from ssn_dob_bot.settings import TELEGRAM_BOT_TOKEN

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot=bot, storage=MemoryStorage())

