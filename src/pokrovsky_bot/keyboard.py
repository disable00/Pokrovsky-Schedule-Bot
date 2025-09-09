from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📅 Посмотреть расписание")],
        [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="🔔 Новостной канал")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="Выберите действие…"
)
