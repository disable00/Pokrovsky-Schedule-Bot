from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import BaseMiddleware, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    TelegramObject,
)

ALLOWED_STATUSES = {"creator", "administrator", "member"}


def make_sub_keyboard(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📣 Подписаться на канал", url=url)],
            [InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_sub")],
        ]
    )


class SubscriptionMiddleware(BaseMiddleware):

    def __init__(self, channel_id: str, news_url: str, admin_id: Optional[int] = None) -> None:
        self.channel_id = channel_id
        self.news_url = news_url
        self.admin_id = admin_id

    async def _is_subscribed(self, bot: Bot, user_id: int) -> bool:
        try:
            member = await bot.get_chat_member(chat_id=self.channel_id, user_id=user_id)
            return getattr(member, "status", None) in ALLOWED_STATUSES
        except TelegramBadRequest:
            return False

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        bot: Bot = data["bot"]

        message: Optional[Message] = None
        user_id: Optional[int] = None
        is_check_cb = False

        if isinstance(event, Message):
            message = event
            user_id = event.from_user.id if event.from_user else None

        elif isinstance(event, CallbackQuery):
            cb: CallbackQuery = event
            message = cb.message
            user_id = cb.from_user.id if cb.from_user else None
            is_check_cb = (cb.data == "check_sub")
          
        if not user_id:
            return await handler(event, data)
          
        if self.admin_id and user_id == self.admin_id:
            return await handler(event, data)

        if is_check_cb:
            return await handler(event, data)
          
        if await self._is_subscribed(bot, user_id):
            return await handler(event, data)

        text = (
            "👋 Чтобы пользоваться ботом, сначала подпишитесь на наш канал.\n"
            "После подписки нажмите «Проверить подписку»."
        )
        if message:
            await message.answer(text, reply_markup=make_sub_keyboard(self.news_url))
        return
