from aiogram.types import Message, CallbackQuery

async def show_loader(cb_or_msg, toast="Загружаю…", text="⚙️ Загружаю…") -> Message:
    if isinstance(cb_or_msg, CallbackQuery):
        try:
            await cb_or_msg.answer(toast, show_alert=False)
        except:
            pass
        return await cb_or_msg.message.answer(text)
    return await cb_or_msg.answer(text)

async def replace_loader(loader: Message, text: str, **kw):
    try:
        await loader.edit_text(text, **kw)
    except:
        try:
            await loader.answer(text, **kw)
        finally:
            try:
                await loader.delete()
            except:
                pass
