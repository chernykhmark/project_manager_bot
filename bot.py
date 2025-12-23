import os

from telegram import Update, InlineKeyboardButton,InlineKeyboardMarkup
from telegram.ext import Application, ContextTypes, MessageHandler, filters, CommandHandler, CallbackQueryHandler

from database import db
from services.worker import MessageSaver,MediaSaver


def user_chat(update:Update):
    user = update.effective_user
    chat = update.effective_chat
    db.add_or_update_user(
        user_id=user.id,
        username=user.username or "",
        first_name=user.first_name or "",
        last_name=user.last_name or "",
        chat_id=chat.id,
        chat_title=chat.title if hasattr(chat, 'title') else "Private chat",
        chat_type=chat.type,
        is_bot=user.is_bot,
        last_seen=update.message.date if update.message else None
    )
    return user,chat


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user, chat = user_chat(update)

    bot_username = (await context.bot.get_me()).username

    await update.message.reply_text(f"Мы уже знакомы {user.username}!\nДля того чтобы отправить задачу, напиши:\n\n@{bot_username} 'текст задачи' @исполнитель\n\nПосмотреть список задач можно по команде /tasks")


async def handle_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user, chat = user_chat(update)
    taskmaker_user_id= user.id
    taskmaker_username = user.username

    message_text = update.message.text

    bot_username = (await context.bot.get_me()).username

    if message_text.startswith(f'@{bot_username}'):
        message = message_text.replace(f'@{bot_username}', "").strip()
        # Проверка 1: пустое сообщение
        if message_text == "":
            await update.message.reply_text(
                f"Чтобы добавить задачу используй структуру:\n\n@{bot_username} 'текст задачи' @исполнитель\n\nПосмотреть список задач можно по команде /tasks")
            return  # Выходим из функции

        # Разделяем по @
        parts = message.split('@', 1)  # Разделяем только по первому @

        if len(parts) != 2:
            await update.message.reply_text(f"Я не понимаю используй формат\n\n@{bot_username} 'текст задачи' @исполнитель")
            return

        # Проверка 2: нет исполнителя
        if len(parts) == 1:
            await update.message.reply_text("Надо добавить исполнителя")
            return

        # Проверка 3: больше 2-х исполнителей
        if len(parts) > 3:
            await update.message.reply_text('У задачи не может быть больше двух исполнителей')
            return

        # Проверка 4: правильный формат (1 задача и 1 исполнитель)
        if len(parts) == 2:
            task = parts[0].strip().lower()
            after_at = parts[1].strip()
            username_parts = after_at.split()
            executor_username = username_parts[0] if username_parts else ""

            if not task:
                await update.message.reply_text("Задача не может быть пустой!")
                return

            if not executor_username:
                await update.message.reply_text("Имя исполнителя не может быть пустым!")
                return

            # Добавляем в БД
            db.add_task(task, executor_username,taskmaker_user_id,taskmaker_username)
            await update.message.reply_text(f'🔰 {task}\nВыполняет: @{executor_username}')
            return

        # Если что-то пошло не так
        await update.message.reply_text(f"Неверный формат. Используйте:\n@{bot_username} задача @исполнитель")

    await MessageSaver(db).save_group_message(update,context)

async def handle_media(update:Update,context:ContextTypes.DEFAULT_TYPE):
    await MessageSaver(db).save_group_message(update, context)
    #await MediaSaver(db).save_group_media(update,context)


async def show_all_tasks(update:Update,context:ContextTypes.DEFAULT_TYPE):

        user, chat = user_chat(update)

        task_list_tuples = db.show_all_tasks()
        if not task_list_tuples:
            await update.message.reply_text('Пока еще не было создано ни одной задачи')
            return

        task_list_tuples.sort(key=lambda x: x[0])
        answer=''
        for i in task_list_tuples:
            answer+=f'{i[0]}. {i[1]}- {i[2]} ({i[3]})\n'

        keyboard = []
        keyboard.append([
            InlineKeyboardButton("Изменить статус задачи", callback_data="change_task")
        ])
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(f'{answer}',reply_markup=reply_markup)




async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    callback_data = query.data
    changer_user_id = query.from_user.id
    changer_username = query.from_user.username

    await query.answer()

    task_list_tuples = db.show_all_tasks()
    if not task_list_tuples:
        await update.message.reply_text('Пока еще не было создано ни одной задачи')
        return
    task_list_tuples.sort(key=lambda x: x[0])
    answer = ''
    for i in task_list_tuples:
        answer += f'{i[0]}. {i[1]}- {i[2]} ({i[3]})\n'

    if callback_data == "change_task":

        tasks_numbers = [i[0] for i in task_list_tuples]
        # Создаем сетку 4 колонки
        columns = 4
        keyboard = []

        for i in range(0, len(tasks_numbers), columns):
            row_numbers = tasks_numbers[i:i + columns]
            row_buttons = [
                InlineKeyboardButton(str(num), callback_data=f"selected_task_{num}")
                for num in row_numbers
            ]
            keyboard.append(row_buttons)
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            text=f"{answer}\nВыбери номер задачи:",
            reply_markup=reply_markup
        )

    if callback_data.startswith("selected_task_"):
        task_id = int(callback_data.split("_")[2])
        context.user_data['selected_task_id'] = task_id

        keyboard = [
            [
                InlineKeyboardButton("🔄 Начали", callback_data="status_🔄"),
                InlineKeyboardButton("❌ Отменена", callback_data="status_❌")

            ],
            [
                InlineKeyboardButton("✅ Выполнена", callback_data="status_✅"),
                InlineKeyboardButton("🔰 Новая", callback_data="status_🔰")
            ],
            [
                InlineKeyboardButton("🏁 Завершена", callback_data="status_🏁")
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            text=f"{answer}\nКакой статус поставим?",
            reply_markup=reply_markup
        )

    if callback_data.startswith("status_"):
        status = callback_data.split("_")[1]
        task_id = context.user_data.get('selected_task_id')
        db.change_status(task_id=task_id, status=status,changer_user_id=changer_user_id,changer_username=changer_username)

        keyboard = []
        keyboard.append([
            InlineKeyboardButton("Изменить статус задачи", callback_data="change_task")
        ])
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            text=f"{answer}\nЗадача # {task_id} получила статус {status}",
            reply_markup=reply_markup
        )




def main():

    app = Application.builder().token(os.getenv('TOKEN')).build()
    app.add_handler(CommandHandler('start',start_command))
    app.add_handler(CommandHandler('tasks',show_all_tasks))
    app.add_handler(MessageHandler(
        filters.TEXT & (~filters.COMMAND), handle_messages,
    ))
    app.add_handler(MessageHandler(
        filters.ALL & (~filters.COMMAND), handle_media,
    ))


    app.add_handler(CallbackQueryHandler(button_callback))

    print('bot starts')

    app.run_polling()


if __name__=='__main__':
    main()