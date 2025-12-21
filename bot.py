import os

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters, CommandHandler

from database import db



async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.message.from_user.id
    username = update.message.from_user.username or ""
    first_name = update.message.from_user.first_name or ""
    last_name = update.message.from_user.last_name or ""
    bot_username = (await context.bot.get_me()).username
    db.add_user(user_id,username,first_name,last_name)

    await update.message.reply_text(f"Приятно познакомиться {username}!\nДля того чтобы отправить задачу, напиши:\n@{bot_username} 'название задачи' @исполнитель")

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):

    message_text = update.message.text
    bot_username = (await context.bot.get_me()).username
    if not f'@{bot_username}' in message_text:
        return
    messages = message_text.replace(f'@{bot_username}', "").strip().split('@')
    task = messages[0].lower()
    username = messages[1]
    db.add_task(task,username)

    await update.message.reply_text(f'🔰{task}\nВыполняет: @{username}')

async def show_all_tasks(update:Update,context:ContextTypes.DEFAULT_TYPE):
        tasks = db.show_all_tasks()
        answer=''
        for i in tasks:
            answer+=f'{i[0]}. {i[1]}- {i[2]} ({i[3]})\n'

        await update.message.reply_text(f'{answer}')


def main():

    app = Application.builder().token(os.getenv(TOKEN)).build()
    app.add_handler(CommandHandler('start',start_command))
    app.add_handler(CommandHandler('tasks',show_all_tasks))
    app.add_handler(MessageHandler(
        filters.TEXT & (~filters.COMMAND), handle_group_message
    ))


    print('bot starts')

    app.run_polling()


if __name__=='__main__':
    main()