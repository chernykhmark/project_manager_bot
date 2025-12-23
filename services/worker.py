from telegram import Update
from telegram.ext import ContextTypes

from database import db
# получаем сообщение, разбираем его на мета информацию
# складываем ее в базу

#def archive_message(message:Update):

class MessageSaver:

    def __init__(self, db):
        self.db = db

    async def save_group_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:

        if not update.message:
            return False

        message = update.message
        chat = message.chat

        if chat.type == 'private':
            return False

        allowed_chat_types = ['group', 'supergroup', 'channel']
        if chat.type not in allowed_chat_types:
            return False

        print(f"📨 Сообщение из {chat.type} '{chat.title}' от {message.from_user.username}")


        message_data = self._extract_message_data(message)

        # Сохраняем в БД
        try:
            self._save_to_database(message_data)
            print(f"✅ Сообщение {message.message_id} сохранено в БД")

            # Если есть медиа - сохраняем отдельно
            if message_data['has_media']:
                print(f"✅ Сообщение {message.message_id} это MEDIA file'")

            return True

        except Exception as e:
            print(f"❌ Ошибка сохранения сообщения: {e}")
            return False

    def _extract_message_data(self, message) -> dict:
        """Извлекает ВСЕ данные из сообщения"""

        # ВАЖНО: Все поля из SQL запроса должны быть здесь!
        data = {
            # Основные идентификаторы
            'telegram_message_id': message.message_id,
            'telegram_chat_id': message.chat.id,
            'telegram_thread_id': message.message_thread_id,

            # Отправитель
            'sender_user_id': message.from_user.id,
            'sender_username': message.from_user.username,
            'sender_first_name': message.from_user.first_name,
            'sender_last_name': message.from_user.last_name or '',
            'sender_is_bot': message.from_user.is_bot,
            'sender_language_code': message.from_user.language_code or 'ru',

            # Чат
            'chat_type': message.chat.type,
            'chat_title': message.chat.title or '',
            'chat_is_forum': message.chat.is_forum if hasattr(message.chat, 'is_forum') else False,

            # Сообщение
            'message_type': 'text',
            'message_text': message.text or message.caption or '',

            # Медиа поля (все должны быть!)
            'has_media': False,
            'media_type': None,
            'media_file_id': None,
            'media_file_unique_id': None,
            'media_file_name': None,
            'media_mime_type': None,
            'media_file_size': None,
            'media_duration': None,
            'media_width': None,
            'media_height': None,

            # Флаги состояния
            'is_topic_message': message.is_topic_message,
            'is_forwarded': False,  # ← ДОБАВИЛ ЭТО!
            'is_reply': False,

            # Ответы
            'reply_to_message_id': None,
            'reply_to_user_id': None,

            # Форум
            'forum_topic_name': None,
            'forum_topic_icon_color': None,

            # Пересылки
            'forward_from_user_id': None,
            'forward_from_user_name': None,
            'forward_date': None,

            # Время
            'telegram_date': message.date,
        }

        # Определяем тип сообщения
        message_type = self._determine_message_type(message)
        data['message_type'] = message_type

        # Если есть медиа
        if message_type != 'text':
            media_info = self._extract_media_info(message)
            if media_info:
                data.update(media_info)
                data['has_media'] = True

        # Проверяем пересланные сообщения
        if message.forward_from or message.forward_from_chat:
            data['is_forwarded'] = True
            if message.forward_from:
                data['forward_from_user_id'] = message.forward_from.id
                data['forward_from_user_name'] = message.forward_from.username or message.forward_from.first_name
            if message.forward_date:
                data['forward_date'] = message.forward_date

        # Проверяем ответ
        if message.reply_to_message:
            data['is_reply'] = True
            data['reply_to_message_id'] = message.reply_to_message.message_id
            data[
                'reply_to_user_id'] = message.reply_to_message.from_user.id if message.reply_to_message.from_user else None

        # Форум топик
        if message.reply_to_message and hasattr(message.reply_to_message, 'forum_topic_created'):
            if message.reply_to_message.forum_topic_created:
                data['forum_topic_name'] = message.reply_to_message.forum_topic_created.name
                data['forum_topic_icon_color'] = message.reply_to_message.forum_topic_created.icon_color
        else:
            data['forum_topic_name'] = 'General'
            data['is_topic_message'] = True

        return data

    def _determine_message_type(self, message):
        """Определяет тип сообщения"""
        if message.text:
            return 'text'
        elif message.photo:
            return 'photo' or ''
        elif message.voice:
            return 'voice' or ''
        elif message.document:
            return 'document' ''
        elif message.video:
            return 'video' or ''
        elif message.audio:
            return 'audio' or ''
        elif message.sticker:
            return 'sticker' or ''
        elif message.video_note:
            return 'video_note', ''
        elif message.location:
            return 'location', ''
        elif message.contact:
            return 'contact', ''
        elif message.poll:
            return 'poll'
        elif message.dice:
            return 'dice'
        else:
            return 'unknown', ''

    def _extract_media_info(self, message):
        """Извлекает информацию о медиа"""
        info = {}

        if message.photo:
            # Берем самую большую фотографию
            photo = message.photo[-1]
            info.update({
                'media_type': 'photo',
                'media_file_id': photo.file_id,
                'media_file_unique_id': photo.file_unique_id,
                'media_file_size': photo.file_size,
                'media_width': photo.width,
                'media_height': photo.height
            })
        elif message.voice:
            info.update({
                'media_type': 'voice',
                'media_file_id': message.voice.file_id,
                'media_file_unique_id': message.voice.file_unique_id,
                'media_file_size': message.voice.file_size,
                'media_duration': message.voice.duration,
                'media_mime_type': message.voice.mime_type
            })
        elif message.document:
            info.update({
                'media_type': 'document',
                'media_file_id': message.document.file_id,
                'media_file_unique_id': message.document.file_unique_id,
                'media_file_size': message.document.file_size,
                'media_file_name': message.document.file_name,
                'media_mime_type': message.document.mime_type
            })
        elif message.video:
            info.update({
                'media_type': 'video',
                'media_file_id': message.video.file_id,
                'media_file_unique_id': message.video.file_unique_id,
                'media_file_size': message.video.file_size,
                'media_duration': message.video.duration,
                'media_width': message.video.width,
                'media_height': message.video.height,
                'media_mime_type': message.video.mime_type
            })
        elif message.audio:
            info.update({
                'media_type': 'audio',
                'media_file_id': message.audio.file_id,
                'media_file_unique_id': message.audio.file_unique_id,
                'media_file_size': message.audio.file_size,
                'media_duration': message.audio.duration,
                'media_mime_type': message.audio.mime_type,
                'media_file_name': message.audio.file_name or message.audio.title
            })
        elif message.sticker:
            info.update({
                'media_type': 'sticker',
                'media_file_id': message.sticker.file_id,
                'media_file_unique_id': message.sticker.file_unique_id,
                'media_file_size': message.sticker.file_size
            })

        return info if info else None

    def _save_to_database(self, message_data) -> int:

        sql = """
        INSERT INTO messages.group_messages (
            telegram_message_id, telegram_chat_id, telegram_thread_id,
            sender_user_id, sender_username, sender_first_name, sender_last_name,
            sender_is_bot, sender_language_code,
            chat_type, chat_title, chat_is_forum,
            message_type, message_text,
            has_media, media_type, media_file_id, media_file_unique_id,
            media_file_name, media_mime_type, media_file_size, media_duration,
            media_width, media_height,
            is_topic_message, is_reply, is_forwarded,
            reply_to_message_id, reply_to_user_id,
            forum_topic_name, forum_topic_icon_color,
            forward_from_user_id, forward_from_user_name, forward_date,
            telegram_date
        ) VALUES (
            %(telegram_message_id)s, %(telegram_chat_id)s, %(telegram_thread_id)s,
            %(sender_user_id)s, %(sender_username)s, %(sender_first_name)s, %(sender_last_name)s,
            %(sender_is_bot)s, %(sender_language_code)s,
            %(chat_type)s, %(chat_title)s, %(chat_is_forum)s,
            %(message_type)s, %(message_text)s,
            %(has_media)s, %(media_type)s, %(media_file_id)s, %(media_file_unique_id)s,
            %(media_file_name)s, %(media_mime_type)s, %(media_file_size)s, %(media_duration)s,
            %(media_width)s, %(media_height)s,
            %(is_topic_message)s, %(is_reply)s, %(is_forwarded)s,
            %(reply_to_message_id)s, %(reply_to_user_id)s,
            %(forum_topic_name)s, %(forum_topic_icon_color)s,
            %(forward_from_user_id)s, %(forward_from_user_name)s, %(forward_date)s,
            %(telegram_date)s
        )
        ON CONFLICT (telegram_message_id, telegram_chat_id) 
        DO UPDATE SET
            message_text = EXCLUDED.message_text,
            has_media = EXCLUDED.has_media,
            media_type = EXCLUDED.media_type,
            updated_at = NOW()
        RETURNING id;
        """

        with self.db.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, message_data)


class MediaSaver:
    def __init__(self,db):
        self.db = db

    async def save_group_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE):

        if not update.message:
            return False

        message = update.message
        chat = message.chat

        if chat.type == 'private':
            return False

        allowed_chat_types = ['group', 'supergroup', 'channel']
        if chat.type not in allowed_chat_types:
            return False

        print(f"📨 Сообщение из {chat.type} '{chat.title}' от {message.from_user.username}")

        message_data = MessageSaver(db)._extract_message_data(message)

        # Сохраняем в БД
        try:
            if message_data['has_media']:
                print(f"✅ Сообщение {message.message_id} это MEDIA file'")

            return True

        except Exception as e:
            print(f"❌ Ошибка сохранения сообщения: {e}")
            return False


