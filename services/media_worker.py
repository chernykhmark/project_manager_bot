from telegram import Update
from dotenv import load_dotenv
load_dotenv()
import whisper
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging


logger = logging.getLogger(__name__)

WHISPER_MODEL = whisper.load_model("small")

class MediaSaver:
    def __init__(self, db, storage_path: str = str(os.getenv('LOCAL_PATH'))):
        logger.info(f" 🔰 MediaSaver инициализирован. Путь: {storage_path}")
        self.db = db
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)
        self.model = WHISPER_MODEL
        self._executor = ThreadPoolExecutor(max_workers=2)

    async def save_group_media(self, update: Update, context):
        message = update.effective_message
        message_id = message.message_id
        mime_type = None
        if message.photo:
            file = await message.photo[-1].get_file()
            ext = "jpg"
        elif message.audio:
            file = await message.audio.get_file()
            ext = message.audio.file_name.split('.')[-1] if message.audio.file_name else "mp3"
            mime_type = 'audio'
        elif message.video:
            file = await message.video.get_file()
            ext = message.video.file_name.split('.')[-1] if message.video.file_name else "mp4"
            mime_type = 'video'
        elif message.document:
            file = await message.document.get_file()
            ext = message.document.file_name.split('.')[-1] if message.document.file_name else "bin"
            mime_type = 'document'
        elif message.voice:
            file = await message.voice.get_file()
            ext = "ogg"
            mime_type = 'voice'
        elif message.video_note:
            file = await message.video_note.get_file()
            ext = "mp4"
            mime_type = 'video_note'
        else:
            return None

        filename = f"message_id_{message_id}.{ext}"
        logger.info(self.storage_path)
        file_path = os.path.join(self.storage_path,  filename)
        logger.info(file_path)
        try:
            await file.download_to_drive(file_path)
        except Exception as e:
            logger.info(e)


        # Добавь запись в БД и очередь обработки
        logger.info({
            "START" : " 🔄",
            "file_path": file_path,
            "message_id": message.message_id,
            "mime_type": mime_type
        })

        try:
            await self.extract_text_from_media(file_path,mime_type,message_id)
            os.remove(file_path)
            logger.info(f'REMOVED FILE {file_path}')
        except Exception as e:
            logger.info(e)


    async def extract_text_from_media(self,file_path: str, mime_type: str,message_id : int) -> str:
        if mime_type.startswith('voice') or mime_type.startswith('video_note') or mime_type.startswith('audio') or mime_type.startswith('video'):
            # Асинхронная транскрипция
            try:
                text = await self.transcribe_async(file_path)
                self.db.media_text_update(message_id, text)
                logger.info(f'✅message_id:{message_id} saved to database')
            except Exception as e:
                logger.info(f'❌message_id:{message_id} cannot be saved with error: \n{e}\n')


    async def transcribe_async(self, file_path):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._transcribe_sync,
            file_path
        )

    def _transcribe_sync(self, file_path):
        return self.model.transcribe(file_path)["text"]


