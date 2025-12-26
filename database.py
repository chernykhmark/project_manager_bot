
from datetime import datetime

from contextlib import contextmanager
from typing import Generator
from dotenv import load_dotenv

load_dotenv()

import psycopg2
from psycopg2.extensions import connection as Connection
import os


class PgConnect:
    def __init__(self) -> None:
        self.host = str(os.getenv('PG_HOST'))
        self.port = int(str(os.getenv('PG_PORT')))
        self.db_name = str(os.getenv('PG_DBNAME'))
        self.user = str(os.getenv('PG_USER'))
        self.pw = str(os.getenv('PG_PASSWORD'))

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw)

    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        conn = psycopg2.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()


class DataBase:

    def __init__(self, pg: PgConnect):

        self.pg = pg

        create_schema = """
                         CREATE SCHEMA IF NOT EXISTS bot_data;

                         CREATE TABLE IF NOT EXISTS bot_data.users ( 
                            user_id BIGINT not null primary key,
                            username varchar,
                            first_name varchar,
                            last_name varchar,
                            is_bot bool,
                            last_seen timestamp,
                            created_at timestamp DEFAULT NOW(),
                            updated_at timestamp 
                            );

                        CREATE TABLE IF NOT EXISTS bot_data.chats ( 
                            chat_id BIGINT not null primary key,
                            chat_title varchar,
                            chat_type varchar
                            );

                        CREATE TABLE IF NOT EXISTS bot_data.tasks (
                            id serial primary key,
                            task varchar not null,
                            executor_user_id BIGINT REFERENCES bot_data.users(user_id),
                            executor_username varchar,
                            taskmaker_user_id BIGINT REFERENCES bot_data.users(user_id),
                            taskmaker_username varchar,
                            status varchar,
                            created_dt timestamp not null,
                            update_dt timestamp
                            );

                        CREATE TABLE IF NOT EXISTS bot_data.transactions (
                            id serial primary key,
                            task_id int REFERENCES bot_data.tasks(id),
                            changer_user_id BIGINT REFERENCES bot_data.users(user_id),
                            changer_username varchar,
                            status varchar,
                            update_dt timestamp
                            );


                            -- Таблица для хранения всех сообщений из групп
                        CREATE TABLE IF NOT EXISTS bot_data.group_messages (
                            id SERIAL PRIMARY KEY,
                            telegram_message_id BIGINT NOT NULL,
                            telegram_chat_id BIGINT NOT NULL,
                            telegram_thread_id INTEGER,  -- ID топика (для форумов)

                            -- Информация об отправителе
                            sender_user_id BIGINT NOT NULL,
                            sender_username VARCHAR(100),
                            sender_first_name VARCHAR(100),
                            sender_last_name VARCHAR(100),
                            sender_is_bot BOOLEAN DEFAULT FALSE,
                            sender_language_code VARCHAR(10),

                            -- Информация о чате
                            chat_type VARCHAR(20) NOT NULL,  
                            chat_title VARCHAR(255),
                            chat_is_forum BOOLEAN DEFAULT FALSE,

                            -- Содержимое сообщения
                            message_type VARCHAR(50) NOT NULL DEFAULT 'text',
                            message_text TEXT,  -- Текст сообщения или подпись

                            -- Медиа информация
                            has_media BOOLEAN DEFAULT FALSE,
                            media_type VARCHAR(50),  
                            media_file_id VARCHAR(255),  
                            media_file_unique_id VARCHAR(255),  
                            media_file_name VARCHAR(255),
                            media_mime_type VARCHAR(100),
                            media_file_size BIGINT,
                            media_duration INTEGER,  
                            media_width INTEGER,     
                            media_height INTEGER,    

                            -- Служебные флаги
                            is_topic_message BOOLEAN DEFAULT FALSE,
                            is_forwarded BOOLEAN DEFAULT FALSE,
                            is_reply BOOLEAN DEFAULT FALSE,

                            -- Ответ на сообщение
                            reply_to_message_id BIGINT,
                            reply_to_user_id BIGINT,

                            -- Форум информация (только для супергрупп)
                            forum_topic_name VARCHAR(255),
                            forum_topic_icon_color INTEGER,


                            -- Пересланные сообщения
                            forward_from_user_id BIGINT,
                            forward_from_user_name VARCHAR(255),
                            forward_date TIMESTAMP,


                            -- Временные метки
                            telegram_date TIMESTAMP NOT NULL,
                            created_at TIMESTAMP DEFAULT NOW(),
                            updated_at TIMESTAMP DEFAULT NOW(),
                            CONSTRAINT unique_message_chat UNIQUE (telegram_message_id, telegram_chat_id)
                            );
                        
                        CREATE INDEX IF NOT EXISTS idx_group_messages_text 
                        ON bot_data.group_messages USING GIN (to_tsvector('russian', message_text)
                        );
        """

        with pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_schema)

    def add_task(self, task, executor_username, taskmaker_user_id, taskmaker_username):

        add_task_sql = """
                        INSERT INTO bot_data.tasks (task,executor_user_id,executor_username, taskmaker_user_id ,taskmaker_username,status,created_dt)
                        VALUES (%(task)s,(SELECT user_id FROM bot_data.users WHERE username = %(executor_username)s), %(executor_username)s,%(taskmaker_user_id)s ,%(taskmaker_username)s,%(status)s,%(created_dt)s)
                        RETURNING id;
        """
        add_task_transaction_sql = """                
                        INSERT INTO bot_data.transactions (task_id,changer_user_id,changer_username,status,update_dt)
                        VALUES (%(task_id)s, %(taskmaker_user_id)s,%(taskmaker_username)s,%(status)s,%(update_dt)s);
        """

        params = {

            'task': task,
            'executor_username': executor_username,
            'status': '🔰',
            'created_dt': datetime.now(),
            'update_dt': datetime.now(),
            'taskmaker_user_id': taskmaker_user_id,
            'taskmaker_username': taskmaker_username
        }

        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(add_task_sql, params)
                task_id = cur.fetchone()[0]
                params['task_id'] = task_id
                cur.execute(add_task_transaction_sql, params)

    def show_all_tasks(self):
        show_tasks_sql = """
                        SELECT id, status, task, executor_username  FROM bot_data.tasks WHERE status != '🏁';
        """
        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(show_tasks_sql)
                    return cur.fetchall()
                except:
                    return False

    def change_status(self, task_id, status, changer_user_id, changer_username):
        change_status_sql = """
                   UPDATE bot_data.tasks SET status = %(status)s, update_dt = %(update_dt)s WHERE id = %(task_id)s;

                   INSERT INTO bot_data.transactions (task_id,changer_user_id,changer_username,status,update_dt)
                   VALUES (%(task_id)s,%(changer_user_id)s,%(changer_username)s,%(status)s,%(update_dt)s);
        """
        params = {
            'status': status,
            'task_id': task_id,
            'update_dt': datetime.now(),
            'changer_user_id': changer_user_id,
            'changer_username': changer_username
        }
        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(change_status_sql, params)

    def add_or_update_user(self,
                           user_id,
                           username,
                           first_name,
                           last_name,
                           chat_id,
                           chat_title,
                           chat_type,
                           is_bot,
                           last_seen
                           ):

        add_user_sql = """
                        INSERT INTO bot_data.users (user_id,username,first_name,last_name,is_bot,last_seen)
                        VALUES (%(user_id)s,%(username)s,%(first_name)s,%(last_name)s,%(is_bot)s,%(last_seen)s)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET 
                            username = EXCLUDED.username,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            last_seen = EXCLUDED.last_seen
                            ;

                        INSERT INTO bot_data.chats (chat_id,chat_title,chat_type)
                        VALUES (%(chat_id)s,%(chat_title)s,%(chat_type)s)
                        ON CONFLICT (chat_id) 
                        DO UPDATE SET 
                            chat_title = EXCLUDED.chat_title,
                            chat_type = EXCLUDED.chat_type
                            ;
                    """
        add_user_params = {
            'user_id': user_id,
            'username': username,
            'first_name': first_name,
            'last_name': last_name,
            'chat_id': chat_id,
            'chat_title': chat_title,
            'chat_type': chat_type,
            'is_bot': is_bot,
            'last_seen': last_seen
        }

        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(add_user_sql, add_user_params)


    def save_message(self, message_data) -> None:

        sql = """
        INSERT INTO bot_data.group_messages (
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
        ;
        """

        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, message_data)



    def media_text_update(self, message_id, text: str):

        media_text_update_sql = """
                                UPDATE bot_data.group_messages SET message_text = %(text)s WHERE telegram_message_id = %(message_id)s;
        """
        media_text_update_params = {
            "message_id": message_id,
            "text": text
        }
        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(media_text_update_sql, media_text_update_params)


db = DataBase(PgConnect())