
import logging
from datetime import datetime

# Настройка логирования для отладки
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
                         CREATE SCHEMA IF NOT EXISTS userstasks;
                         
                         CREATE TABLE IF NOT EXISTS userstasks.users ( 
                            user_id BIGINT not null primary key,
                            username varchar,
                            first_name varchar,
                            last_name varchar,
                            is_bot bool,
                            last_seen timestamp,
                            created_at timestamp DEFAULT NOW(),
                            updated_at timestamp 
                            );
                        
                        CREATE TABLE IF NOT EXISTS userstasks.chats ( 
                            chat_id BIGINT not null primary key,
                            chat_title varchar,
                            chat_type varchar
                            );
                        
                        CREATE TABLE IF NOT EXISTS userstasks.tasks (
                            id serial primary key,
                            task varchar not null,
                            executor_user_id BIGINT REFERENCES userstasks.users(user_id),
                            executor_username varchar,
                            taskmaker_user_id BIGINT REFERENCES userstasks.users(user_id),
                            taskmaker_username varchar,
                            status varchar,
                            created_dt timestamp not null,
                            update_dt timestamp
                            );
                            
                        CREATE TABLE IF NOT EXISTS userstasks.transactions (
                            id serial primary key,
                            task_id int REFERENCES userstasks.tasks(id),
                            changer_user_id BIGINT REFERENCES userstasks.users(user_id),
                            changer_username varchar,
                            status varchar,
                            update_dt timestamp
                            );
                            
                            
                        CREATE SCHEMA IF NOT EXISTS messages;
                            
                            -- Таблица для хранения всех сообщений из групп
                        CREATE TABLE IF NOT EXISTS messages.group_messages (
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
                            updated_at TIMESTAMP DEFAULT NOW()
                        );
                            
                        ---ALTER TABLE messages.group_messages
                        ---ADD CONSTRAINT unique_message_chat 
                        ---UNIQUE (telegram_message_id, telegram_chat_id);
                        
                        ---CREATE INDEX IF NOT EXISTS idx_group_messages_text 
                        ---ON messages.group_messages USING GIN (to_tsvector('russian', message_text))
    
                        ;
        """

        with pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_schema)


    def add_task(self, task, executor_username,taskmaker_user_id,taskmaker_username):

        add_task_sql = """
                        INSERT INTO userstasks.tasks (task,executor_user_id,executor_username, taskmaker_user_id ,taskmaker_username,status,created_dt)
                        VALUES (%(task)s,(SELECT user_id FROM userstasks.users WHERE username = %(executor_username)s), %(executor_username)s,%(taskmaker_user_id)s ,%(taskmaker_username)s,%(status)s,%(created_dt)s)
                        RETURNING id;
        """
        add_task_transaction_sql = """                
                        INSERT INTO userstasks.transactions (task_id,changer_user_id,changer_username,status,update_dt)
                        VALUES (%(task_id)s, %(taskmaker_user_id)s,%(taskmaker_username)s,%(status)s,%(update_dt)s);
        """

        params = {

            'task': task,
            'executor_username': executor_username,
            'status': '🔰',
            'created_dt': datetime.now(),
            'update_dt': datetime.now(),
            'taskmaker_user_id':taskmaker_user_id,
            'taskmaker_username':taskmaker_username
        }

        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(add_task_sql, params)
                task_id = cur.fetchone()[0]
                params['task_id'] = task_id
                cur.execute(add_task_transaction_sql, params)


    def show_all_tasks(self):
        show_tasks_sql = """
                        SELECT id, status, task, executor_username  FROM userstasks.tasks WHERE status != '🏁';
        """
        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(show_tasks_sql)
                    return cur.fetchall()
                except:
                    return False





    def change_status(self,task_id,status,changer_user_id,changer_username):
        change_status_sql = """
                   UPDATE userstasks.tasks SET status = %(status)s, update_dt = %(update_dt)s WHERE id = %(task_id)s;
                   
                   INSERT INTO userstasks.transactions (task_id,changer_user_id,changer_username,status,update_dt)
                   VALUES (%(task_id)s,%(changer_user_id)s,%(changer_username)s,%(status)s,%(update_dt)s);
        """
        params = {
            'status':status,
            'task_id':task_id,
            'update_dt': datetime.now(),
            'changer_user_id': changer_user_id,
            'changer_username':changer_username
        }
        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(change_status_sql,params)


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
                        INSERT INTO userstasks.users (user_id,username,first_name,last_name,is_bot,last_seen)
                        VALUES (%(user_id)s,%(username)s,%(first_name)s,%(last_name)s,%(is_bot)s,%(last_seen)s)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET 
                            username = EXCLUDED.username,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            last_seen = EXCLUDED.last_seen
                            ;
                            
                        INSERT INTO userstasks.chats (chat_id,chat_title,chat_type)
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
                cur.execute(add_user_sql,add_user_params)





db = DataBase(PgConnect())