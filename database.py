
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
from psycopg import Connection
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
                            user_id int not null primary key,
                            username varchar,
                            first_name varchar,
                            last_name varchar
                            );
                            
                        CREATE TABLE IF NOT EXISTS userstasks.tasks (
                            id serial primary key,
                            task varchar not null,
                            user_id int REFERENCES userstasks.users(user_id),
                            username varchar,
                            status varchar,
                            created_dt timestamp not null,
                            update_dt timestamp
                            );

        """

        with pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_schema)


    def add_task(self, task, username):

        add_task_sql = """
                        INSERT INTO userstasks.tasks (task,user_id,username,status,created_dt)
                        VALUES (%(task)s,(SELECT user_id FROM userstasks.users WHERE username = %(username)s), %(username)s,%(status)s,%(created_dt)s);
                    """
        params = {

                        'task': task,
                        'username': username,
                        'status': '🔰',
                        'created_dt': datetime.now()
                    }

        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(add_task_sql,params)


    def show_all_tasks(self):
        show_tasks_sql = """
                        SELECT id, status, task, username  FROM userstasks.tasks WHERE status != '🏁';
        """
        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(show_tasks_sql)
                return  cur.fetchall()



    def change_status(self,task_id,status):
        change_status_sql = """
                   UPDATE userstasks.tasks SET status = %(status)s, update_dt = %(update_dt)s WHERE id = %(task_id)s;
        """
        params = {
            'status':status,
            'task_id':task_id,
            'update_dt': datetime.now()
        }
        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(change_status_sql,params)


    def add_user(self,user_id:int, username:str,first_name:str,last_name:str) -> bool:

        def check_registred(user_id):
            check_user = """
                        SELECT user_id FROM userstasks.users WHERE user_id = %(user_id)s LIMIT 1;
                        """
            check_user_params = {
                'user_id': user_id
            }
            with self.pg.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(check_user, check_user_params)
                    if cur.fetchall():
                        return True


        add_user_sql = """
                        INSERT INTO userstasks.users (user_id,username,first_name,last_name)
                        VALUES (%(user_id)s,%(username)s,%(first_name)s,%(last_name)s)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET 
                            username = EXCLUDED.username,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name;
                    """
        add_user_params = {
            'user_id': user_id,
            'username': username,
            'first_name': first_name,
            'last_name': last_name
        }

        with self.pg.connection() as conn:
            with conn.cursor() as cur:
                if check_registred(user_id):
                    return False
                cur.execute(add_user_sql,add_user_params)


db = DataBase(PgConnect())