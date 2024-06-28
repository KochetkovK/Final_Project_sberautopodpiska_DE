import os
import pandas as pd
import psycopg2
import time
from io import StringIO
from modules.clean_df import clean_df

# Укажем путь к файлам проекта:
# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '..')

start = time.time()

df_hits = pd.read_csv(f'{path}/data/ga_hits.csv').sample(frac=0.25, random_state=200)
df_sessions = pd.read_csv(f'{path}/data/ga_sessions.csv', low_memory=False).sample(frac=0.25, random_state=200)
df_full_sessions, df_full_hits = clean_df(df_sessions, df_hits)


end_df_clean = time.time()
start_insert = time.time()

with psycopg2.connect(dbname='autopodpiska_db', user='postgres', password=1234, host='localhost') as conn:
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS hits;
            DROP TABLE IF EXISTS sessions;
            """)
        conn.commit()

        cur.execute("""
            CREATE TABLE sessions (
                visit_date date,
                visit_time time,
                visit_number int,
                utm_source varchar(30),
                utm_medium varchar(25),
                utm_campaign varchar(30),
                utm_adcontent varchar(30),
                device_category varchar(10),
                device_os varchar(20),
                device_brand varchar(20),
                device_screen_resolution varchar(15),
                device_browser varchar(40),
                geo_country varchar(30),
                geo_city varchar(35),
                session_id1 bigint,
                session_id2 bigint,
                session_id3 bigint)
            """)

        cur.execute("""
            CREATE TABLE hits (
                hit_id serial,
                hit_date date,
                hit_number int,
                event_category varchar(40),
                event_action varchar(50),
                car_brand varchar(20),
                car_model varchar(25),
                session_id1 bigint,
                session_id2 bigint,
                session_id3 bigint)
            """)
        conn.commit()

    sio = StringIO()
    sio.write(df_full_sessions.to_csv(index=None, header=None))
    sio.seek(0)
    with conn.cursor() as cur:
        cur.copy_from(
            file=sio,
            table="sessions",
            columns=['visit_date',
                     'visit_time',
                     'visit_number',
                     'utm_source',
                     'utm_medium',
                     'utm_campaign',
                     'utm_adcontent',
                     'device_category',
                     'device_os',
                     'device_brand',
                     'device_screen_resolution',
                     'device_browser',
                     'geo_country',
                     'geo_city',
                     'session_id1',
                     'session_id2',
                     'session_id3'],
            sep=","
        )
        conn.commit()

    sio = StringIO()
    sio.write(df_full_hits.to_csv(index=None, header=None))
    sio.seek(0)
    with conn.cursor() as cur:
        cur.copy_from(
            file=sio,
            table="hits",
            columns=['hit_date',
                     'hit_number',
                     'event_category',
                     'event_action',
                     'car_brand',
                     'car_model',
                     'session_id1',
                     'session_id2',
                     'session_id3'],
            sep=","
        )
        conn.commit()

    with conn.cursor() as cur:
        cur.execute("""
                    ALTER TABLE hits
                    ADD CONSTRAINT PK_hits_hit_id PRIMARY KEY(hit_id)
                    """)
        cur.execute("""
                    ALTER TABLE sessions
                    ADD CONSTRAINT PK_sessions_session_id PRIMARY KEY(session_id1, session_id2, session_id3)
                    """)
        cur.execute("""
                    ALTER TABLE hits
                    ADD CONSTRAINT FK_hits_session_id FOREIGN KEY(session_id1, session_id2, session_id3)
                    REFERENCES sessions(session_id1, session_id2, session_id3)
                    """)
        conn.commit()

    end_insert = time.time()

    if __name__ == '__main__':

        print(f'Количество визитов в датафрейме: {df_full_sessions.shape[0]}')
        print(f'Количество событий в датафрейме: {df_full_hits.shape[0]}')
        print(f'Время очистки данных: {end_df_clean - start} c')

        with conn.cursor() as cur:
            cur.execute("SELECT count(1) FROM sessions;")
            print(f'Количество визитов в базе данных: {cur.fetchone()[0]}')
            cur.execute("SELECT count(1) FROM hits;")
            print(f'Количество событий в базе данных: {cur.fetchone()[0]}')

        print(f'Время добавления данных в базу : {end_insert - start_insert} c')
        print(f'Время работы программы : {end_insert - start} c')




