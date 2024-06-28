import json
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from modules.clean_df import clean_df
from io import StringIO


path = os.environ.get('PROJECT_PATH', '..')
path_new_json = f'{path}/data/extra_de_dataset'  # путь до новых файлов


def clean_json():
    from modules.clean_df import clean_df
    data_hits = []
    data_sessions = []
    for filename in os.listdir(path_new_json):
        with open(f'{path_new_json}/{filename}', 'r') as file:
            if filename.startswith('ga_hits'):
                data = json.loads(file.read())
                data_hits += list(data.values())[0]
            elif filename.startswith('ga_sessions'):
                data = json.loads(file.read())
                data_sessions += list(data.values())[0]

    df_hits_json = pd.DataFrame(data_hits)
    df_sessions_json = pd.DataFrame(data_sessions)
    df_full_sessions_json, df_full_hits_json = clean_df(df_sessions_json, df_hits_json)
    return df_full_sessions_json, df_full_hits_json


def insert_json_to_sql() -> None:
    df_full_sessions_json, df_full_hits_json = clean_json()
    engine = create_engine('postgresql://postgres:1234@localhost:5432/autopodpiska_db')
    df_full_sessions_json.to_sql('sessions', engine, if_exists='append', index=False, chunksize=10_000)
    df_full_hits_json.to_sql('hits', engine, if_exists='append', index=False, chunksize=10_000)


def insert_json_copy() -> None:
    df_full_sessions_json, df_full_hits_json = clean_json()
    with psycopg2.connect(dbname='autopodpiska_db', user='postgres', password=1234, host='localhost') as conn:
        sio = StringIO()
        sio.write(df_full_sessions_json.to_csv(index=None, header=None))
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
        sio.write(df_full_hits_json.to_csv(index=None, header=None))
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
