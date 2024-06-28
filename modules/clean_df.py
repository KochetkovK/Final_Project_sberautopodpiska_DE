import os
import pandas as pd


# Укажем путь к файлам проекта:
# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '..')


def clean_df(df_sessions, df_hits):

    def clean_df_sessions(df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates()
        df = df.drop(columns=['utm_keyword', 'device_model', 'client_id'])
        df['utm_campaign'] = df['utm_campaign'].fillna('other')
        df['utm_adcontent'] = df['utm_adcontent'].fillna('other')
        df['utm_source'] = df['utm_source'].fillna('other')

        def brand_fanc(x):
            if pd.isna(x.device_brand) or x.device_brand == '(not set)':
                if x.device_brand == 'Macintosh':
                    return 'Apple'
                return 'other'
            return x.device_brand

        df.device_brand = df.apply(brand_fanc, axis=1)

        def os_fanc(x):
            if pd.isna(x.device_os) or x.device_os == '(not set)':
                if x.device_brand == 'Apple':
                    if x.device_category == 'desktop':
                        return 'Macintosh'
                    return 'iOS'
                elif x.device_category != 'desktop':
                    return 'Android'
                return 'other'
            return x.device_os

        df.device_os = df.apply(os_fanc, axis=1)

        return df

    def clean_df_hits(df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(columns=['hit_time',
                              'hit_type',
                              'event_value',
                              'hit_referer',
                              'event_label'])
        df = df.drop_duplicates()
        df['car_brand'] = df.hit_page_path.apply(
            lambda x: x.split('/')[3] if '/cars/all/' in x else 'empty')
        df['car_model'] = df.hit_page_path.apply(
            lambda x: x.split('/')[4] if '/cars/all/' in x else 'empty')
        df = df.drop(columns=['hit_page_path'])

        return df

    df_sessions_new = clean_df_sessions(df_sessions)
    df_hits_new = clean_df_hits(df_hits)
    df_full = pd.merge(df_sessions_new, df_hits_new, how='inner', on='session_id')
    df_full['session_id1'] = [int(x[0]) for x in df_full['session_id'].str.split('.')]
    df_full['session_id2'] = [int(x[1]) for x in df_full['session_id'].str.split('.')]
    df_full['session_id3'] = [int(x[2]) for x in df_full['session_id'].str.split('.')]
    df_full = df_full.drop(columns='session_id')

    df_full_sessions = df_full.groupby(['session_id1',
                                        'session_id2',
                                        'session_id3'], as_index=False).agg({'visit_date': 'first',
                                                                             'visit_time': 'first',
                                                                             'visit_number': 'first',
                                                                             'utm_source': 'first',
                                                                             'utm_medium': 'first',
                                                                             'utm_campaign': 'first',
                                                                             'utm_adcontent': 'first',
                                                                             'device_category': 'first',
                                                                             'device_os': 'first',
                                                                             'device_brand': 'first',
                                                                             'device_screen_resolution': 'first',
                                                                             'device_browser': 'first',
                                                                             'geo_country': 'first',
                                                                             'geo_city': 'first',
                                                                             'session_id1': 'first',
                                                                             'session_id2': 'first',
                                                                             'session_id3': 'first'})

    df_full_hits = df_full[['hit_date', 'hit_number', 'event_category', 'event_action',	'car_brand', 'car_model',
                            'session_id1', 'session_id2', 'session_id3']]

    return df_full_sessions, df_full_hits


if __name__ == '__main__':
    df_hits = pd.read_csv(f'{path}/data/ga_hits.csv').sample(frac=0.25, random_state=200)
    df_sessions = pd.read_csv(f'{path}/data/ga_sessions.csv', low_memory=False).sample(frac=0.25, random_state=200)
    df_full_sessions, df_full_hits = clean_df(df_sessions, df_hits)
    print(df_full_sessions.shape)
    print(df_full_hits.shape)
