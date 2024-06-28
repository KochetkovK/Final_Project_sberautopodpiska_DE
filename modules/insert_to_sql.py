import pandas as pd
import time
from sqlalchemy import create_engine, text, PrimaryKeyConstraint, ForeignKeyConstraint
from sqlalchemy import Column, Integer, String, Time, Date, BigInteger
from sqlalchemy.orm import declarative_base
from modules.clean_df import clean_df


start = time.time()

df_hits = pd.read_csv(f'../data/ga_hits.csv')  # .sample(frac=0.25, random_state=200)
df_sessions = pd.read_csv(f'../data/ga_sessions.csv', low_memory=False)  # .sample(frac=0.25, random_state=200)
df_full_sessions, df_full_hits = clean_df(df_sessions, df_hits)

end_df_clean = time.time()
start_insert = time.time()

engine = create_engine('postgresql+psycopg2://postgres:1234@localhost:5432/autopodpiska_db')

Base = declarative_base()


class SessionsTable(Base):
    __tablename__ = 'sessions'
    visit_date = Column(Date)
    visit_time = Column(Time)
    visit_number = Column(String(30))
    utm_source = Column(String(30))
    utm_medium = Column(String(25))
    utm_campaign = Column(String(30))
    utm_adcontent = Column(String(30))
    device_category = Column(String(10))
    device_os = Column(String(20))
    device_brand = Column(String(20))
    device_screen_resolution = Column(String(15))
    device_browser = Column(String(40))
    geo_country = Column(String(30))
    geo_city = Column(String(35))
    session_id1 = Column(BigInteger)
    session_id2 = Column(BigInteger)
    session_id3 = Column(BigInteger)
    __table_args__ = (PrimaryKeyConstraint('session_id1', 'session_id2', 'session_id3'),)


class HitsTable(Base):
    __tablename__ = 'hits'
    hit_id = Column(Integer, primary_key=True)
    hit_date = Column(Date)
    hit_number = Column(Integer)
    event_category = Column(String(40))
    event_action = Column(String(50))
    car_brand = Column(String(20))
    car_model = Column(String(25))
    session_id1 = Column(BigInteger)
    session_id2 = Column(BigInteger)
    session_id3 = Column(BigInteger)
    __table_args__ = (ForeignKeyConstraint(
        ['session_id1', 'session_id2', 'session_id3'],
        ['sessions.session_id1', 'sessions.session_id2', 'sessions.session_id3'],
        ondelete="CASCADE"),)


Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

df_full_sessions.to_sql('sessions', engine, if_exists='append', index=False, chunksize=10_000)
df_full_hits.to_sql('hits', engine, if_exists='append', index=False, chunksize=10_000)

end_insert = time.time()

if __name__ == '__main__':
    print(f'Количество визитов в датафрейме: {df_full_sessions.shape[0]}')
    print(f'Количество событий в датафрейме: {df_full_hits.shape[0]}')
    print(f'Время очистки данных: {end_df_clean - start} c')

    with engine.connect() as conn:
        res1 = conn.execute(text('SELECT COUNT(1) FROM sessions'))
        res2 = conn.execute(text('SELECT COUNT(1) FROM hits'))

        print(f'Количество визитов в базе данных: {res1.fetchone()[0]}')
        print(f'Количество событий в базе данных: {res2.first()[0]}')

    print(f'Время добавления данных в базу : {end_insert - start_insert} c')
    print(f'Время работы программы : {end_insert - start} c')

