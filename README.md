# Анализ сайта «СберАвтоподписка»
## 1. EDA
[Датасет] (https://drive.google.com/drive/folders/1rA4o6KHH-M2KMvBLHp5DZ5gioF2q7hZw) представлен двумя таблицами ga_hits.csv и ga_sessions.csv. 
Подробная информация о EDA находится в файле sberautopodpiska.ipynb
## 2. Специализация DE
### Создать базу данных Postgresql
Получить доступ к программной оболочке PostgreSQL:\
`sudo su - postgres` \
Выполнить команду: \
`psql` \
Создаем базу autopodpiska_db\
`CREATE DATABASE autopodpiska_db;`
### Сохранить данные в базу данных
Я реализовал три способа:
1. Медленный, около 35 минут создает таблицы и заливает данные в базу без учета очистки данных.\
Сохроняет данные построчно кусками, с помощью библиотеки psycopg2.\
Запустить скрипт **insert_chunks.py** из папки **modules**
2. Средний, около 15 минут создает таблицы и заливает данные в базу без учета очистки данных.\
Создает таблицы с помощью библиотеки sqlalchemy и сохроняет данные с помощью метода .to_sql 
библиотеки pandas.\
Запустить скрипт **insert_to_sql.py** из папки **modules**
3. Быстрый, около 3 минут создает таблицы и заливает данные в базу без учета очистки данных.\
Cохроняет данные с помощью функции copy из библиотеки psycopg2.\
Запустить скрипт **insert_copy.py** из папки **modules**
### Добавить данные в базу данных
[Новые данные] (https://drive.google.com/drive/folders/10LlyVJeMvVKQJaHRWkeo2t3sqklPRLJd)
Запустить в **Airflow** DAG **to_sql_dag** или **copy_dag.py**



