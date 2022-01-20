import os

import psycopg2

from contextlib import closing

pg_creds = {"host": "192.168.1.106",
            "port": 5432,
            "database": "postgres",
            "user": "pguser",
            "password": "secret"}


def read_pg():
    with closing(psycopg2.connect(**pg_creds)) as pg_connection:
        cursor = pg_connection.cursor()

        sql = "select * from public.movies where id < 24"
        cursor.execute(sql)

        # data = cursor.fetchone()
        # print(data)

        with open(os.path.join('.', 'data', 'movies.csv'), 'w') as csv_file:
            cursor.copy_expert('COPY (SELECT * FROM public.movies) TO STDOUT WITH HEADER CSV', csv_file)


if __name__ == '__main__':
    read_pg()
