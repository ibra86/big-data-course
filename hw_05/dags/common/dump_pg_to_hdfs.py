from contextlib import closing

import psycopg2
from hdfs import InsecureClient

client = InsecureClient('http://localhost:50070/', user='user')


def dump_pg_to_hdfs(pg_creds, dump_hdfs_dir_name):
    client.makedirs(dump_hdfs_dir_name)

    with closing(psycopg2.connect(**pg_creds)) as pg_connection:
        cursor = pg_connection.cursor()
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        cursor.execute(sql)
        tables = [z[0] for z in list(cursor)]

        for t in tables:
            print(f'table: {t}')
            with client.write(f'{dump_hdfs_dir_name}/{t}.csv', overwrite=True) as writer:
                cursor.copy_to(writer, t)
