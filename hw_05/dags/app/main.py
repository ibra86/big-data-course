import json
import os
from datetime import datetime

from hdfs import InsecureClient
from requests import Timeout

from client import Client
from config import Config
from logger import logger

config = Config().get_config('robot-dreams-currency-api')
api_client = Client(config)
hdfs_client = InsecureClient('http://localhost:50070/', user='user')

DATE_FMT = '%Y-%m-%d'


def create_file_path(client, date):
    year = str(datetime.strptime(date, DATE_FMT).year)
    year_month = f'{year}_{datetime.strptime(date, DATE_FMT).month:02d}'
    file_name = f'product_{date}.json'
    dir_path = os.path.join('/', 'bronze', 'raw_data', 'product', year, year_month)

    client.makedirs(dir_path)
    file_path = os.path.join(dir_path, file_name)
    return file_path


def app_0(date=None, **kwargs):
    if date is None:
        processing_date = kwargs.get('ds', datetime.today().strftime('%Y-%m-%d'))
        date = processing_date

    logger.info(f'app processing date={date}')
    try:
        data = api_client.get_data(date)
        if data:
            file_path = create_file_path(hdfs_client, date)
            print('file_path:', file_path)
            with hdfs_client.write(file_path, overwrite=True, encoding='utf-8') as writer:
                json.dump(data, writer)
            logger.info(f'data is written to file {file_path}')
    except Timeout:
        logger.info(f'the request timed out for date={date}')


def app(date_list, **kwargs):
    if date_list:
        for d in date_list:
            app_0(d, **kwargs)
    else:
        app_0(**kwargs)


if __name__ == '__main__':
    # date_list = ['2021-09-04', '2021-01-05']
    date_list = []
    app(date_list)
