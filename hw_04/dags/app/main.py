import json
import os

from requests import Timeout

from client import Client
from config import Config
from logger import logger

DATA_PATH = os.path.join(os.path.dirname(__file__), 'data')

config = Config().get_config('robot-dreams-currency-api')
client = Client(config)


def save_data(data, path):
    with open(path, 'w') as f:
        json.dump(data, f)
    logger.info(f'data is written to file {path}')


def app_0(date):
    logger.info(f'app processing date={date}')
    try:
        data = client.get_data(date)
        if data:
            dir_path = os.path.join(DATA_PATH, date)
            file_path = os.path.join(dir_path, 'data.json')
            os.makedirs(dir_path, exist_ok=True)
            save_data(data, file_path)
    except Timeout:
        logger.info(f'the request timed out for date={date}')


def app(date_list):
    for d in date_list:
        app_0(d)


if __name__ == '__main__':
    date_list = ['2021-09-04', '2021-01-05']
    app(date_list)
