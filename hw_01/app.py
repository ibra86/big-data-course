import json
import os

from client import Client
from config import Config
from constants import DATA_PATH
from logger import logger

config = Config().get_config('robot-dreams-currency-api')
client = Client(config)


def save_data(data, path):
    with open(path, 'w') as f:
        json.dump(data, f)
    logger.info(f'data is written to file {path}')


def app(date):
    logger.info(f'app processing date={date}')
    data = client.get_data(date)
    if data:
        dir_path = os.path.join(DATA_PATH, date)
        file_path = os.path.join(dir_path, 'data.json')
        os.makedirs(dir_path, exist_ok=True)
        save_data(data, file_path)


if __name__ == '__main__':
    date_list = ['2021-09-04', '2021-01-05']
    for d in date_list:
        app(d)
