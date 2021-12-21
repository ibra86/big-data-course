from http.client import HTTPException

import requests

from logger import logger


class Client:

    def __init__(self, config):
        self.config = config
        self.header = {'Content-Type': 'application/json'}

    @property
    def url(self):
        return self.config.get('url')

    def _auth_header(self):
        endpoint = self.config.get('auth', {}).get('endpoint')
        payload = self.config.get('auth', {}).get('payload')
        auth_url = self.url + endpoint
        res = requests.post(url=auth_url, json=payload, headers=self.header)
        token = res.json().get('access_token')
        self.header['Authorization'] = f'JWT {token}'

    def get_data(self, date):
        endpoint = self.config.get('api', {}).get('endpoint')
        payload = {'date': f'{date}'}
        data_url = self.url + endpoint

        res = requests.get(url=data_url, json=payload, headers=self.header)

        if res.status_code == 401:
            self._auth_header()
            res = requests.get(url=data_url, json=payload, headers=self.header)

        if res.status_code == 200:
            return res.json()
        else:
            if res.status_code == 401:
                raise HTTPException('authorization failed')
            elif res.status_code == 404:
                logger.info(f'no data available for date={date}')
                return {}
            else:
                raise HTTPException(f'unexpected error accessing {res.url}: {res.status_code}, {res.text}')
