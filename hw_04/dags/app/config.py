import os

import yaml

from constants import CONFIG_FILE_NAME


class Config:
    config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILE_NAME)

    def __init__(self):
        with open(self.config_path, 'r') as f:
            self.__config = yaml.safe_load(f)

    def get_config(self, application):
        return self.__config.get(application)
