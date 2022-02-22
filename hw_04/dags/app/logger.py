import logging
import colorlog

from constants import APP_NAME

colorlog.basicConfig(level=logging.INFO)
logger = colorlog.getLogger(APP_NAME)
