# -*- coding: utf-8 -*-
import os

BIN_PATH = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(os.getcwd(), '.s3sync')
KEY_PAT = '{name} {storage} {size} {modified} {owner} {md5}'
KEY_PAT_NAME_LEN = 60
LIST_LIMIT = 20
MODES = '-=<>+'
CONFIRM_PERMANENT = dict()
COMPARE_HASH = True
THREAD_MAX_COUNT = 24
UPLOAD_CB_NUM = 5
# UPLOAD_FORMAT = '{speed}\r'
UPLOAD_FORMAT = '[{progress}>{left}] {progress_percent}% {speed}\r'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': '[%(asctime)s] %(levelname).1s %(message)s',
            'datefmt': '%Y%m%d %H%M%S',
        },
        'stream': {
            'format': '%(message)s',
            'datefmt': '%Y%m%d %H%M%S',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'console',
        },
        'stream': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'stream',
        },
    },
    'loggers': {
        '': {
            'handlers': ['stream'],
            'propagate': False,
            'level': 'INFO',
        },
    }
}
