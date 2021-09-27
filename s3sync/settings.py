import os

BIN_PATH = os.path.dirname(__file__)

CONFIG_DIR = os.path.expanduser('~/.config/s3sync/')
CONFIG_GLOBAL = os.path.join(CONFIG_DIR, 'config.yml')
CONFIG_LOCAL_NAME = '.s3sync'
KEY_PATTERN = '{name} {storage} {size} {modified} {owner} {md5}'
KEY_PATTERN_NAME_LEN = 60
THREAD_MAX_COUNT = 16
ENDED_OUTPUT_MAX_COUNT = 4
UPLOAD_CB_NUM = 10
UPLOAD_FORMAT = '[{progress}>{left}] {progress_percent:3.0f}% {speed} {info}'
REDUCED_REDUNDANCY = False

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
