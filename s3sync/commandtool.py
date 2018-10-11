# -*- coding: utf-8 -*-
import logging
import argparse
import os
import sys
import time

import yaml

from . import utils


class CommandTool(object):
    options = [
        ("--config-file", dict(
            action="store",
            dest="config_file",
            metavar="FILE",
            help="load external config file")),
        ("-q", "--quiet", dict(
            action="store_true",
            dest="quiet",
            help="quiet (no interactive)")),
    ]

    def __init__(self, **options):
        self.conf = self.default_config()
        self.opt, self.args = None, None

        try:
            if options:
                self.conf.update(options)
            self._init_log()
            self._init_success = True

        except self.InitError as exc:
            self.error(exc.args[0])
            self._init_success = False

    @staticmethod
    def default_config():
        path = os.path.dirname(__file__)
        configuration = dict(
            bin_path=path,
            config_file=os.path.join(os.getcwd(), '.s3sync'),
            log_file=False,
            log_name=__name__,
            log_level=0,
            log_fmt='%(asctime)s %(levelname)s %(message)s',
            log_date_fmt='%Y%m%d %H%M%S',
            log_stream_fmt='%(message)s',
        )
        return configuration

    def _load_config_file(self, path):
        """load config from external file"""
        if not os.path.exists(path):
            self.conf['config_file'] = None
            return

        try:
            with open(path, 'r') as file_:
                _loaded = yaml.load(file_)
                if not _loaded:
                    raise self.InitError('Config file is empty')
            self.conf.update(_loaded)
            self.conf['config_file'] = path
            self.log("load config file: '{config_file}'")
        except self.Error:
            raise
        except ImportError:
            raise self.InitError('Missing yaml module')
        except Exception as exc:
            raise self.InitError('Error on config load', exc)

    def _init_log(self):
        self._logger = logging.getLogger(self.conf['log_name'])
        if not self._logger.handlers:
            if self.conf.get('log_file'):
                log_handler = logging.FileHandler(
                    self.conf['log_file'], 'a', 'utf-8')
                log_handler.setFormatter(logging.Formatter(
                    fmt=self.conf['log_fmt'],
                    datefmt=self.conf['log_date_fmt']))
            else:
                log_handler = logging.StreamHandler()
                log_handler.setFormatter(logging.Formatter(
                    fmt=self.conf['log_stream_fmt']))
            log_handler.setLevel(self.conf['log_level'])
            self._logger.addHandler(log_handler)

    def log(self, message, level=logging.INFO, *args, **kwargs):
        kwargs.update(self.conf)
        message = message.format(*args, **kwargs)
        if not hasattr(self, '_logger'):
            print message
        else:
            if self.conf.get('to_clear_command_line'):
                sys.stdout.write(' ' * utils.get_terminal_size()[1] + '\r')
            self._logger._log(level, message, None)

    def info(self, message, *args, **kwargs):
        self.log(message, logging.INFO, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.log('! ' + message, logging.ERROR, *args, **kwargs)
        return False

    def _del_speed(self):
        if 'speed' in self.conf:
            del self.conf['speed']
        self.conf['speed'] = []

    def _set_speed(self, t_before, size):
        _t = time.time() - t_before
        _t = (float(size) / _t) if _t else 0
        if not self.conf.get('speed'):
            self.conf['speed'] = []
        self.conf['speed'].append(_t)

    def _get_speed(self):
        if not self.conf.get('speed'):
            return 'n\\a'
        info = 'Bps'
        speed = float(sum(self.conf['speed'])) / len(self.conf['speed'])
        if speed > 1024 ** 3:
            speed /= 1024 ** 3
            info = "GBps"
        elif speed > 1024 ** 2:
            speed /= 1024 ** 2
            info = "MBps"
        elif speed > 1024:
            speed /= 1024
            info = "KBps"
        return "{0} {1}".format(round(speed, 2), info)

    def run(self):
        if not self._init_success:
            return

        self.handler()

    def run_cli(self):
        if not self._init_success:
            return

        parser = optparse.OptionParser(
            usage=u"usage: %prog [options] arg1 [arg2...]")
        for opt in self.options:
            opt = list(opt)
            opt_kw = opt.pop()
            parser.add_option(*opt, **opt_kw)
        self.opt, self.args = parser.parse_args()
        for key, value in self.opt.__dict__.iteritems():
            if value:
                self.conf[key] = value

        if self.conf.get('config_file'):
            self._load_config_file(self.conf['config_file'])

        self.handler(cli=True)

    def handler(self, cli=False):
        if cli:
            if not self.args:
                if hasattr(self, 'on_default'):
                    getattr(self, 'on_default')()
                else:
                    self.error("missing argument. type --help for usage")
            else:
                if hasattr(self, 'on_{0}'.format(self.args[0])):
                    getattr(self, 'on_{0}'.format(self.args[0]))()
                else:
                    self.error("invalid argument. type --help for usage")
            return

    class Error(Exception):
        pass

    class InitError(Error):
        pass
