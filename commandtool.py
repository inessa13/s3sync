# -*- coding: utf-8 -*-
import os
import logging
from optparse import OptionParser


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
        try:
            self._load_config_file(self.conf['config_file'])

            if options:
                if options.get('config_file'):
                    self._load_config_file(options['config_file'])
                self.conf.update(options)

            self._init_log()
            self._init_success = True

        except self.InitError, e:
            self.error(e.args[0])
            self._init_success = False

    @staticmethod
    def default_config():
        path = os.path.dirname(__file__)
        configuration = dict(
            bin_path=path,
            config_file=os.path.join(path, 'config.yaml'),
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
            import yaml
            with open(path, 'r') as f:
                l = yaml.load(f)
                if not l:
                    raise self.InitError('Config file is empty')
            self.conf.update(l)
            self.conf['config_file'] = path
            self.log("load config file: '{config_file}'")
        except self.Error:
            raise
        except ImportError:
            raise self.InitError('Missing yaml module')
        except Exception as e:
            raise self.InitError('Error on config load', e)

    def _init_log(self):
        self._logger = logging.getLogger(self.conf['log_name'])
        if not self._logger.handlers:
            if self.conf.get('log_file'):
                log_handler = logging.FileHandler(self.conf['log_file'], 'a', 'utf-8')
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
        """"""
        kwargs.update(self.conf)
        message = message.format(*args, **kwargs)
        if not hasattr(self, '_logger'):
            print message
        else:
            if self.conf.get('to_clear_command_line'):
                from sys import stdout
                stdout.write(' ' * get_terminal_size()[1] + '\r')
            self._logger._log(level, message, None)

    def info(self, message, *args, **kwargs):
        self.log(message, logging.INFO, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.log('! ' + message, logging.ERROR, *args, **kwargs)
        return False

    def confirm(self, promt, code, values=None, allow_remember=False):
        if self.conf.get('quiet'):
            return 'n'
        if allow_remember and code in self.conf['confirm_permanent']:
            return self.conf['confirm_permanent'][code]

        values = list(values)
        if 'n' not in values:
            values.append('n')

        values_str = u'/'.join(values) if values else '<answer>'
        if allow_remember:
            values_str += u' [all]'
        pr = u"{0} ({1})? ".format(promt, values_str)

        inp = ['']
        values = values or ['']
        while inp[0] not in values:
            inp = raw_input(pr.encode('cp1251'))
            inp = inp.split(' ')
        if allow_remember and len(inp) > 1 and inp[1] == 'all':
            self.conf['confirm_permanent'][code] = inp[0]
        return inp[0]

    def _del_speed(self):
        if 'speed' in self.conf:
            del self.conf['speed']
        self.conf['speed'] = []

    def _set_speed(self, t_before, size):
        from time import time
        _t = time() - t_before
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

        parser = OptionParser(usage=u"usage: %prog [options] arg1 [arg2...]")
        for o in self.options:
            o = list(o)
            p = o.pop()
            parser.add_option(*o, **p)
        self.opt, self.args = parser.parse_args()
        for k, v in self.opt.__dict__.iteritems():
            if v:
                self.conf[k] = v

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


def get_terminal_size(fd=1):
    """
    Returns height and width of current terminal. First tries to get
    size via termios.TIOCGWINSZ, then from environment. Defaults to 25
    lines x 80 columns if both methods fail.

    :param fd: file descriptor (default: 1=stdout)
    """
    try:
        import fcntl, termios, struct
        hw = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ, '1234'))
    except:
        try:
            hw = (os.environ['LINES'], os.environ['COLUMNS'])
        except:
            hw = (25, 80)

    return hw
