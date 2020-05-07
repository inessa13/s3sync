# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import hashlib
import os
import re
import time

import six

from . import errors, settings


def file_hash(f_path):
    file_ = open(f_path, 'rb')
    hash_ = hashlib.md5()
    while True:
        block = file_.read(128)
        if not block:
            break
        hash_.update(block)
    file_.close()
    return hash_.hexdigest()


def file_path_info(path):
    project_root = find_project_root() or get_cwd()
    current_root = get_cwd()

    if not path or path == '.':
        path = current_root

    if os.path.isabs(path):
        if path == project_root:
            key = ''
            path = project_root.replace('\\', '/')
        else:
            path = path.replace('\\', '/')
            key = re.sub(
                '^{}/'.format(project_root.replace('\\', '/')), '', path)

    elif project_root == current_root:
        key = path.replace('\\', '/')
        path = os.path.join(project_root, path).replace('\\', '/')

    else:
        path = os.path.join(current_root, path).replace('\\', '/')
        key = re.sub(
            '^{}/'.format(project_root.replace('\\', '/')), '', path)

    try:
        key = key.decode('utf8')
    except UnicodeEncodeError:
        try:
            key = key.decode('cp1251')
        except UnicodeEncodeError:
            key = key

    # TODO: fix for windows
    path = '/' + os.path.join(*path.split('/'))
    return path, key


def file_key(path):
    return file_path_info(path)[1]


def file_path(path):
    return file_path_info(path)[0]


def iter_local_path(path, recursive=False):
    path = file_path(path)
    if os.path.isdir(path):
        if recursive:
            for dir_path, __, file_names in os.walk(path):
                for file_ in file_names:
                    yield os.path.join(dir_path, file_)
        else:
            for file_ in os.listdir(path):
                yield os.path.join(path, file_)

    elif os.path.isfile(path):
        yield path

    else:
        raise errors.UserError('Invalid path {}'.format(path))


def iter_remote_path(bucket, path, recursive=False):
    assert bucket

    local_path, key = file_path_info(path)
    if key and os.path.isdir(local_path) and key[-1] != '/':
        key += '/'

    params = dict()
    if not recursive:
        params['delimiter'] = '/'

    if key:
        params['prefix'] = key.replace('\\', '/')

    return bucket.list(**params)


def humanize_size(value, multiplier=1024, label='Bps'):
    if value > multiplier ** 4:
        value /= multiplier ** 4
        label = 'T' + label
    elif value > multiplier ** 3:
        value /= multiplier ** 3
        label = 'G' + label
    elif value > multiplier ** 2:
        value /= multiplier ** 2
        label = 'M' + label
    elif value > multiplier:
        value /= multiplier
        label = 'K' + label
    else:
        label = ' ' + label

    return '{:7.2f} {}'.format(value, label)


def check_file_type(filename, types):
    if not types:
        return True

    filename = filename.lower()

    file_types = types.lower().split(',')
    if file_types[0][0] == '^':
        exclude = True
        file_types[0] = file_types[0][1:]
    else:
        exclude = False
    if exclude and filename.split('.')[-1] in file_types:
        return False
    if not exclude and filename.split('.')[-1] not in file_types:
        return False
    return True


def memoize(func):
    memo = {}

    def wrapper(*args, **kwargs):
        memo_key = ''

        if args:
            memo_key += ','.join(map(str, args))
        if kwargs:
            memo_key += ','.join(
                '{}:{}'.format(k, v) for k, v in six.iteritems(kwargs))

        if memo_key not in memo:
            memo[memo_key] = func(*args, **kwargs)

        return memo[memo_key]

    return wrapper


@memoize
def find_project_root():
    root = get_cwd()
    while root:
        path = os.path.join(root, settings.CONFIG_LOCAL_NAME)
        if os.path.exists(path):
            return root

        # TODO: fix for windows
        if root == '/':
            return None

        root = os.path.dirname(root)
    return None


@memoize
def get_cwd():
    return os.getcwd()


class Timeit(object):
    def __init__(self, func=None):
        self.func = func
        self._t = None

    def __enter__(self):
        self._t = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('{:.2f}'.format(time.time() - self._t))

    def __call__(self, *args, **kwargs):
        _t = time.time()
        self.func(*args, **kwargs)
        print('{} {:.2f}'.format(
            self.func.__name__, time.time() - _t))


class Formatter(argparse.HelpFormatter):
    def __init__(
            self, prog, indent_increment=2, max_help_position=30, width=None):
        super(Formatter, self).__init__(
            prog, indent_increment, max_help_position, width)

    def _format_action_invocation(self, action):
        if not action.option_strings:
            metavar, = self._metavar_formatter(action, action.dest)(1)
            return metavar

        parts = []
        # if the Optional doesn't take a value, format is:
        #    -s, --long
        if action.nargs == 0:
            parts.extend(action.option_strings)

        # if the Optional takes a value, format is:
        #    -s, --long ARGS
        else:
            default = action.dest.upper()
            args_string = self._format_args(action, default)
            for option_string in action.option_strings:
                parts.append(option_string)
            parts[-1] += ' %s' % args_string

        return ', '.join(parts)
