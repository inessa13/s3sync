# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import collections
import datetime
import logging
import logging.config
import os
import time

import boto.s3
import boto.s3.connection
import boto.s3.key
import reprint
import six
import yaml

from . import settings, tasks, utils

logger = logging.getLogger(__name__)


class BaseError(Exception):
    pass


class UserError(BaseError):
    pass


class S3SyncTool(object):
    def __init__(self):
        self.conn = None
        self.confirm_permanent = {}

        # load configs
        self.conf = {k.lower(): v for k, v in settings.__dict__.items()}
        self.load_config(settings.CONFIG_GLOBAL, update=True)
        self.load_config(settings.CONFIG_LOCAL, update=True)

    def load_config(self, path, update=False):
        if os.path.exists(path):
            with open(path, 'r') as config_file:
                loaded = yaml.load(config_file)
                if update:
                    self.conf.update(loaded)
                    return None
                return loaded

    def log(self, message, level=logging.INFO, *args, **kwargs):
        if '%s' in message:
            logger.log(level, message, *args, **kwargs)
        else:
            logger.log(level, message.format(*args, **kwargs))

    def info(self, message, *args, **kwargs):
        self.log(message, logging.INFO, *args, **kwargs)

    def debug(self, message, *args, **kwargs):
        self.log(message, logging.DEBUG, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.log('! ' + message, logging.ERROR, *args, **kwargs)
        return False

    def run_cli(self):
        parser = argparse.ArgumentParser()

        subparsers = parser.add_subparsers()

        cmd = subparsers.add_parser('config', help='show/edit config')
        cmd.set_defaults(func=self.on_config)
        cmd.add_argument(
            '--local',
            action='store_true',
            help='show/edit local config; by default global')
        cmd.add_argument(
            '--set', action='store', help='set config data')

        cmd = subparsers.add_parser('init', help='init project')
        cmd.set_defaults(func=self.on_init)
        cmd.add_argument(
            'bucket', action='store', help='bucket for sync')

        cmd = subparsers.add_parser('buckets', help='list buckets')
        cmd.set_defaults(func=self.on_list_buckets)

        cmd = subparsers.add_parser('list', help='list files')
        cmd.set_defaults(func=self.on_list)
        cmd.add_argument(
            '-b', '--bucket', action='store', help='bucket')
        cmd.add_argument(
            '-p', '--path',
            action='store', type=str, help='path to compare')
        cmd.add_argument(
            '-r', '--recursive', action='store_true', help='list recursive')
        cmd.add_argument(
            '-l', '--limit',
            action='store', default=10, type=int, help='output limit')

        cmd = subparsers.add_parser('diff', help='diff local and remote')
        cmd.set_defaults(func=self.on_diff)
        cmd.add_argument(
            '-f', '--file-types',
            action='store',
            help='file types (extension) for compare')
        cmd.add_argument(
            '-m', '--modes',
            action='store', default='-<>+r',
            help='modes of comparing (by default: -=<>+r)')
        cmd.add_argument(
            '-p', '--path',
            action='store', default='', help='path to compare')
        cmd.add_argument(
            '-r', '--recursive', action='store_true', help='list recursive')
        cmd.add_argument(
            '--skip-md5',
            action='store_true',
            help='skip file content comparing')

        cmd = subparsers.add_parser('rm', help='remove remote file')
        cmd.set_defaults(func=self.on_remove)
        cmd.add_argument('path', action='store', help='path to remove')

        cmd = subparsers.add_parser('upload', help='upload file')
        cmd.set_defaults(func=self.on_upload)
        cmd.add_argument('path', action='store', help='path to upload')

        cmd = subparsers.add_parser('update', help='update local or remote')
        cmd.set_defaults(func=self.on_update)
        cmd.add_argument(
            '-f', '--file-types',
            action='store',
            help='file types (extension) for compare')
        cmd.add_argument(
            '-m', '--modes',
            action='store', default='-<>+',
            help='modes of comparing (values: -=<>+)')
        cmd.add_argument(
            '-p', '--path',
            action='store', default='', help='path to compare')
        cmd.add_argument(
            '-q', '--quiet',
            action='store_true', help='quiet (no interactive)')
        cmd.add_argument(
            '-r', '--recursive', action='store_true', help='list recursive')
        cmd.add_argument(
            '--skip-md5',
            action='store_true',
            help='skip file content comparing')
        cmd.add_argument(
            '--confirm-upload',
            action='store_true', help='confirm upload action')
        cmd.add_argument(
            '--confirm-download',
            action='store_true', help='confirm download action')
        cmd.add_argument(
            '--confirm-replace-upload',
            action='store_true', help='confirm replace on upload')
        cmd.add_argument(
            '--confirm-replace-download',
            action='store_true', help='confirm replace on download')
        cmd.add_argument(
            '--confirm-delete-local',
            action='store_true', help='confirm delete local file')
        cmd.add_argument(
            '--confirm-delete-remote',
            action='store_true', help='confirm delete remote file')
        cmd.add_argument(
            '--confirm-rename-remote',
            action='store_true', help='confirm rename remote file')
        cmd.add_argument(
            '--force-upload',
            action='store_true',
            help='data transfer direction force change to upload')

        namespace = parser.parse_args()

        if getattr(namespace, 'func', None):
            return self.handler(namespace)

        parser.print_help()

    def handler(self, namespace):
        if not self.conf.get('access_key') or not self.conf.get('secret_key'):
            raise UserError('Missing access or secret key')

        self.debug('connecting s3...')
        # os.environ['S3_USE_SIGV4'] = 'True'
        self.conn = boto.s3.connection.S3Connection(
            self.conf.get('access_key'), self.conf.get('secret_key'))

        return namespace.func(namespace)

    def bucket(self, name=None):
        name = name or self.conf.get('bucket')
        if not name:
            return None

        for region in boto.s3.regions():
            if (self.conf.get('allowed_regions')
                    and region.name not in self.conf['allowed_regions']):
                continue
            conn = boto.s3.connection.S3Connection(
                self.conf.get('access_key'),
                self.conf.get('secret_key'),
                host=region.endpoint)
            if not conn:
                continue
            bucket = conn.lookup(name, validate=True)
            if bucket is not None:
                return bucket
        return None

    def local_root(self, stripped=False):
        local_root = None

        if self.conf.get('local_root'):
            local_root = self.conf['local_root']

            if local_root and stripped:
                local_root = local_root.replace('\\', '/')
                if local_root[-1] != '/':
                    local_root += '/'

        if not local_root:
            local_root = os.getcwd()

        return local_root

    def on_config(self, namespace):
        if namespace.local:
            config_path = settings.CONFIG_LOCAL
        else:
            config_path = settings.CONFIG_GLOBAL

        config = self.load_config(config_path) or {}

        if namespace.set:
            if '=' not in namespace.set:
                raise UserError('Invalid config option')
            key, value = namespace.set.split('=', 1)
            config[key.encode('utf8')] = value.encode('utf8')

        elif config:
            print(config)

        else:
            print('Config is empty')

        if config:
            if not namespace.local and not os.path.exists(
                    settings.CONFIG_DIR):
                os.makedirs(settings.CONFIG_DIR)

            with open(config_path, 'w') as config_file:
                yaml.dump(config, config_file, default_flow_style=False)

    @classmethod
    def on_init(cls, namespace):
        config = {'bucket'.encode('utf8'): namespace.bucket.encode('utf8')}
        with open(settings.CONFIG_LOCAL, 'w') as config_file:
            yaml.dump(config, config_file, default_flow_style=False)

    def on_list_buckets(self, namespace):
        self.info('listing buckets:')
        for bucket in self.conn.get_all_buckets():
            self.info(bucket.name)

    def on_list(self, namespace):
        bucket = utils.list_remote_dir(
            self.bucket(namespace.bucket),
            namespace.path,
            None,
            recursive=namespace.recursive)

        if bucket is False:
            raise UserError('Missing bucket')

        for index, key in enumerate(bucket):
            if index >= namespace.limit > 0:
                self.info('list limit reached!')
                break
            self._print_key(key)

    def on_diff(self, namespace, print_=True):
        local_root_s = self.local_root(stripped=True)

        src_path = os.path.join(self.local_root(), namespace.path)
        src_files = []
        if os.path.isdir(src_path):
            if namespace.recursive:
                for dir_path, __, file_names in os.walk(src_path):
                    for file_ in file_names:
                        file_path = os.path.join(dir_path, file_)
                        key = utils.file_key(
                            local_root_s, file_path, namespace.file_types)
                        if key and os.path.isfile(file_path):
                            src_files.append((key, file_path))
            else:
                for file_ in os.listdir(src_path):
                    file_path = os.path.join(src_path, file_)
                    key = utils.file_key(
                        local_root_s, file_path, namespace.file_types)
                    if key and os.path.isfile(file_path):
                        src_files.append((key, file_path))

        elif os.path.isfile(src_path):
            key = utils.file_key(
                local_root_s, src_path, namespace.file_types)
            if key:
                src_files.append((key, src_path))

        self.info('{0} local objects', len(src_files))

        bucket = self.bucket()
        if not bucket:
            raise UserError('missing bucket')

        remote_files = dict()
        ls_remote = utils.list_remote_dir(
            bucket, src_path, local_root_s, recursive=namespace.recursive)

        for file_ in ls_remote:
            if not isinstance(file_, boto.s3.key.Key) or file_.name[-1] == '/':
                continue
            if not utils.check_file_type(file_.name, namespace.file_types):
                continue

            remote_files[file_.name] = dict(
                key=file_,
                name=file_.name,
                size=file_.size,
                modified=file_.last_modified,
                md5=file_.etag[1:-1],
                state='-',
                comment=[],
            )

        self.info('{0} remote objects', len(remote_files.keys()))

        if not src_files and not remote_files:
            return

        self.info('comparing...')
        for key, f_path in src_files:
            stat = os.stat(f_path)

            if key in remote_files:
                equal = True
                if stat.st_size != remote_files[key]['size']:
                    equal = False
                    remote_files[key]['comment'].append('size: {0}%'.format(
                        round(float(
                            remote_files[key]['size']) / stat.st_size * 100, 2)
                    ))
                elif not namespace.skip_md5:
                    hash_ = utils.file_hash(f_path)
                    if hash_ != remote_files[key]['md5']:
                        equal = False
                        remote_files[key]['comment'].append('md5: different')

                if equal:
                    remote_files[key].update(state='=', comment=[])
                else:
                    remote_files[key]['local_size'] = stat.st_size
                    local_modified = datetime.datetime.fromtimestamp(
                        stat.st_ctime).replace(microsecond=0)
                    remote_modified = datetime.datetime.strptime(
                        remote_files[key]['modified'],
                        '%Y-%m-%dT%H:%M:%S.000Z')
                    remote_modified += datetime.timedelta(hours=4)

                    remote_files[key]['comment'].append(
                        'modified: {0}'.format(
                            local_modified - remote_modified))
                    if local_modified > remote_modified:
                        remote_files[key]['state'] = '>'
                    else:
                        remote_files[key]['state'] = '<'

                if remote_files[key]['state'] not in namespace.modes:
                    del remote_files[key]

            else:
                if ('+' not in namespace.modes
                        and 'r' not in namespace.modes):
                    continue

                remote_files[key] = dict(
                    local_size=stat.st_size,
                    modified=stat.st_mtime,
                    md5=utils.file_hash(f_path),
                    state='+',
                    comment=[],
                )

        # find renames
        if 'r' in namespace.modes:
            to_del = []
            for key, new_data in six.iteritems(remote_files):
                if new_data['state'] != '+':
                    continue
                for name, data in six.iteritems(remote_files):
                    if data['state'] != '-':
                        continue
                    if data['size'] != new_data['local_size']:
                        continue
                    if data['md5'] != new_data['md5']:
                        continue
                    remote_files[name].update(
                        state='r',
                        local_name=key,
                        local_size=new_data['local_size']
                    )
                    remote_files[name]['comment'].append(
                        'new: {0}'.format(key))
                    to_del.append(key)
                    break
            for key in to_del:
                del remote_files[key]

        if '-' not in namespace.modes or '+' not in namespace.modes:
            for key, value in remote_files.items():
                if value['state'] not in namespace.modes:
                    del remote_files[key]

        if print_:
            keys = remote_files.keys()
            keys.sort()
            for key in keys:
                self._print_diff_line(key, remote_files[key])
            self.info('{0} differences', len(remote_files.keys()))
        else:
            return remote_files

    def on_remove(self, namespace):
        bucket = self.bucket()
        if not bucket:
            raise UserError('Missing bucket')

        path = namespace.path.replace('\\', '/')

        if path[-1] == '/':
            raise UserError('Path is dir')

        ls = bucket.list(delimiter='/', prefix=path)
        ls = list(ls)

        if not ls:
            raise UserError('File not found')

        if len(ls) > 1:
            raise UserError('Multiple files found')

        remote_file = ls[0]

        if not isinstance(remote_file, boto.s3.key.Key):
            raise UserError('Try to remove dir')

        remote_file.delete()
        print('File successful deleted')

    def on_upload(self, namespace):
        bucket = self.bucket()
        if not bucket:
            raise UserError('Missing bucket')

        local_path = os.path.join(self.conf['local_root'], namespace.path)
        if not os.path.exists(local_path):
            raise UserError('Local path does not exists')

        local_root_s = self.local_root(stripped=True)
        key = utils.file_key(local_root_s, namespace.path)

        ls = bucket.list(delimiter='/', prefix=key)
        ls = list(ls)

        if ls:
            raise UserError('Remote path exists')

        stat = os.stat(local_path)
        data = {'local_size': stat.st_size}

        task = tasks.Upload()
        task(bucket, None, self.conf, key, data, None)
        if data['comment']:
            print(data['comment'][0])
        else:
            print('File successful uploaded')

    def on_update(self, namespace):
        files = self.on_diff(namespace, print_=False)
        if not files:
            self.error('no changes')
            return

        self.info('processing...')

        _t = time.time()
        processed, _size = 0, 0

        try:
            processed, _size = self._update(files, namespace)
        finally:
            if _t:
                speed = utils.humanize_size(_size / _t)
                self.info('average speed: %s', speed)

            self.info(
                '{0} actions processed, {1} skipped',
                processed, len(files.keys()) - processed
            )

    def _update(self, files, namespace):
        processed = 0
        _size = 0

        pool = tasks.ThreadPool(settings.THREAD_MAX_COUNT)
        output_manager = reprint.output(
            output_type="list",
            initial_len=settings.THREAD_MAX_COUNT,
            interval=0)
        output = output_manager.__enter__()

        for name, data in six.iteritems(files):
            if data['state'] == '=':
                processed += 1
                continue

            elif data['state'] == '+':
                if namespace.confirm_upload:
                    action = tasks.Upload()
                elif namespace.confirm_delete_local:
                    action = tasks.DeleteLocal()
                elif namespace.quiet:
                    continue
                else:
                    act = self._confirm_update(
                        name, data,
                        tasks.Upload(), tasks.DeleteLocal())
                    if act == 'n':
                        continue
                    else:
                        action = act

            elif data['state'] == '-':
                if namespace.confirm_download:
                    action = tasks.Download()
                elif namespace.confirm_delete_remote:
                    action = tasks.DeleteRemote()
                elif namespace.quiet:
                    continue
                else:
                    act = self._confirm_update(
                        name, data,
                        tasks.Download(), tasks.DeleteRemote())

                    if act == 'n':
                        continue
                    action = act

            elif data['state'] == '>' or namespace.force_upload:
                data['state'] = '>'
                if self._check(
                        name, data, namespace.quiet,
                        namespace.confirm_replace_upload):
                    action = tasks.ReplaceUpload()
                continue

            elif data['state'] == '<':
                if self._check(
                        name, data, namespace.quiet,
                        namespace.confirm_replace_download):
                    action = tasks.Download()
                continue

            elif data['state'] == 'r':
                if self._check(
                        name, data, namespace.quiet,
                        namespace.confirm_rename_remote):
                    action = tasks.RenameRemote()
                continue

            _size += data.get('local_size', 0)

            pool.add_task(
                action,
                self.bucket(),
                None,
                self.conf,
                name,
                data,
                output,
            )

        pool.join()
        output_manager.__exit__(None, None, None)

        return processed, _size

    def _check(self, name, data, quiet, confirm):
        if confirm:
            return True
        if quiet:
            return False

        return self._confirm_update(name, data, 'y') == 'y'

    def _confirm_update(self, name, data, *values):
        assert values

        code = data['state']
        if code in self.confirm_permanent:
            return self.confirm_permanent[code]

        values_map = collections.OrderedDict(
            (str(value), value) for value in values)

        if 'n' not in values_map:
            values_map['n'] = 'n'

        prompt_str = '{} {} {} ({} [all])? '.format(
            code, name,
            ', '.join(data.get('comment', [])),
            '/'.join(six.iterkeys(values_map)),
        )

        input_data = []
        while not input_data or input_data[0] not in values_map:
            input_data = six.moves.input(prompt_str.encode('utf8'))
            input_data = input_data.split(' ', 1)

        if len(input_data) > 1 and input_data[1] == 'all':
            self.confirm_permanent[code] = input_data[0]

        return values_map[input_data[0]]

    def _print_key(self, key):
        name_len = self.conf.get(
            'key_pattern_name_len') or settings.KEY_PATTERN_NAME_LEN

        if len(key.name) < name_len:
            name = key.name.ljust(name_len, ' ')
        else:
            name = key.name[:self.conf['key_pat_name_len'] - 3] + '...'

        if isinstance(key, boto.s3.key.Key):
            params = {
                'name': name,
                'size': str(key.size).ljust(10, ' '.encode('ascii')),
                'owner': key.owner.display_name,
                'modified': key.last_modified,
                'storage': settings.STORAGE_ALIASES.get(key.storage_class, '?'),
                'md5': key.etag[1:-1],
            }
        else:
            params = {
                'name': name,
                'size': '<DIR>'.ljust(10, ' '),
                'owner': '',
                'modified': '',
                'storage': '?',
                'md5': ''
            }

        pattern = self.conf.get('key_pattern') or settings.KEY_PATTERN
        self.info(pattern.format(**params))

    def _print_diff_line(self, name, data):
        self.info(
            '{0} {1} {2}',
            data['state'],
            name,
            ', '.join(data.get('comment', []))
        )


def main():
    tool = S3SyncTool()

    if settings.LOGGING:
        logging.config.dictConfig(settings.LOGGING)

    try:
        tool.run_cli()
    except UserError as exc:
        tool.error(exc.args[0])
    except KeyboardInterrupt:
        tool.error('interrupted')


if __name__ == '__main__':
    main()
