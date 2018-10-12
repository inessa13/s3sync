# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import datetime
import logging
import logging.config
import os
import sys
import threading
import time

import boto.s3
import boto.s3.connection
import boto.s3.key
import yaml
import six

from . import settings, utils

logger = logging.getLogger(__name__)


class BaseError(Exception):
    pass


class UserError(BaseError):
    pass


class UploadThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(UploadThread, self).__init__(*args, **kwargs)
        self.result = 0

    def run(self):
        try:
            self.run()
        except KeyboardInterrupt:
            pass
        except:
            logging.exception('thread failed!')


CLR_END = '\033[0m'


def error(message):
    CLR_FAIL = '\033[91m'
    print(CLR_FAIL + message + CLR_END)


def warn(message):
    CLR_WARNING = '\033[93m'
    print(CLR_WARNING + message + CLR_END)


def success(message):
    CLR_OKGREEN = '\033[92m'
    print(CLR_OKGREEN + message + CLR_END)


class S3SyncTool(object):
    def __init__(self):
        self.conn = None
        self.conf = {k.lower(): v for k, v in settings.__dict__.items()}

    def _load_config_file(self, path):
        if not os.path.exists(path):
            return

        try:
            with open(path, 'r') as file_:
                _loaded = yaml.load(file_)
                if not _loaded:
                    raise UserError('Config file is empty')
            self.conf.update(_loaded)
            self.info("load config file: '%s'", path)
        except BaseError:
            raise
        except ImportError:
            raise UserError('Missing yaml module')
        except Exception as exc:
            raise UserError('Error on config load', exc)

    def log(self, message, level=logging.INFO, *args, **kwargs):
        if self.conf.get('to_clear_command_line'):
            sys.stdout.write(' ' * utils.get_terminal_size()[1] + '\r')
            sys.stdout.write(message.format(*args, **kwargs))
        if '%s' in message:
            logger.log(level, message, *args, **kwargs)
        else:
            logger.log(level, message.format(*args, **kwargs))

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

    def run_cli(self):
        self._load_config_file(os.path.join(os.getcwd(), '.s3sync'))

        if not self.conf.get('access_key') or not self.conf.get('secret_key'):
            return self.error('missing access or secret key')

        self.info('connecting s3...')
        # os.environ['S3_USE_SIGV4'] = 'True'
        self.conn = boto.s3.connection.S3Connection(
            self.conf.get('access_key'), self.conf.get('secret_key'))

        parser = argparse.ArgumentParser()

        # TODO: init
        subparsers = parser.add_subparsers()
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
            action='store', default='-<>+',
            help='modes of comparing (by default: -=<>+)')
        cmd.add_argument(
            '-p', '--path',
            action='store', default='', help='path to compare')
        cmd.add_argument(
            '-r', '--recursive', action='store_true', help='list recursive')
        cmd.add_argument(
            '--skip-md5',
            action='store_true',
            help='skip file content comparing')

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
            try:
                return namespace.func(namespace)
            except KeyboardInterrupt:
                return error('Interrupted')
            except Exception as exc:
                logging.exception('!!!')
                return error(exc.args[0])

        parser.print_help()

    def bucket(self, name=None):
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
            bucket = conn.lookup(name or self.conf['bucket'], validate=True)
            if bucket is not None:
                return bucket
        return None

    def confirm(
            self, promt, code, quiet=False, values=None, allow_remember=False):
        if quiet:
            return 'n'
        if allow_remember and code in self.conf['confirm_permanent']:
            return self.conf['confirm_permanent'][code]

        values = list(values)
        if 'n' not in values:
            values.append('n')

        values_str = u'/'.join(values) if values else '<answer>'
        if allow_remember:
            values_str += u' [all]'
        promt_str = u"{0} ({1})? ".format(promt, values_str).encode('cp1251')

        inp = ['']
        values = values or ['']
        while inp[0] not in values:
            inp = six.moves.input(promt_str)
            inp = inp.split(' ')
        if allow_remember and len(inp) > 1 and inp[1] == 'all':
            self.conf['confirm_permanent'][code] = inp[0]
        return inp[0]

    def on_list_buckets(self, namespace):
        self.info('listing buckets:')
        for bucket in self.conn.get_all_buckets():
            self.log(bucket.name)

    def on_list(self, namespace):
        bucket = utils.list_remote_dir(
            self.bucket(namespace.bucket),
            namespace.path,
            recursive=namespace.recursive)

        if bucket is False:
            return self.error('missing bucket')

        for index, key in bucket:
            if index >= namespace.limit > 0:
                self.info('list limit reached!')
                break
            self._print_key(key)

    def on_diff(self, namespace, print_=True):
        if not self.conf.get('local_root'):
            return self.error('missing local root directory reference')
        else:
            # ета переменная нужна, чтобы убирая из полного пути до
            #  локального файла получить путь до удаленного файла с корня
            #  (с букета)
            local_root_s = self.conf['local_root'].replace('\\', '/')
            if local_root_s[-1] != '/':
                local_root_s += '/'

        src_path = os.path.join(self.conf['local_root'], namespace.path)
        src_files = []
        if namespace.recursive:
            for dir_path, __, file_names in os.walk(src_path):
                for file_ in file_names:
                    if not self._check_file_type(file_, namespace.file_types):
                        continue
                    key = f_path = os.path.join(dir_path, file_)

                    try:
                        key = key.decode('utf8')
                    except UnicodeEncodeError:
                        try:
                            key = key.decode('cp1251')
                        except UnicodeDecodeError:
                            key = key

                    key = key.replace('\\', '/').replace(
                        local_root_s, '').lower()
                    src_files.append((key, f_path))
        else:
            for file_ in os.listdir(src_path):
                if not self._check_file_type(file_, namespace.file_types):
                    continue
                f_path = os.path.join(src_path, file_)
                if not os.path.isfile(f_path):
                    continue

                key = os.path.join(namespace.path, file_)

                try:
                    key = key.decode('utf8')
                except UnicodeEncodeError:
                    try:
                        key = key.decode('cp1251')
                    except UnicodeEncodeError:
                        key = key

                key = key.replace('\\', '/').lower()
                src_files.append((key, f_path))

        self.info('{0} local objects', len(src_files))

        remote_files = dict()
        ls_remote = utils.list_remote_dir(
            self.bucket(), namespace.path, recursive=namespace.recursive)
        if ls_remote is False:
            return self.error('missing bucket')

        for __, file_ in ls_remote:
            if not isinstance(file_, boto.s3.key.Key) or file_.name[-1] == '/':
                continue
            if not self._check_file_type(file_.name, namespace.file_types):
                continue

            remote_files[file_.name.lower()] = dict(
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

    def on_update(self, namespace):
        files = self.on_diff(namespace, print_=False)
        self.info('processing...')
        processed = 0
        threads = []
        _t = time.time()
        _size = 0

        try:
            for name, data in six.iteritems(files):
                if data['state'] == '=':
                    processed += 1
                    continue
                elif data['state'] == '+':
                    if namespace.confirm_upload:
                        data['action'] = 'upload'
                    elif namespace.confirm_delete_local:
                        data['action'] = 'delete_local'
                    else:
                        act = self._confirm_update(
                            name, data, namespace.quiet,
                            'upload', 'delete_local')
                        if act == 'n':
                            continue
                        else:
                            data['action'] = act
                elif data['state'] == '-':
                    if namespace.confirm_download:
                        data['action'] = 'download'
                    elif namespace.confirm_delete_remote:
                        data['action'] = 'delete_remote'
                    else:
                        act = self._confirm_update(
                            name, data, namespace.quiet,
                            'download', 'delete_remote')
                        if act == 'n':
                            continue
                        else:
                            data['action'] = act
                elif data['state'] == '>' or namespace.force_upload:
                    data['state'] = '>'
                    if (not namespace.confirm_replace_upload
                            and self._confirm_update(
                                name, data, namespace.quiet, 'y') == 'n'):
                        continue
                    data['action'] = 'replace_upload'
                elif data['state'] == '<':
                    if (not namespace.confirm_replace_download
                            and self._confirm_update(
                                name, data, namespace.quiet, 'y') == 'n'):
                        continue
                    data['action'] = 'replace_download'
                elif data['state'] == 'r':
                    if (not namespace.confirm_rename_remote
                            and self._confirm_update(
                                name, data, namespace.quiet, 'y') == 'n'):
                        continue
                    data['action'] = 'rename_remote'

                _size += data.get('local_size', 0)

                if self.conf['thread_max_count'] > 1:
                    threads = [thr for thr in threads if thr.isAlive()]
                    while len(threads) >= self.conf['thread_max_count']:
                        thr_last = threads.pop()
                        thr_last.join()
                    thr = UploadThread(
                        target=self._update_process, args=(name, data))
                    threads.append(thr)
                    thr.start()
                else:
                    processed += self._update_process(name, data)

            for thr in threads:
                thr.join()

        except KeyboardInterrupt:
            print('interrupted')

        finally:
            self._del_speed()
            self._set_speed(_t, _size)
            self.info('average speed: {0}', self._get_speed())
            self.info(
                '{0} actions processed, {1} skipped',
                processed, len(files.keys()) - processed
            )

    def _update_process(self, name, data):
        try:
            getattr(self, '_update_{0}'.format(data['action']))(name, data)
            self._print_diff_line(name, data)
            return 1
        except (AttributeError, NotImplementedError):
            self.error('not developed yet')
            return 0
        except Exception:
            self.error('file {0} update failed', name)
            raise

    def _update_replace_upload(self, name, data):
        with open(os.path.join(self.conf['local_root'], name), 'rb') as file_:
            _t = time.time()
            data['key'].set_contents_from_file(
                file_, headers=None, replace=True,
                cb=self._upload_cb, num_cb=self.conf['upload_cb_num'],
                policy=None, md5=None,
                reduced_redundancy=True, query_args=None,
                encrypt_key=False, size=None, rewind=True)
            if data.get('local_size'):
                self._set_speed(_t, data['local_size'])

        data['comment'] = ['uploaded(replaced)']

    def _update_upload(self, name, data):
        bucket = self.bucket()
        key = boto.s3.key.Key(bucket=bucket, name=name)
        with open(os.path.join(self.conf['local_root'], name), 'rb') as file_:
            _t = time.time()
            key.set_contents_from_file(
                file_, replace=True,
                cb=self._upload_cb, num_cb=self.conf['upload_cb_num'],
                policy=None, md5=None,
                reduced_redundancy=True, query_args=None,
                encrypt_key=False, rewind=True)
            if data.get('local_size'):
                self._set_speed(_t, data['local_size'])
        data['comment'] = ['uploaded']

    def _update_delete_remote(self, __, data):
        data['key'].delete()
        data['comment'] = ['deleted from s3']

    def _update_rename_remote(self, __, data):
        new_key = data['key'].copy(
            self.conf['bucket'], data['local_name'],
            metadata=None,
            reduced_redundancy=True, preserve_acl=True,
            encrypt_key=False, validate_dst_bucket=True)
        if new_key:
            data['key'].delete()
            data['comment'] = ['renamed']
        else:
            raise Exception('s3 key copy failed')

    def _update_replace_download(self, name, data):
        data['key'].get_contents_to_filename(
            os.path.join(self.conf['local_root'], name), headers=None,
            cb=self._action_cb, num_cb=20, torrent=False, version_id=None,
            res_download_handler=None, response_headers=None)

    def _update_download(self, name, data):
        data['key'].get_contents_to_filename(
            os.path.join(self.conf['local_root'], name), headers=None,
            cb=self._action_cb, num_cb=20, torrent=False, version_id=None,
            res_download_handler=None, response_headers=None)

    def _check_file_type(self, filename, types):
        filename = filename.lower()
        if not types:
            return True

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

    def _upload_cb(self, uploaded, full):
        len_full = 40
        progress = round(float(uploaded) / full, 2) * 100
        len_pr = int(progress) * len_full / 100
        sys.stdout.write(self.conf['upload_format'].format(
            progress='=' * len_pr,
            left=' ' * (len_full - len_pr),
            progress_percent=progress,
            speed=self._get_speed()))
        self.conf['to_clear_command_line'] = True

    def _action_cb(self, uploaded, full):
        pr_line = '|/-\\'
        if '_action_progress' not in self.conf:
            self.conf['_action_progress'] = 0
        else:
            self.conf['_action_progress'] += 1
        sys.stdout.write('{0}\r'.format(
            pr_line[self.conf['_action_progress'] % len(pr_line)]))
        self.conf['to_clear_command_line'] = True

    def _confirm_update(self, name, data, quiet, *values):
        return self.confirm(
            '{0} {1} {2}'.format(
                data['state'], name, ', '.join(data.get('comment', []))),
            data['state'],
            quiet=quiet,
            values=values,
            allow_remember=True)

    def _print_key(self, key):
        storage = dict(
            GLACIER='G',
            STANDARD='S',
            REDUCED_REDUNDANCY='R',
        )
        name = \
            key.name.ljust(self.conf['key_pat_name_len'], ' ') \
            if len(key.name) < self.conf['key_pat_name_len'] \
            else key.name[:self.conf['key_pat_name_len'] - 3] + '...'
        if isinstance(key, boto.s3.key.Key):
            params = dict(
                name=name,
                size=str(key.size).ljust(10, ' '.encode('ascii')),
                owner=key.owner.display_name,
                modified=key.last_modified,
                storage=storage.get(key.storage_class, '?'),
                md5=key.etag[1:-1],
            )
        else:
            params = dict(
                name=name,
                size='<DIR>'.ljust(10, ' '),
                owner='',
                modified='',
                storage='?',
                md5=''
            )

        self.log(six.text_type(self.conf['key_pat']).format(**params))

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
        tool.log('interrupted')


if __name__ == '__main__':
    main()
