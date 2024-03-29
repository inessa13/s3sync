# PYTHON_ARGCOMPLETE_OK
import argparse
import collections
import datetime
import logging
import logging.config
import os
import time

import argcomplete
import boto.s3
import boto.s3.connection
import boto.s3.key
import reprint
import yaml

from . import __version__, constants, errors, settings, tasks, utils

logger = logging.getLogger(__name__)


class S3SyncTool:
    def __init__(self):
        self.conn = None
        self.confirm_permanent = {}

        # load configs
        self.conf = {
            k.upper(): v
            for k, v in settings.__dict__.items()
            if not k.startswith('_') and isinstance(v, (int, str, dict))
        }
        self.load_config(settings.CONFIG_GLOBAL, update=True)

        project_root = utils.find_project_root()
        if project_root:
            self.conf['PROJECT_ROOT'] = project_root
            self.conf['LOCAL_CONFIG'] = project_config = os.path.join(
                project_root, settings.CONFIG_LOCAL_NAME)
            self.load_config(project_config, update=True)

    def load_config(self, path, update=False):
        if not path or not os.path.exists(path):
            return None

        with open(path, 'r') as config_file:
            loaded = yaml.safe_load(config_file)
            loaded = {k.upper(): v for k, v in loaded.items()}
            if update:
                self.conf.update(loaded)
            return loaded

    @classmethod
    def log(cls, message, level, *args, **kwargs):
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
        parser.add_argument(
            '-V', '--version',
            action='version',
            version='%(prog)s ' + __version__,
            help='show version and exit')

        subparsers = parser.add_subparsers(title='list of commands')

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

        cmd = subparsers.add_parser(
            'list',
            formatter_class=utils.Formatter,
            help='list files')
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

        common_diff = argparse.ArgumentParser(add_help=False)
        common_diff.add_argument(
            '-a', '--all',
            action='store_true', help='use all modes. ignores -m')
        common_diff.add_argument(
            '-b', '--brief', action='store_true', help='brief diff')
        common_diff.add_argument(
            '-i', '--ignore-case',
            action='store_true', help='ignore file path case')
        common_diff.add_argument(
            '-r', '--recursive', action='store_true', help='list recursive')
        common_diff.add_argument(
            '-5', '--md5', action='store_true', help='compare file content')

        common_diff.add_argument(
            '--force-upload',
            action='store_true',
            help='data transfer direction force change to upload')
        common_diff.add_argument(
            '--force-download',
            action='store_true',
            help='data transfer direction force change to download')

        common_diff.add_argument(
            '-p', '--path',
            action='store', default='', help='path to compare')
        common_diff.add_argument(
            '-m', '--modes',
            action='store', default='-<>+r',
            help='modes of comparing (by default: -<>+r)')
        common_diff.add_argument(
            '-f', '--file-types',
            action='store',
            metavar='TYPES',
            help='file types (extension) for compare')

        cmd = subparsers.add_parser(
            'diff',
            parents=[common_diff],
                formatter_class=utils.Formatter,
            help='diff local and remote')
        cmd.set_defaults(func=self.on_diff)

        cmd = subparsers.add_parser('rm', help='remove remote file')
        cmd.set_defaults(func=self.on_remove)
        cmd.add_argument('path', action='store', help='path to remove')

        cmd = subparsers.add_parser('upload', help='upload file')
        cmd.set_defaults(func=self.on_upload)
        cmd.add_argument('path', action='store', help='path to upload')
        cmd.add_argument(
            '-f', '--force', action='store_true', help='force upload')
        cmd.add_argument(
            '-r', '--recursive', action='store_true', help='list recursive')

        cmd = subparsers.add_parser(
            'update',
            parents=[common_diff],
            formatter_class=utils.Formatter,
            help='update local or remote')
        cmd.set_defaults(func=self.on_update)
        cmd.add_argument(
            '-l', '--limit',
            action='store',
            default=0,
            metavar='L',
            type=int,
            help='process limit')
        cmd.add_argument(
            '-q', '--quiet',
            action='store_true', help='quiet (no interactive)')
        cmd.add_argument(
            '-U', '--upload',
            action='store_true', help='confirm upload action')
        cmd.add_argument(
            '-D', '--download',
            action='store_true', help='confirm download action')
        cmd.add_argument(
            '-R', '--rename-remote',
            action='store_true', help='confirm rename remote file')
        cmd.add_argument(
            '-L', '--rename-local',
            action='store_true', help='confirm rename local file')
        cmd.add_argument(
            '--replace-upload',
            action='store_true', help='confirm replace on upload')
        cmd.add_argument(
            '--replace-download',
            action='store_true', help='confirm replace on download')
        cmd.add_argument(
            '--delete-local',
            action='store_true', help='confirm delete local file')
        cmd.add_argument(
            '--delete-remote',
            action='store_true', help='confirm delete remote file')

        argcomplete.autocomplete(parser)
        namespace = parser.parse_args()

        if getattr(namespace, 'func', None):
            self.handler(namespace)
            return

        parser.print_help()

    def handler(self, namespace):
        if not self.conf.get('ACCESS_KEY') or not self.conf.get('SECRET_KEY'):
            raise errors.UserError('Missing access or secret key')

        self.debug('connecting s3...')
        # os.environ['S3_USE_SIGV4'] = 'True'
        self.conn = boto.s3.connection.S3Connection(
            self.conf.get('ACCESS_KEY'), self.conf.get('SECRET_KEY'))

        return namespace.func(namespace)

    def bucket(self, name=None):
        name = name or self.conf.get('BUCKET')
        if not name:
            return None

        for region in boto.s3.regions():
            if (self.conf.get('ALLOWED_REGIONS')
                    and region.name not in self.conf['ALLOWED_REGIONS']):
                continue
            conn = boto.s3.connection.S3Connection(
                self.conf.get('ACCESS_KEY'),
                self.conf.get('SECRET_KEY'),
                host=region.endpoint)
            if not conn:
                continue
            bucket = conn.lookup(name, validate=True)
            if bucket is not None:
                return bucket
        return None

    def on_config(self, namespace):
        if namespace.local:
            config_path = self.conf.get('LOCAL_CONFIG')
        else:
            config_path = settings.CONFIG_GLOBAL

        config = self.load_config(config_path) or {}

        if namespace.set:
            if '=' not in namespace.set:
                raise errors.UserError('Invalid config option')
            key, value = namespace.set.split('=', 1)
            config[key] = value

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
        config_path = os.path.join(os.getcwd(), settings.CONFIG_LOCAL_NAME)
        with open(config_path, 'w') as config_file:
            config = {'bucket': namespace.bucket}
            yaml.dump(config, config_file, default_flow_style=False)

    def on_list_buckets(self, namespace):  # pylint: disable=unused-argument
        self.info('listing buckets:')
        for bucket in self.conn.get_all_buckets():
            self.info(bucket.name)

    def on_list(self, namespace):
        bucket = utils.iter_remote_path(
            self.bucket(namespace.bucket),
            namespace.path,
            recursive=namespace.recursive)

        if bucket is False:
            raise errors.UserError('Missing bucket')

        for index, key in enumerate(bucket):
            if index >= namespace.limit > 0:
                self.info('list limit reached!')
                break
            self._print_key(key)

    def on_diff(self, namespace, print_=True):
        if namespace.all:
            modes = '=+-<>r'
        else:
            modes = namespace.modes

        src_files = []
        for file_path in utils.iter_local_path(
                namespace.path, namespace.recursive):
            if not os.path.isfile(file_path):
                continue

            if not utils.check_file_type(file_path, namespace.file_types):
                continue

            key = utils.file_key(file_path)
            if namespace.ignore_case:
                key = key.lower()
            src_files.append((key, file_path))

        self.info('{0} local objects', len(src_files))

        bucket = self.bucket()
        if not bucket:
            raise errors.UserError('missing bucket')

        remote_files = dict()

        ls_remote = utils.iter_remote_path(
            bucket, namespace.path, recursive=namespace.recursive)

        for file_ in ls_remote:
            if not isinstance(file_, boto.s3.key.Key) or file_.name[-1] == '/':
                continue
            if not utils.check_file_type(file_.name, namespace.file_types):
                continue

            key = file_.name
            if namespace.ignore_case:
                key = key.lower()

            remote_files[key] = dict(
                key=file_,
                name=file_.name,
                size=file_.size,
                modified=file_.last_modified,
                md5=file_.etag[1:-1],
                state='-',
                comment=[],
                local_path=utils.file_path(file_.name),
            )

        self.info('{0} remote objects', len(remote_files.keys()))

        if not src_files and not remote_files:
            return None

        self.info('comparing...')
        for key, f_path in src_files:
            stat = os.stat(f_path)

            if key in remote_files:
                equal = True
                remote = remote_files[key]
                remote['local_path'] = f_path

                if stat.st_size != remote['size']:
                    equal = False
                    if remote['size']:
                        diff = stat.st_size * 100 / float(remote['size'])
                    else:
                        diff = 0
                    remote['comment'].append('size: {:.2f}%'.format(diff))

                elif namespace.md5:
                    if utils.file_hash(f_path) != remote['md5']:
                        equal = False
                        remote['comment'].append('md5: different')

                if equal:
                    remote.update(state='=', comment=[])
                else:
                    remote['local_size'] = stat.st_size
                    local_modified = datetime.datetime.fromtimestamp(
                        stat.st_ctime).replace(microsecond=0)
                    remote_modified = datetime.datetime.strptime(
                        remote['modified'], '%Y-%m-%dT%H:%M:%S.000Z')
                    remote_modified += datetime.timedelta(hours=4)

                    delta = local_modified - remote_modified
                    if delta.days > 1:
                        remote['comment'].append(
                            'modified: remote {0} days older'.format(
                                delta.days))
                    else:
                        remote['comment'].append('modified: {0}'.format(delta))

                    if namespace.force_upload:
                        remote['state'] = '>'
                    elif namespace.force_download:
                        remote['state'] = '<'
                    elif local_modified > remote_modified:
                        remote['state'] = '>'
                    else:
                        remote['state'] = '<'

                if remote['state'] not in modes:
                    del remote_files[key]

            else:
                if '+' not in modes and 'r' not in modes:
                    continue

                remote_files[key] = dict(
                    local_size=stat.st_size,
                    local_path=f_path,
                    modified=stat.st_mtime,
                    md5=None,
                    state='+',
                    comment=[],
                )
                if namespace.md5:
                    remote_files[key]['md5'] = utils.file_hash(f_path)

        # find renames
        if 'r' in modes:
            to_del = []
            for key, new_data in remote_files.items():
                if new_data['state'] != '+':
                    continue
                for name, data in remote_files.items():
                    if data['state'] != '-':
                        continue
                    if data['size'] != new_data['local_size']:
                        continue
                    if namespace.md5 and data['md5'] != new_data['md5']:
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

        remote_files = {
            k: v for k, v in remote_files.items() if v['state'] in modes
        }

        if print_ and not namespace.brief:
            keys = remote_files.keys()
            for key in keys:
                data = remote_files[key]
                print('{} {} {}'.format(
                    data['state'],
                    key,
                    ', '.join(data.get('comment', []))))

        if remote_files:
            counter = collections.Counter()
            for data in remote_files.values():
                counter.update(data['state'])
            info = ', '.join(
                '{}: {}'.format(k, v) for k, v in counter.most_common())
            self.info('{} differences ({})', len(remote_files), info)

        else:
            self.info('{} differences', len(remote_files))

        return remote_files

    def on_remove(self, namespace):
        bucket = self.bucket()
        if not bucket:
            raise errors.UserError('Missing bucket')

        path = namespace.path.replace('\\', '/')

        if path[-1] == '/':
            raise errors.UserError('Path is dir')

        files = bucket.list(delimiter='/', prefix=path)
        files = list(files)

        if not files:
            raise errors.UserError('File not found')

        if len(files) > 1:
            raise errors.UserError('Multiple files found')

        remote_file = files[0]

        if not isinstance(remote_file, boto.s3.key.Key):
            raise errors.UserError('Try to remove dir')

        remote_file.delete()
        print('File successful deleted')

    def on_upload(self, namespace):
        bucket = self.bucket()
        if not bucket:
            raise errors.UserError('Missing bucket')

        files = {}
        for local_path in utils.iter_local_path(
                namespace.path, namespace.recursive):
            if not os.path.isfile(local_path):
                continue

            key = utils.file_key(local_path)
            files[key] = {
                'local_size': os.stat(local_path).st_size,
                'local_path': local_path,
            }

        for remote in utils.iter_remote_path(
                bucket, namespace.path, namespace.recursive):
            if remote.name in files:
                files[remote.name]['key'] = remote

        conflicts = 0
        pool = tasks.ThreadPool(
            self.conf['THREAD_MAX_COUNT'], self.conf, auto_start=False)

        for key, data in files.items():
            if 'key' in data and namespace.force:
                task = tasks.ReplaceUpload()
            elif 'key' not in data:
                data['key'] = boto.s3.key.Key(bucket=bucket, name=key)
                task = tasks.Upload()
            else:
                conflicts += 1
                continue

            pool.add_task(task, bucket, self.conf, key, data)

        if conflicts:
            print('{} remote paths exists, use force flag'.format(conflicts))

        with reprint.output(initial_len=self.conf['THREAD_MAX_COUNT']) as output:
            pool.start(output)
            pool.join()

    def on_update(self, namespace):
        files = self.on_diff(namespace, print_=False)
        if not files:
            self.error('no changes')
            return

        self.info('processing...')

        _t = time.time()
        processed, size = 0, 0

        try:
            processed, size = self._update(files, namespace)
        finally:
            delta = time.time() - _t
            if delta:
                speed = utils.humanize_size(size / delta)
                self.info('average speed: %s', speed)

            self.info(
                '{0} actions processed, {1} skipped',
                processed, len(files.keys()) - processed
            )

    def _update(self, files, namespace):
        processed = 0
        size = 0

        bucket = self.bucket()
        pool = tasks.ThreadPool(self.conf['THREAD_MAX_COUNT'], self.conf)

        for name, data in files.items():
            action = None

            if data['state'] == '=':
                processed += 1
                continue

            elif data['state'] == '+':
                if namespace.upload:
                    action = tasks.Upload()
                elif namespace.delete_local:
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
                if namespace.download:
                    action = tasks.Download()
                elif namespace.delete_remote:
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

            elif data['state'] == 'r':
                if self._check(
                        name, data, namespace.quiet,
                        namespace.rename_remote):
                    action = tasks.RenameRemote()
                elif self._check(
                        name, data, namespace.quiet,
                        namespace.rename_local):
                    action = tasks.RenameLocal()
                else:
                    continue

            elif data['state'] == '>':
                if self._check(
                        name, data, namespace.quiet,
                        namespace.replace_upload):
                    action = tasks.ReplaceUpload()
                else:
                    continue

            elif data['state'] == '<':
                if self._check(
                        name, data, namespace.quiet,
                        namespace.replace_download):
                    action = tasks.Download()
                else:
                    continue

            if not action:
                logging.error('Unknown action')
                continue
            pool.add_task(action, bucket, self.conf, name, data)
            processed += 1

            if isinstance(action, tasks.Download):
                size += data.get('size') or 0
            elif isinstance(action, (tasks.Upload, tasks.ReplaceUpload)):
                size += data.get('local_size') or 0

            if processed >= namespace.limit > 0:
                self.info('list limit reached!')
                break

        with reprint.output(initial_len=self.conf['THREAD_MAX_COUNT']) as output:
            pool.start(output)
            pool.join()

        return processed, size

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

        values_map = {str(value): value for value in values}

        if 'n' not in values_map:
            values_map['n'] = 'n'

        prompt_str = '{} {} {} ({} [all])? '.format(
            code, name,
            ', '.join(data.get('comment', [])),
            '/'.join(values_map.keys()),
        )

        input_data = []
        while not input_data or input_data[0] not in values_map:
            input_data = input(prompt_str)
            input_data = input_data.split(' ', 1)

        if len(input_data) > 1 and input_data[1] == 'all':
            self.confirm_permanent[code] = values_map[input_data[0]]

        return values_map[input_data[0]]

    def _print_key(self, key):
        name_len = self.conf['KEY_PATTERN_NAME_LEN']

        if len(key.name) < name_len:
            name = key.name.ljust(name_len, ' ')
        else:
            name = key.name[:name_len - 3] + '...'

        if isinstance(key, boto.s3.key.Key):
            params = {
                'name': name,
                'size': str(key.size).ljust(10, ' '),
                'owner': key.owner.display_name,
                'modified': key.last_modified,
                'storage': constants.STORAGE_ALIASES.get(
                    key.storage_class, '?'),
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

        pattern = self.conf.get('KEY_PATTERN')
        print(pattern.format(**params))


def main():
    tool = S3SyncTool()

    if settings.LOGGING:
        logging.config.dictConfig(settings.LOGGING)

    try:
        tool.run_cli()
    except errors.UserError as exc:
        tool.error(exc.args[0])
    except KeyboardInterrupt:
        tool.error('interrupted')


if __name__ == '__main__':
    main()
