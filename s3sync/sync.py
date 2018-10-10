# -*- coding: utf-8 -*-
from __future__ import print_function

import datetime
import os
import sys
import threading
import time

import boto.s3
import boto.s3.connection
import boto.s3.key

from . import commandtool, utils

UPLOAD_FMT = "{speed}\r"
# UPLOAD_FMT = "[{progress}>{left}] {progress_percent}% {speed}\r"


class UploadThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(UploadThread, self).__init__(*args, **kwargs)
        self.result = 0

    def run(self):
        if self.__target:
            self.result = self.__target(*self.__args, **self.__kwargs)
        else:
            self.result = 0


class S3SyncTool(commandtool.CommandTool):

    options = commandtool.CommandTool.options + [
        ("-A", "--access-key", dict(
            action="store",
            dest="access_key",
            help="AWS S3 access key")),
        ("-S", "--secret-key", dict(
            action="store",
            dest="secret_key",
            help="AWS S3 secret key")),
        ("-R", "--recursive", dict(
            action="store_true",
            dest="recursive",
            help="list/compare recursively")),
        ("-b", "--bucket", dict(
            action="store",
            dest="bucket",
            help="bucket name")),
        ("-r", "--local-root", dict(
            action="store",
            dest="local_root",
            help="root path to compare with")),
        ("-n", "--list-limit", dict(
            action="store",
            dest="list_limit",
            metavar="LIMIT",
            help="limit of listings")),
        ("--modes", dict(
            action="store",
            dest="modes",
            help="modes of comparing (by default: -=<>+)")),
        ("--file-types", dict(
            action="store",
            dest="file_types",
            help="file types for compare")),
        ("--confirm-upload", dict(
            action="store_true",
            dest="confirm_upload",
            help="confirm upload action")),
        ("--confirm-download", dict(
            action="store_true",
            dest="confirm_download",
            help="confirm download action")),
        ("--confirm-replace-upload", dict(
            action="store_true",
            dest="confirm_replace_upload",
            help="confirm replace on upload")),
        ("--confirm-replace-download", dict(
            action="store_true",
            dest="confirm_replace_download",
            help="confirm replace on download")),
        ("--confirm-delete-local", dict(
            action="store_true",
            dest="confirm_delete_local",
            help="confirm delete local file")),
        ("--confirm-delete-remote", dict(
            action="store_true",
            dest="confirm_delete_remote",
            help="confirm delete remote file")),
        ("--confirm-rename-remote", dict(
            action="store_true",
            dest="confirm_rename_remote",
            help="confirm rename remote file")),
        ("--force-upload", dict(
            action="store_true",
            dest="force_upload",
            help="data transfer direction force change to upload")),
        ("--skip-md5-compare", dict(
            action="store_true",
            dest="skip_md5",
            help="skip file content comparing")),
    ]

    def __init__(self, *args, **kwargs):
        super(S3SyncTool, self).__init__(*args, **kwargs)
        self.conn = None

    @staticmethod
    def default_config():
        conf = commandtool.CommandTool.default_config()
        conf.update(
            key_pat=u"{name} {storage} {size} {modified} {owner} {md5}",
            key_pat_name_len=60,
            list_limit=20,
            # config_file=__file__.split('.')[0] + '.yaml',
            modes='-=<>+',
            confirm_permanent=dict(),
            compare_hash=True,
            thread_max_count=24,
            upload_cb_num=5,
            upload_format=UPLOAD_FMT,
        )
        return conf

    def handler(self, cli=False):
        if not self.conf.get('access_key') or not self.conf.get('secret_key'):
            return self.error('missing access or secret key')

        self.info('connecting s3...')
        # os.environ['S3_USE_SIGV4'] = 'True'
        self.conn = boto.s3.connection.S3Connection(
            self.conf.get('access_key'), self.conf.get('secret_key'))

        super(S3SyncTool, self).handler(cli)

    def bucket(self):
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
            bucket = conn.lookup(self.conf['bucket'], validate=True)
            if bucket is not None:
                return bucket
        return None

    def on_list_buckets(self):
        self.info('listing buckets:')
        for bucket in self.conn.get_all_buckets():
            self.log(bucket.name)

    def on_list(self):
        if len(self.args) < 2:
            self.conf['compare_path'] = ''
        else:
            self.conf['compare_path'] = self.args[1]

        bucket = utils.list_remote_dir(self.bucket(), self.conf)
        if bucket is False:
            return self.error('missing bucket')

        for index, key in bucket:
            if index >= self.conf['list_limit']:
                self.info("list limit reached!")
                break
            self._print_key(key)

    def on_diff(self, print_=True):
        if not self.conf.get('local_root'):
            return self.error('missing local root directory reference')
        else:
            # ета переменная нужна, чтобы убирая из полного пути до
            #  локального файла получить путь до удаленного файла с корня
            #  (с букета)
            local_root_s = self.conf['local_root'].replace('\\', '/')
            if local_root_s[-1] != '/':
                local_root_s += '/'

        if len(self.args) < 2:
            self.conf['compare_path'] = ''
        else:
            self.conf['compare_path'] = self.args[1]

        src_path = os.path.join(
            self.conf['local_root'], self.conf['compare_path'])
        src_files = []
        if self.conf.get('recursive'):
            for dir_path, __, file_names in os.walk(src_path):
                for file_ in file_names:
                    if not self._check_file_type(file_):
                        continue
                    f_path = os.path.join(dir_path, file_)
                    key = f_path.replace('\\', '/').replace(
                        local_root_s, '').decode('cp1251').lower()
                    src_files.append((key, f_path))
        else:
            for file_ in os.listdir(src_path):
                if not self._check_file_type(file_):
                    continue
                f_path = os.path.join(src_path, file_)
                if not os.path.isfile(f_path):
                    continue
                key = os.path.join(self.conf['compare_path'], file_).replace(
                    '\\', '/').decode('cp1251').lower()
                src_files.append((key, f_path))
        self.info(u"{0} local objects", len(src_files))

        remote_files = dict()
        ls_remote = utils.list_remote_dir(self.bucket(), self.conf)
        if ls_remote is False:
            return self.error('missing bucket')

        for __, file_ in ls_remote:
            if not isinstance(file_, boto.s3.key.Key) or file_.name[-1] == '/':
                continue
            if not self._check_file_type(file_.name):
                continue
            remote_files[file_.name.lower()] = dict(
                key=file_,
                name=file_.name,
                size=file_.size,
                modified=file_.last_modified,
                md5=file_.etag[1:-1],
                state=u'-',
                comment=[],
            )
        self.info(u"{0} remote objects", len(remote_files.keys()))

        if not src_files and not remote_files:
            return

        self.info(u"comparing...")
        for key, f_path in src_files:
            stat = os.stat(f_path)

            if key in remote_files:
                equal = True
                if stat.st_size != remote_files[key]['size']:
                    equal = False
                    remote_files[key]['comment'].append(u"size: {0}%".format(
                        round(float(
                            remote_files[key]['size']) / stat.st_size * 100, 2)
                    ))
                elif not self.conf.get('skip_md5'):
                    hash_ = utils.file_hash(f_path)
                    if hash_ != remote_files[key]['md5']:
                        equal = False
                        remote_files[key]['comment'].append(u"md5: different")

                if equal:
                    remote_files[key].update(state='=', comment=[])
                else:
                    remote_files[key]['local_size'] = stat.st_size
                    local_modified = datetime.datetime.fromtimestamp(
                        stat.st_ctime).replace(microsecond=0)
                    remote_modified = datetime.datetime.strptime(
                        remote_files[key]['modified'],
                        u"%Y-%m-%dT%H:%M:%S.000Z")
                    remote_modified += datetime.timedelta(hours=4)

                    remote_files[key]['comment'].append(
                        u"modified: {0}".format(
                            local_modified - remote_modified))
                    if local_modified > remote_modified:
                        remote_files[key]['state'] = '>'
                    else:
                        remote_files[key]['state'] = '<'

                if remote_files[key]['state'] not in self.conf['modes']:
                    del remote_files[key]
            else:
                if ('+' not in self.conf['modes']
                        and 'r' not in self.conf['modes']):
                    continue

                remote_files[key] = dict(
                    local_size=stat.st_size,
                    modified=stat.st_mtime,
                    md5=utils.file_hash(f_path),
                    state='+',
                    comment=[],
                )

        # find renames
        if 'r' in self.conf['modes']:
            to_del = []
            for key, new_data in remote_files.iteritems():
                if new_data['state'] != '+':
                    continue
                for name, data in remote_files.iteritems():
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
                        u'new: {0}'.format(key))
                    to_del.append(key)
                    break
            for key in to_del:
                del remote_files[key]

        if '-' not in self.conf['modes'] or '+' not in self.conf['modes']:
            for key, value in remote_files.items():
                if value['state'] not in self.conf['modes']:
                    del remote_files[key]

        if print_:
            keys = remote_files.keys()
            keys.sort()
            for key in keys:
                self._print_diff_line(key, remote_files[key])
            self.info(u"{0} differences", len(remote_files.keys()))
        else:
            return remote_files

    def on_update(self):
        files = self.on_diff(print_=False)
        self.info('processing...')
        processed = 0
        threads = []
        _t = time.time()
        _size = 0

        try:
            for name, data in files.iteritems():
                if data['state'] == '=':
                    processed += 1
                    continue
                elif data['state'] == '+':
                    if self.conf.get('confirm_upload'):
                        data['action'] = 'upload'
                    elif self.conf.get('confirm_delete_local'):
                        data['action'] = 'delete_local'
                    else:
                        act = self._confirm_update(
                            name, data, 'upload', 'delete_local')
                        if act == 'n':
                            continue
                        else:
                            data['action'] = act
                elif data['state'] == '-':
                    if self.conf.get('confirm_download'):
                        data['action'] = 'download'
                    elif self.conf.get('confirm_delete_remote'):
                        data['action'] = 'delete_remote'
                    else:
                        act = self._confirm_update(
                            name, data, 'download', 'delete_remote')
                        if act == 'n':
                            continue
                        else:
                            data['action'] = act
                elif data['state'] == '>' or self.conf.get('force_upload'):
                    data['state'] = '>'
                    if (not self.conf.get('confirm_replace_upload')
                            and self._confirm_update(name, data, 'y') == 'n'):
                        continue
                    data['action'] = 'replace_upload'
                elif data['state'] == '<':
                    if (not self.conf.get('confirm_replace_download')
                            and self._confirm_update(name, data, 'y') == 'n'):
                        continue
                    data['action'] = 'replace_download'
                elif data['state'] == 'r':
                    if (not self.conf.get('confirm_rename_remote')
                            and self._confirm_update(name, data, 'y') == 'n'):
                        continue
                    data['action'] = 'rename_remote'

                _size += data.get('local_size', 0)

                if self.conf['thread_max_count'] > 1:
                    threads = [thr for thr in threads if thr.isAlive()]
                    while len(threads) >= self.conf['thread_max_count']:
                        thr_last = threads.pop()
                        thr_last.join()
                    thr = threading.Thread(
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
            self.info(u"average speed: {0}", self._get_speed())
            self.info(
                u"{0} actions processed, {1} skipped",
                processed, len(files.keys()) - processed
            )

    def _update_process(self, name, data):
        try:
            getattr(self, '_update_{0}'.format(data['action']))(name, data)
            self._print_diff_line(name, data)
            return 1
        except (AttributeError, NotImplementedError):
            self.error(u'not developed yet')
            return 0
        except Exception:
            self.error(u'file {0} update failed', name)
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

        data['comment'] = [u'uploaded(replaced)']

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
        data['comment'] = [u'uploaded']

    def _update_delete_remote(self, __, data):
        data['key'].delete()
        data['comment'] = [u'deleted from s3']

    def _update_rename_remote(self, __, data):
        new_key = data['key'].copy(
            self.conf['bucket'], data['local_name'],
            metadata=None,
            reduced_redundancy=True, preserve_acl=True,
            encrypt_key=False, validate_dst_bucket=True)
        if new_key:
            data['key'].delete()
            data['comment'] = [u'renamed']
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

    def _check_file_type(self, filename):
        filename = filename.lower()
        if not self.conf.get('file_types'):
            return True
        file_types = self.conf['file_types'].lower().split(',')
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
        pr_line = "|/-\\"
        if '_action_progress' not in self.conf:
            self.conf['_action_progress'] = 0
        else:
            self.conf['_action_progress'] += 1
        sys.stdout.write("{0}\r".format(
            pr_line[self.conf['_action_progress'] % len(pr_line)]))
        self.conf['to_clear_command_line'] = True

    def _confirm_update(self, name, data, *values):
        return self.confirm(
            u"{0} {1} {2}".format(
                data['state'], name, ', '.join(data.get('comment', []))),
            data['state'],
            values=values,
            allow_remember=True)

    def _print_key(self, key):
        storage = dict(
            GLACIER='G',
            STANDARD='S',
            REDUCED_REDUNDANCY='R',
        )
        name = \
            key.name.ljust(self.conf['key_pat_name_len'], " ") \
            if len(key.name) < self.conf['key_pat_name_len'] \
            else key.name[:self.conf['key_pat_name_len'] - 3] + '...'
        if isinstance(key, boto.s3.key.Key):
            params = dict(
                name=name,
                size=str(key.size).ljust(10, " "),
                owner=key.owner.display_name,
                modified=key.last_modified,
                storage=storage.get(key.storage_class, '?'),
                md5=key.etag[1:-1],
            )
        else:
            params = dict(
                name=name,
                size='<DIR>'.ljust(10, " "),
                owner='',
                modified='',
                storage='?',
                md5=''
            )

        self.log(unicode(self.conf['key_pat']).format(**params))

    def _print_diff_line(self, name, data):
        self.info(
            u"{0} {1} {2}",
            data['state'],
            name,
            ', '.join(data.get('comment', []))
        )


def main():
    tool = S3SyncTool()
    try:
        tool.run_cli()
    except KeyboardInterrupt:
        tool.log('interrupted')


if __name__ == '__main__':
    main()
