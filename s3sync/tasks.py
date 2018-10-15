# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import logging.config
import os
import threading
import time

import boto.s3.key
import six

from . import settings, utils

logger = logging.getLogger(__name__)


class Worker(threading.Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, index, task_queue, result_queue):
        super(Worker, self).__init__().__init__()
        self.index = index
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.daemon = True

    def run(self):
        while True:
            func, args = self.task_queue.get()
            try:
                result = func(*args, worker=self)
                if self.result_queue:
                    self.result_queue.put(result)
            finally:
                self.task_queue.task_done()


class ThreadPool(object):
    def __init__(self, num_threads):
        # self.result_queue = six.moves.queue.Queue()
        self.result_queue = None

        self.task_queue = six.moves.queue.Queue(num_threads)
        for index in six.moves.range(num_threads):
            worker = Worker(index, self.task_queue, self.result_queue)
            worker.start()

    def add_task(self, task, bucket, speed_queue, conf, name, data, output):
        args = bucket, speed_queue, conf, name, data, output
        self.task_queue.put((task, args))

    def join(self):
        self.task_queue.join()


class Task(object):
    def __init__(self):
        self.bucket = None
        self.speed_queue = None
        self.conf = None
        self.name = None
        self.data = None
        self.output = None
        self.worker = None

    def handler(self):
        raise NotImplementedError()

    def __call__(
            self, bucket, speed_queue, conf, name, data, output, worker=None):

        self.bucket = bucket
        self.speed_queue = speed_queue
        self.conf = conf
        self.name = name
        self.data = data
        self.output = output
        self.worker = worker
        self._t = time.time()
        self.handler()

    def progress(self, uploaded, full):
        len_full = 40
        progress = round(float(uploaded) / full, 2) * 100
        progress_len = int(progress) * len_full / 100

        local_size = self.data.get('local_size')
        if local_size:
            uploaded = local_size * float(uploaded) / full
            speed = utils.humanize_size(uploaded / (time.time() - self._t))
        else:
            speed = 'n\\a'

        line = settings.UPLOAD_FORMAT.format(
            progress='=' * progress_len,
            left=' ' * (len_full - progress_len),
            progress_percent=progress,
            speed=speed,
            name=self.name,
            action=str(self),
        )
        if self.output and self.worker:
            self.output[self.worker.index] = line
        else:
            print(line)


def _upload(key, callback, speed_queue, local_size, local_path, replace=False):
    local_file_path = utils.file_path(local_path)

    with open(local_file_path, 'rb') as local_file:
        time_start = time.time()
        key.set_contents_from_file(
            local_file,
            replace=replace,
            cb=callback,
            num_cb=settings.UPLOAD_CB_NUM,
            reduced_redundancy=True,
            rewind=True,
        )
        delta = time.time() - time_start
        if delta and speed_queue:
            speed_queue.put(float(local_size / delta))


class Upload(Task):
    def __str__(self):
        return 'upload'

    def handler(self):
        _upload(
            boto.s3.key.Key(bucket=self.bucket, name=self.name),
            self.progress,
            self.speed_queue,
            self.data.get('local_size'),
            self.data['local_path'],
        )
        self.data['comment'] = ['uploaded']
        self.output.append('uploaded {}'.format(self.name))


class ReplaceUpload(Task):
    def __str__(self):
        return 'upload_replace'

    def handler(self):
        _upload(
            self.data['key'],
            self.progress,
            self.speed_queue,
            self.data.get('local_size'),
            self.data['local_path'],
            replace=True,
        )
        self.data['comment'] = ['uploaded(replaced)']
        self.output.append('uploaded (replaced) {}'.format(self.name))


class DeleteRemote(Task):
    def __str__(self):
        return 'delete_remote'

    def handler(self):
        self.data['key'].delete()
        self.data['comment'] = ['deleted from s3']


class RenameRemote(Task):
    def __str__(self):
        return 'rename_remote'

    def handler(self):
        new_key = self.data['key'].copy(
            self.conf['bucket'], self.data['local_name'],
            metadata=None,
            reduced_redundancy=True, preserve_acl=True,
            encrypt_key=False, validate_dst_bucket=True)

        if new_key:
            self.data['key'].delete()
            self.data['comment'] = ['renamed']
        else:
            raise Exception('s3 key copy failed')


class Download(Task):
    def __str__(self):
        return 'download'

    def handler(self):
        file_path = utils.file_path(self.data['local_path'])

        # ensure path
        file_dir = os.path.dirname(file_path)
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)

        self.data['key'].get_contents_to_filename(
            file_path,
            cb=self.progress,
            num_cb=20,
        )

        line = 'downloaded {}'.format(self.name)
        prefix = settings.THREAD_MAX_COUNT
        total = prefix + settings.ENDED_OUTPUT_MAX_COUNT
        if len(self.output) >= total:
            self.output[prefix:total] = self.output[prefix + 1:total]
        self.output.append(line)


class DeleteLocal(Task):
    def __str__(self):
        return 'delete_local'

    def handler(self):
        self.progress(0, 1)
        os.remove(self.data['local_path'])
        self.progress(1, 1)
