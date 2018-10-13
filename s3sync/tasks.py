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
            func, args, kwargs = self.task_queue.get()
            try:
                result = func(*args, worker=self, **kwargs)
                self.result_queue.put(result)
            finally:
                self.task_queue.task_done()


class ThreadPool(object):
    def __init__(self, num_threads):
        self.result_queue = six.moves.queue.Queue()

        self.task_queue = six.moves.queue.Queue(num_threads)
        for index in six.moves.range(num_threads):
            w = Worker(index, self.task_queue, self.result_queue)
            w.start()

    def add_task(self, func, *args, **kargs):
        self.task_queue.put((func, args, kargs))

    def join(self):
        self.task_queue.join()


class Task(object):
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
        self.handler()

    def progress(self, uploaded, full):
        len_full = 40
        progress = round(float(uploaded) / full, 2) * 100
        progress_len = int(progress) * len_full / 100

        if False and self.speed_queue.qsize():
            count = self.speed_queue.qsize()
            speed_sum = sum(
                self.speed_queue.get(i)
                for i in six.moves.range(count))
            speed_value = float(speed_sum / count)
            speed = utils.humanize_size(speed_value)
        else:
            speed = 'n\\a'

        self.output[self.worker.index] = settings.UPLOAD_FORMAT.format(
            progress='=' * progress_len,
            left=' ' * (len_full - progress_len),
            progress_percent=progress,
            speed=speed,
        )


def _upload(key, local_root, name, callback, speed_queue, local_size, replace=False):
    local_file_path = os.path.join(local_root, name)

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
            self.conf['local_root'],
            self.name,
            self.progress,
            self.speed_queue,
            self.data.get('local_size'),
        )
        self.data['comment'] = ['uploaded']


class ReplaceUpload(Task):
    def __str__(self):
        return 'upload_replace'

    def handler(self):
        _upload(
            self.data['key'],
            self.conf['local_root'],
            self.name,
            self.progress,
            self.speed_queue,
            self.data.get('local_size'),
            replace=True,
        )
        self.data['comment'] = ['uploaded(replaced)']


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
        self.data['key'].get_contents_to_filename(
            os.path.join(self.conf['local_root'], self.name),
            cb=self.progress,
            num_cb=20,
        )
