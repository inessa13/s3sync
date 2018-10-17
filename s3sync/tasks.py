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


class QueueEx(six.moves.queue.Queue):
    def join_with_timeout(self, timeout):
        self.all_tasks_done.acquire()
        try:
            end_time = time.time() + timeout
            while self.unfinished_tasks:
                remaining = end_time - time.time()
                if remaining <= 0.0:
                    raise RuntimeError('not finished')
                self.all_tasks_done.wait(remaining)
        finally:
            self.all_tasks_done.release()


class Worker(threading.Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, index, task_queue, result_queue, output=None):
        super(Worker, self).__init__().__init__()
        self.index = index
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.daemon = True
        self.speed_list = []
        self.output = output

    def run(self):
        while True:
            func, args = self.task_queue.get()
            try:
                result = func(*args, worker=self)
                if self.result_queue:
                    self.result_queue.put(result)
            finally:
                self.task_queue.task_done()

    def speed(self, current):
        if not self.speed_list:
            return current
        return (sum(
            self.speed_list) + current) / float(len(self.speed_list) + 1)


class ThreadPool(object):
    def __init__(self, num_threads, auto_start=False):
        self.num_threads = num_threads
        self.result_queue = None  # six.moves.queue.Queue()
        self.task_queue = QueueEx()

        if auto_start:
            self.start()

    def start(self, output=None):
        for index in six.moves.range(self.num_threads):
            worker = Worker(index, self.task_queue, self.result_queue, output)
            worker.start()

    def add_task(self, task, bucket, conf, name, data):
        args = bucket, conf, name, data
        self.task_queue.put((task, args))

    def join(self):
        # self.task_queue.join_with_timeout(10)
        self.task_queue.join()


class Task(object):
    done = 'finished'

    def __init__(self):
        self.bucket = None
        self.conf = None
        self.name = None
        self.data = None
        self.worker = None

    def handler(self):
        raise NotImplementedError()

    def __call__(self, bucket, conf, name, data, worker=None):
        self.bucket = bucket
        self.conf = conf
        self.name = name
        self.data = data
        self.worker = worker

        self._t = time.time()

        self.handler()

        size = self.size()
        if size:
            self.worker.speed_list.append(size / (time.time() - self._t))

        self.output_finish()

    def size(self):
        return 0

    def progress(self, uploaded, full):
        len_full = 40
        progress = round(float(uploaded) / full, 2) * 100
        progress_len = int(progress) * len_full / 100

        size = self.size()
        if size:
            uploaded = size * float(uploaded) / full
            speed_value = self.worker.speed(uploaded / (time.time() - self._t))
            speed = utils.humanize_size(speed_value)
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
        self.output_edit(line)

    def output_edit(self, line):
        if self.worker:
            self.worker.output[self.worker.index] = line
        else:
            print(line)

    def output_finish(self):
        line = '{} {}'.format(self.done, self.name)
        if not self.worker:
            print(line)
            return

        output = self.worker.output
        with output.lock:
            prefix = settings.THREAD_MAX_COUNT
            total = prefix + settings.ENDED_OUTPUT_MAX_COUNT
            if len(output) >= total:
                output[prefix:total] = output[prefix + 1:total]

        output.append(line)


def _upload(key, callback, local_path, replace=False):
    local_file_path = utils.file_path(local_path)

    with open(local_file_path, 'rb') as local_file:
        key.set_contents_from_file(
            local_file,
            replace=replace,
            cb=callback,
            num_cb=settings.UPLOAD_CB_NUM,
            reduced_redundancy=True,
            rewind=True,
        )


class Upload(Task):
    done = 'uploaded'

    def __str__(self):
        return 'upload'

    def size(self):
        return self.data.get('local_size') or 0

    def handler(self):
        _upload(
            boto.s3.key.Key(bucket=self.bucket, name=self.name),
            self.progress,
            self.data['local_path'],
        )
        self.data['comment'] = ['uploaded']


class ReplaceUpload(Task):
    done = 'uploaded (replace)'

    def __str__(self):
        return 'upload_replace'

    def size(self):
        return self.data.get('local_size') or 0

    def handler(self):
        _upload(
            self.data['key'],
            self.progress,
            self.data['local_path'],
            replace=True,
        )
        self.data['comment'] = ['uploaded(replaced)']


class DeleteRemote(Task):
    done = 'deleted (remote)'

    def __str__(self):
        return 'delete_remote'

    def handler(self):
        self.data['key'].delete()
        self.data['comment'] = ['deleted from s3']


class RenameRemote(Task):
    done = 'renamed (remote)'

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
    done = 'downloaded'

    def __str__(self):
        return 'download'

    def size(self):
        return self.data.get('size') or 0

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


class DeleteLocal(Task):
    done = 'deleted (local)'

    def __str__(self):
        return 'delete_local'

    def handler(self):
        self.progress(0, 1)
        os.remove(self.data['local_path'])
        self.progress(1, 1)
