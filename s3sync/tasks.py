import logging.config
import os
import queue
import threading
import time

import boto.s3.key

from . import utils

logger = logging.getLogger(__name__)


class Worker(threading.Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, index, task_queue, result_queue, output=None):
        super(Worker, self).__init__()
        self.index = index
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.daemon = True
        self.speed_list = []
        self.output = output

    def run(self):
        while self.task_queue.unfinished_tasks:
            try:
                func, args = self.task_queue.get(timeout=10)
            except queue.Empty:
                break

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


class System(threading.Thread):
    def __init__(self, index, result_queue, output, tasks_total, conf):
        super(System, self).__init__()
        self.daemon = True

        self.index = index
        self.queue = result_queue
        self.output = output
        self.conf = conf

        self.tasks_total = tasks_total
        self.tasks_processed = 0
        self.size = 0

        self._t = time.time()

    def run(self):
        while True:
            data = self.queue.get()
            try:
                self.handler(data)
            finally:
                self.queue.task_done()

    def handler(self, data):
        self.tasks_processed += 1
        self.size += data['size']

        len_full = 40
        progress = float(self.tasks_processed) / self.tasks_total * 100
        progress_len = int(progress) * len_full // 100

        delta = time.time() - self._t
        if delta:
            speed = utils.humanize_size(self.size / delta)
        else:
            speed = 'n\\a'

        self.output[self.index] = self.conf['UPLOAD_FORMAT'].format(
            progress='=' * progress_len,
            left=' ' * (len_full - progress_len),
            progress_percent=progress,
            speed=speed,
            info='{}/{}'.format(self.tasks_processed, self.tasks_total),
        )


class ThreadPool:
    def __init__(self, num_threads, conf, auto_start=False):
        self.num_threads = num_threads
        self.result_queue = queue.Queue()
        self.task_queue = queue.Queue()
        self.sys = None
        self.tasks_total = 0
        self.conf = conf

        if auto_start:
            self.start()

    def start(self, output=None):
        if self.num_threads > 1:
            self.sys = System(
                0, self.result_queue, output, self.tasks_total, self.conf)
            self.sys.start()

        for index in range(1, self.num_threads):
            worker = Worker(index, self.task_queue, self.result_queue, output)
            worker.start()

    def add_task(self, task, bucket, conf, name, data):
        self.tasks_total += 1
        args = bucket, conf, name, data
        self.task_queue.put((task, args))

    def join(self):
        while self.task_queue.unfinished_tasks:
            time.sleep(0.1)

        self.task_queue.join()

        if self.sys is not None:
            self.sys.join(timeout=1)


class Task:
    done = 'finished'

    def __init__(self):
        self.bucket = None
        self.conf = None
        self.name = None
        self.data = None
        self.worker = None
        self._t = None

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

        return {'size': size}

    def size(self):  # pylint: disable=no-self-use
        return 0

    def progress(self, uploaded, full):
        len_full = 40
        progress = round(float(uploaded) / full, 2) * 100
        progress_len = int(progress) * len_full // 100

        size = self.size()
        if size:
            uploaded = size * float(uploaded) / full
            speed_value = self.worker.speed(uploaded / (time.time() - self._t))
            speed = utils.humanize_size(speed_value)
        else:
            speed = 'n\\a'

        line = self.conf['UPLOAD_FORMAT'].format(
            progress='=' * progress_len,
            left=' ' * (len_full - progress_len),
            progress_percent=progress,
            speed=speed,
            info='{} {}'.format(self, self.name)
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
        prefix = self.conf['THREAD_MAX_COUNT']
        total = prefix + self.conf['ENDED_OUTPUT_MAX_COUNT']
        if len(output) >= total:
            output[prefix:total] = output[prefix + 1:total] + [line]
        else:
            output.append(line)


def _upload(key, callback, local_path, cb_num, replace=False, rrs=False):
    local_file_path = utils.file_path(local_path)

    with open(local_file_path, 'rb') as local_file:
        key.set_contents_from_file(
            local_file,
            replace=replace,
            cb=callback,
            num_cb=cb_num,
            reduced_redundancy=rrs,
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
            self.conf['UPLOAD_CB_NUM'],
            rrs=self.conf['REDUCED_REDUNDANCY'],
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
            self.conf['UPLOAD_CB_NUM'],
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
            self.conf['BUCKET'], self.data['local_name'],
            metadata=None,
            reduced_redundancy=self.conf['REDUCED_REDUNDANCY'],
            preserve_acl=True,
            encrypt_key=False,
            validate_dst_bucket=True,
        )

        if new_key:
            self.data['key'].delete()
            self.data['comment'] = ['renamed']
        else:
            raise Exception('s3 key copy failed')


class RenameLocal(Task):
    done = 'renamed (local)'

    def __str__(self):
        return 'rename_local'

    def handler(self):
        dest_name = os.path.join(
            self.conf['PROJECT_ROOT'], self.data['key'].name)

        dest_dir = os.path.dirname(dest_name)
        # TODO: add lock
        if not os.path.exists(dest_dir):
            try:
                os.makedirs(dest_dir)
            except OSError as exc:
                self.data['comment'] = ['failed: {}'.format(exc)]
                return

        os.rename(
            os.path.join(self.conf['PROJECT_ROOT'], self.data['local_name']),
            dest_name,
        )
        self.data['comment'] = ['renamed']


class Download(Task):
    done = 'downloaded'

    def __str__(self):
        return 'download'

    def size(self):
        return self.data.get('size') or 0

    def handler(self):
        file_path = self.data['local_path']

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
