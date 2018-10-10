import fcntl
import os
import struct
import termios


def file_hash(f_path):
    from hashlib import md5
    file_ = open(f_path, 'rb')
    hash_ = md5()
    while True:
        block = file_.read(128)
        if not block:
            break
        hash_.update(block)
    file_.close()
    return hash_.hexdigest()


def list_remote_dir(bucket, opt):
    if not bucket:
        return False

    params = dict()
    if not opt.get('recursive'):
        params['delimiter'] = '/'
    if opt.get('compare_path'):
        params['prefix'] = opt['compare_path'].replace('\\', '/')
        if params['prefix'][-1] != '/':
            params['prefix'] += '/'

    return enumerate(bucket.list(**params))


def get_terminal_size(descriptor=1):
    """
    Returns height and width of current terminal. First tries to get
    size via termios.TIOCGWINSZ, then from environment. Defaults to 25
    lines x 80 columns if both methods fail.

    :param descriptor: file descriptor (default: 1=stdout)
    """
    try:
        return struct.unpack(
            'hh', fcntl.ioctl(descriptor, termios.TIOCGWINSZ, '1234'))
    except ValueError:
        return os.getenv('LINES', '25'), os.getenv('COLUMNS', '80')
