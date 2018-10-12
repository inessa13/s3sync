import fcntl
import os
import re
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


def file_key(local_root_s, file_path, file_types=None):
    if file_types and not check_file_type(file_path, file_types):
        return None

    key = file_path

    try:
        key = key.decode('utf8')
    except UnicodeEncodeError:
        try:
            key = key.decode('cp1251')
        except UnicodeEncodeError:
            key = key

    key = key.replace('\\', '/')
    key = re.sub('^({})'.format(local_root_s), '', key)
    return key


def list_remote_dir(bucket, src_path, local_root_s, recursive=False):
    if not bucket:
        return False

    if local_root_s:
        compare_path = file_key(local_root_s, src_path)
    else:
        compare_path = src_path

    if compare_path and os.path.isdir(src_path):
        if compare_path[-1] != '/':
            compare_path += '/'

    params = dict()
    if not recursive:
        params['delimiter'] = '/'

    if compare_path:
        params['prefix'] = compare_path.replace('\\', '/')

    return bucket.list(**params)


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
    return '{:.2f} {}'.format(value, label)


def check_file_type(filename, types):
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
