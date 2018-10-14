import pytest
import mock

from s3sync import utils


@pytest.mark.unit
@mock.patch('s3sync.utils.get_cwd')
@mock.patch('s3sync.utils.find_project_root')
def test_file_path_info_1(find_project_root, get_cwd):
    find_project_root.return_value = '/project'
    get_cwd.return_value = '/project'

    file_path, key = utils.file_path_info('file')
    assert file_path == '/project/file'
    assert key == 'file'


@pytest.mark.unit
@mock.patch('s3sync.utils.get_cwd')
@mock.patch('s3sync.utils.find_project_root')
def test_file_path_info_2(find_project_root, get_cwd):
    find_project_root.return_value = '/project'
    get_cwd.return_value = '/project/sub_path'

    file_path, key = utils.file_path_info('file')
    assert file_path == '/project/sub_path/file'
    assert key == 'sub_path/file'


@pytest.mark.unit
@mock.patch('s3sync.utils.get_cwd')
@mock.patch('s3sync.utils.find_project_root')
def test_file_path_info_3(find_project_root, get_cwd):
    find_project_root.return_value = '/project'
    get_cwd.return_value = '/project'

    file_path, key = utils.file_path_info(None)
    assert file_path == '/project'
    assert key == ''


@pytest.mark.unit
@mock.patch('s3sync.utils.get_cwd')
@mock.patch('s3sync.utils.find_project_root')
def test_file_path_info_4(find_project_root, get_cwd):
    find_project_root.return_value = '/project'
    get_cwd.return_value = '/project/sub_path'

    file_path, key = utils.file_path_info(None)
    assert file_path == '/project/sub_path'
    assert key == 'sub_path'


@pytest.mark.unit
@mock.patch('s3sync.utils.get_cwd')
@mock.patch('s3sync.utils.find_project_root')
def test_file_path_info_5(find_project_root, get_cwd):
    find_project_root.return_value = '/project'
    get_cwd.return_value = '/project'

    file_path, key = utils.file_path_info('/project/sub/file')
    assert file_path == '/project/sub/file'
    assert key == 'sub/file'


@pytest.mark.unit
@mock.patch('s3sync.utils.get_cwd')
@mock.patch('s3sync.utils.find_project_root')
def test_file_path_info_6(find_project_root, get_cwd):
    find_project_root.return_value = '/project'
    get_cwd.return_value = '/project/sub_path'

    file_path, key = utils.file_path_info('/project/sub_path/file')
    assert file_path == '/project/sub_path/file'
    assert key == 'sub_path/file'
