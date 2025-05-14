import pytest
import pyarrow as pa
import pyarrow.fs as pa_fs
from unittest.mock import Mock, patch
import os
from guidewire.storage import Storage

@pytest.fixture
def mock_azure_fs():
    with patch('pyarrow.fs.AzureFileSystem') as mock:
        yield mock

@pytest.fixture
def mock_s3_fs():
    with patch('pyarrow.fs.S3FileSystem') as mock:
        yield mock

@pytest.fixture
def azure_storage(mock_azure_fs):
    with patch.dict(os.environ, {
        'AZURE_STORAGE_ACCOUNT_NAME': 'test_account',
        'AZURE_STORAGE_ACCOUNT_KEY': 'test_key'
    }):
        storage = Storage(cloud="azure")
        storage.filesystem = mock_azure_fs.return_value
        return storage

@pytest.fixture
def aws_storage(mock_s3_fs):
    with patch.dict(os.environ, {
        'AWS_REGION': 'us-west-2',
        'AWS_ACCESS_KEY_ID': 'test_key',
        'AWS_SECRET_ACCESS_KEY': 'test_secret'
    }):
        storage = Storage(cloud="aws")
        storage.filesystem = mock_s3_fs.return_value
        return storage

def test_storage_init_azure():
    with patch.dict(os.environ, {
        'AZURE_STORAGE_ACCOUNT_NAME': 'test_account',
        'AZURE_STORAGE_ACCOUNT_KEY': 'test_key'
    }), patch('pyarrow.fs.AzureFileSystem') as mock_fs:
        storage = Storage(cloud="azure")
        mock_fs.assert_called_once_with(
            account_name='test_account',
            account_key='test_key'
        )

def test_storage_init_aws():
    with patch.dict(os.environ, {
        'AWS_REGION': 'us-west-2',
        'AWS_ACCESS_KEY_ID': 'test_key',
        'AWS_SECRET_ACCESS_KEY': 'test_secret'
    }), patch('pyarrow.fs.S3FileSystem') as mock_fs:
        storage = Storage(cloud="aws")
        mock_fs.assert_called_once_with(
            region='us-west-2',
            access_key='test_key',
            secret_key='test_secret'
        )

def test_storage_init_invalid_cloud():
    with pytest.raises(ValueError, match="Invalid cloud provider: invalid"):
        Storage(cloud="invalid")

def test_read_parquet(azure_storage):
    mock_table = pa.table({'col1': [1, 2, 3]})
    azure_storage.filesystem.open_input_stream = Mock()
    with patch('pyarrow.parquet.read_table', return_value=mock_table):
        result = azure_storage.read_parquet('test.parquet')
        assert result == mock_table

def test_read_parquet_error(azure_storage):
    azure_storage.filesystem.open_input_stream = Mock(side_effect=Exception("Test error"))
    with pytest.raises(Exception):
        azure_storage.read_parquet('test.parquet')

def test_write_parquet(azure_storage):
    mock_table = pa.table({'col1': [1, 2, 3]})
    azure_storage.filesystem.open_output_stream = Mock()
    with patch('pyarrow.parquet.write_table') as mock_write:
        azure_storage.write_parquet('test.parquet', mock_table)
        mock_write.assert_called_once()

def test_write_parquet_error(azure_storage):
    mock_table = pa.table({'col1': [1, 2, 3]})
    azure_storage.filesystem.open_output_stream = Mock(side_effect=Exception("Test error"))
    with pytest.raises(Exception):
        azure_storage.write_parquet('test.parquet', mock_table)

def test_read_json(azure_storage):
    mock_data = {'key': ['value']}
    mock_table = pa.table({'key': ['value']})
    azure_storage.filesystem.open_input_stream = Mock()
    with patch('pyarrow.json.read_json', return_value=mock_table):
        result = azure_storage.read_json('test.json')
        assert result == mock_data

def test_read_json_error(azure_storage):
    azure_storage.filesystem.open_input_stream = Mock(side_effect=Exception("Test error"))
    with pytest.raises(Exception):
        azure_storage.read_json('test.json')

def test_list_files(azure_storage):
    expected_files = ['file1.txt', 'file2.txt']
    azure_storage.filesystem.ls = Mock(return_value=expected_files)
    result = azure_storage.list_files('test_dir')
    assert result == expected_files
    azure_storage.filesystem.ls.assert_called_once_with('test_dir')

def test_list_files_error(azure_storage):
    azure_storage.filesystem.ls = Mock(side_effect=Exception("Test error"))
    with pytest.raises(Exception):
        azure_storage.list_files('test_dir')

def test_delete_file(azure_storage):
    azure_storage.filesystem.delete_file = Mock(return_value=True)
    result = azure_storage.delete_file('test.txt')
    assert result is True
    azure_storage.filesystem.delete_file.assert_called_once_with('test.txt')

def test_delete_file_error(azure_storage):
    azure_storage.filesystem.delete_file = Mock(side_effect=Exception("Test error"))
    with pytest.raises(Exception):
        azure_storage.delete_file('test.txt')

def test_delete_dir(azure_storage):
    azure_storage.filesystem.delete_dir = Mock(return_value=True)
    result = azure_storage.delete_dir('test_dir')
    assert result is True
    azure_storage.filesystem.delete_dir.assert_called_once_with('test_dir')

def test_delete_dir_error(azure_storage):
    azure_storage.filesystem.delete_dir = Mock(side_effect=Exception("Test error"))
    with pytest.raises(Exception):
        azure_storage.delete_dir('test_dir')

def test_get_file_info(azure_storage):
    mock_file_info = [pa_fs.FileInfo('test.txt', pa_fs.FileType.File)]
    azure_storage.filesystem.get_file_info = Mock(return_value=mock_file_info)
    result = azure_storage.get_file_info('test_dir')
    assert result == mock_file_info
    azure_storage.filesystem.get_file_info.assert_called_once()

def test_get_file_info_error(azure_storage):
    azure_storage.filesystem.get_file_info = Mock(side_effect=Exception("Test error"))
    with pytest.raises(Exception):
        azure_storage.get_file_info('test_dir') 