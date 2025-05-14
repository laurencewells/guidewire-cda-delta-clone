import pytest
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa
import pyarrow.compute as pc
from guidewire.delta_log import DeltaLog, DeltaError, DeltaValidationError

@pytest.fixture
def mock_storage():
    with patch('guidewire.delta_log.Storage') as mock:
        storage_instance = Mock()
        storage_instance._azure_account_key = "fake_key"
        storage_instance.read_parquet.return_value = None
        storage_instance.write_parquet.return_value = None
        storage_instance.delete_dir.return_value = True
        storage_instance.delete_file.return_value = True
        mock.return_value = storage_instance
        yield storage_instance

@pytest.fixture
def delta_log(mock_storage):
    return DeltaLog(
        storage_account="test_account",
        storage_container="test_container",
        table_name="test_table"
    )

def test_init_validation():
    with pytest.raises(DeltaValidationError):
        DeltaLog("", "container", "table")
    with pytest.raises(DeltaValidationError):
        DeltaLog("account", "", "table")
    with pytest.raises(DeltaValidationError):
        DeltaLog("account", "container", "")

def test_init_success(delta_log):
    assert delta_log.table_name == "test_table"
    assert delta_log.log_uri == "abfss://test_container@test_account.dfs.core.windows.net/test_table/"
    assert delta_log.check_point_path == "test_container/test_table/_checkpoints/log"

def test_table_exists(delta_log):
    # Test when table doesn't exist
    assert not delta_log.table_exists()
    
    # Test when table exists
    delta_log.delta_log = Mock()
    assert delta_log.table_exists()

def test_get_table_stats(delta_log):
    # Test when table doesn't exist
    with pytest.raises(DeltaError):
        delta_log.get_table_stats()
    
    # Test when table exists
    mock_delta_log = Mock()
    mock_delta_log.version.return_value = 1
    mock_delta_log.files.return_value = ["file1", "file2"]
    delta_log.delta_log = mock_delta_log
    
    stats = delta_log.get_table_stats()
    assert stats["version"] == 1
    assert stats["num_files"] == 2
    assert stats["table_uri"] == delta_log.log_uri

def test_write_checkpoint_validation(delta_log):
    with pytest.raises(DeltaValidationError):
        delta_log.write_checkpoint(None)
    with pytest.raises(DeltaValidationError):
        delta_log.write_checkpoint("not_an_int")

def test_write_checkpoint_success(delta_log, mock_storage):
    timestamp = 1234567890
    delta_log.write_checkpoint(timestamp)
    mock_storage.write_parquet.assert_called_once()

def test_get_latest_timestamp(delta_log, mock_storage):
    # Test when no checkpoint exists
    assert delta_log.get_latest_timestamp() == 0
    
    # Test when checkpoint exists
    mock_table = pa.table({"elt_timestamp": [100, 200, 300]})
    mock_storage.read_parquet.return_value = mock_table
    assert delta_log.get_latest_timestamp() == 300

def test_remove_log(delta_log, mock_storage):
    assert delta_log.remove_log()
    mock_storage.delete_dir.assert_called_once_with(path=delta_log.log_uri)

def test_remove_checkpoint(delta_log, mock_storage):
    assert delta_log.remove_checkpoint()
    mock_storage.delete_file.assert_called_once_with(path=delta_log.check_point_path)

def test_validate_parquet_info(delta_log):
    valid_info = {
        "path": "test.parquet",
        "size": 1000,
        "last_modified": 1234567890
    }
    delta_log._validate_parquet_info(valid_info)  # Should not raise
    
    # Test missing fields
    with pytest.raises(DeltaValidationError):
        delta_log._validate_parquet_info({"path": "test.parquet"})
    
    # Test invalid size
    with pytest.raises(DeltaValidationError):
        delta_log._validate_parquet_info({
            "path": "test.parquet",
            "size": -1,
            "last_modified": 1234567890
        })
    
    # Test invalid last_modified
    with pytest.raises(DeltaValidationError):
        delta_log._validate_parquet_info({
            "path": "test.parquet",
            "size": 1000,
            "last_modified": -1
        })

def test_add_transaction_validation(delta_log):
    schema = pa.schema([("col1", pa.int64())])
    
    # Test invalid mode
    with pytest.raises(DeltaValidationError):
        delta_log.add_transaction([], schema, mode="invalid_mode")
    
    # Test empty parquets
    with pytest.raises(DeltaValidationError):
        delta_log.add_transaction([], schema)

def test_add_transaction_success(delta_log):
    schema = pa.schema([("col1", pa.int64())])
    parquets = [{
        "path": "test.parquet",
        "size": 1000,
        "last_modified": 1234567890
    }]
    
    # Mock the delta_log instance
    delta_log.delta_log = Mock()
    
    # Test append mode
    delta_log.add_transaction(parquets, schema, mode="append")
    delta_log.delta_log.create_write_transaction.assert_called_once()
    
    # Test new table creation
    delta_log.delta_log = None
    with patch('guidewire.delta_log.i.write_new_deltalake') as mock_write:
        delta_log.add_transaction(parquets, schema, mode="overwrite")
        mock_write.assert_called_once() 