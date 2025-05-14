import pytest
import os
import json
from unittest.mock import Mock, patch
from guidewire.manifest import Manifest

@pytest.fixture
def mock_storage():
    """Fixture to create a mock Storage object."""
    with patch('guidewire.manifest.Storage') as mock:
        storage_instance = Mock()
        mock.return_value = storage_instance
        yield storage_instance

@pytest.fixture
def sample_manifest_data():
    """Fixture to provide sample manifest data."""
    return {
        "table1": [{"name": "table1", "schema": "public", "columns": ["id", "name"]}],
        "table2": [{"name": "table2", "schema": "public", "columns": ["id", "value"]}]
    }

def test_manifest_initialization_valid(mock_storage, sample_manifest_data, tmp_path):
    """Test successful initialization of Manifest with valid inputs."""
    # Create a temporary manifest file
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(sample_manifest_data, f)
    
    mock_storage.read_json.return_value = sample_manifest_data
    
    manifest = Manifest(str(tmp_path), ["table1", "table2"])
    
    assert manifest.is_initialized()
    assert manifest.manifest == sample_manifest_data
    assert manifest.table_names == ["table1", "table2"]
    assert manifest.location == str(tmp_path)

def test_manifest_initialization_empty_location():
    """Test initialization with empty location."""
    with pytest.raises(ValueError, match="Location cannot be empty"):
        Manifest("", ["table1"])

def test_manifest_initialization_empty_table_names():
    """Test initialization with empty table names."""
    with pytest.raises(ValueError, match="Table names cannot be empty"):
        Manifest("/path/to/manifest", [])

def test_manifest_file_not_found(mock_storage, tmp_path):
    """Test handling of missing manifest file."""
    mock_storage.read_json.side_effect = FileNotFoundError()
    
    with pytest.raises(FileNotFoundError):
        Manifest(str(tmp_path), ["table1"])

def test_manifest_invalid_format(mock_storage, tmp_path):
    """Test handling of invalid manifest format."""
    mock_storage.read_json.return_value = "invalid"  # Not a dictionary
    
    with pytest.raises(ValueError, match="Manifest file must contain a dictionary"):
        Manifest(str(tmp_path), ["table1"])

def test_read_valid_entry(mock_storage, sample_manifest_data, tmp_path):
    """Test reading a valid entry from the manifest."""
    mock_storage.read_json.return_value = sample_manifest_data
    
    manifest = Manifest(str(tmp_path), ["table1"])
    entry = manifest.read("table1")
    
    assert entry is not None
    assert entry["name"] == "table1"
    assert entry["schema"] == "public"
    assert entry["columns"] == ["id", "name"]
    assert entry["entry"] == "table1"  # Check that entry name is added

def test_read_nonexistent_entry(mock_storage, sample_manifest_data, tmp_path):
    """Test reading a non-existent entry."""
    mock_storage.read_json.return_value = sample_manifest_data
    
    manifest = Manifest(str(tmp_path), ["table1"])
    entry = manifest.read("nonexistent")
    
    assert entry is None


def test_filtered_table_names(mock_storage, sample_manifest_data, tmp_path):
    """Test that only requested table names are loaded."""
    mock_storage.read_json.return_value = sample_manifest_data
    
    manifest = Manifest(str(tmp_path), ["table1"])
    
    assert "table1" in manifest.manifest
    assert "table2" not in manifest.manifest 