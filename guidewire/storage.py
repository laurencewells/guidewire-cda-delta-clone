import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pyarrow.json as pj
from guidewire.logging import logger as L
import os
from typing import Literal, List, Dict, Any


class Storage:
    """A class to handle cloud storage operations using PyArrow filesystem interface.
    
    Supports Azure Blob Storage and AWS S3 as storage backends.
    """
    
    def __init__(self, cloud: Literal["azure", "aws"]):
        """Initialize the storage client for the specified cloud provider.
        
        Args:
            cloud: The cloud provider to use ("azure" or "aws")
            
        Raises:
            ValueError: If an invalid cloud provider is specified
            KeyError: If required environment variables are missing
        """
        if cloud == "azure":
            account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
            account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
            tenant_id = os.environ.get("AZURE_TENANT_ID")
            client_id = os.environ.get("AZURE_CLIENT_ID")
            client_secret = os.environ.get("AZURE_CLIENT_SECRET")
            
            if not account_name:
                raise KeyError("AZURE_STORAGE_ACCOUNT_NAME must be set")
            if client_id and client_secret and tenant_id:
                L.info("Using Client ID and Client Secret for Azure storage")
                self._storage_options = {
                    "account_name": account_name,
                    "tenant_id": tenant_id,
                    "client_id": client_id,
                    "client_secret": client_secret,
                }
                self.filesystem = pa_fs.AzureFileSystem(
                    account_name=self._storage_options["account_name"],
                )
            elif account_key:
                L.info("Using Account Key for Azure storage")
                self._storage_options = {
                    "account_name": account_name,
                    "account_key": account_key,
                }
                self.filesystem = pa_fs.AzureFileSystem(
                    account_name=self._storage_options["account_name"],
                    account_key=self._storage_options["account_key"],
                )
            else:
                L.error("Azure storage credentials must be set")
                raise KeyError("Azure storage credentials must be set")
                
        elif cloud == "aws":
            region = os.environ.get("AWS_REGION")
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            endpoint = os.environ.get("AWS_ENDPOINT_URL")
            
            if not all([region, access_key, secret_key]):
                raise KeyError("AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY must be set")
            if endpoint:
                L.info(f"Using custom endpoint {endpoint} for AWS S3")
                self.filesystem = pa_fs.S3FileSystem(
                    region=region,
                    access_key=access_key,
                    secret_key=secret_key,
                    endpoint_override=endpoint,
                )  
            else:
                L.info("Using default AWS S3 endpoint")
                self.filesystem = pa_fs.S3FileSystem(
                    region=region,
                    access_key=access_key,
                    secret_key=secret_key,
                )

        else:
            raise ValueError(f"Invalid cloud provider: {cloud}")

    def read_parquet(self, path: str) -> pa.Table:
        """Read a Parquet file from the storage.
        
        Args:
            path: Path to the Parquet file
            
        Returns:
            PyArrow Table containing the data
            
        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        try:
            return pq.read_table(source=path, filesystem=self.filesystem)
        except Exception as e:
            L.warning(f"Failed to read parquet file {path}: {str(e)}")
            raise
    
    def write_parquet(self, path: str, table: pa.Table) -> None:
        """Write a PyArrow Table to Parquet format in storage.
        
        Args:
            path: Destination path for the Parquet file
            table: PyArrow Table to write
            
        Raises:
            IOError: If writing fails
        """
        try:
            pq.write_table(filesystem=self.filesystem, table=table, where=path)
        except Exception as e:
            L.error(f"Failed to write parquet file {path}: {str(e)}")
            raise
    
    def read_json(self, path: str) -> Dict[str, Any]:
        """Read a JSON file from storage.
        
        Args:
            path: Path to the JSON file
            
        Returns:
            Dictionary containing the JSON data
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the JSON is invalid
        """
        try:
            return pj.read_json(self.filesystem.open_input_stream(path)).to_pydict()
        except Exception as e:
            L.error(f"Failed to read JSON file {path}: {str(e)}")
            raise

    def list_files(self, path: str) -> List[str]:
        """List files in a directory.
        
        Args:
            path: Directory path to list
            
        Returns:
            List of file paths
            
        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        try:
            return self.filesystem.ls(path)
        except Exception as e:
            L.error(f"Failed to list files in {path}: {str(e)}")
            raise
    
    def delete_file(self, path: str) -> bool:
        """Delete a file from storage.
        
        Args:
            path: Path to the file to delete
            
        Returns:
            True if deletion was successful
            
        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        try:
            return self.filesystem.delete_file(path)
        except Exception as e:
            L.error(f"Failed to delete file {path}: {str(e)}")
            raise
    
    def delete_dir(self, path: str) -> bool:
        """Delete a directory from storage.
        
        Args:
            path: Path to the directory to delete
            
        Returns:
            True if deletion was successful
            
        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        try:
            return self.filesystem.delete_dir(path)
        except Exception as e:
            L.error(f"Failed to delete directory {path}: {str(e)}")
            raise
    
    def get_file_info(self, path: str) -> List[pa_fs.FileInfo]:
        """Get information about files in a directory.
        
        Args:
            path: Directory path to get info for
            
        Returns:
            List of FileInfo objects
            
        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        try:
            selector = pa.fs.FileSelector(path)
            return self.filesystem.get_file_info(selector)
        except Exception as e:
            L.error(f"Failed to get file info for {path}: {str(e)}")
            raise


