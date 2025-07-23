from deltalake.transaction import AddAction, create_table_with_add_actions
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import Schema
from deltalake import DeltaTable
import pyarrow as pa
import pyarrow.compute as pc
from guidewire.logging import logger as L
from guidewire.storage import Storage
from typing import List, Dict, Optional, Union, Literal


class DeltaError(Exception):
    """Base exception class for Delta-related errors."""
    pass


class DeltaValidationError(DeltaError):
    """Exception raised for validation errors in Delta operations."""
    pass


class DeltaLog:
    """A class to manage Delta Lake transaction logs and checkpoints.
    
    This class provides functionality to write and read checkpoints, manage transaction logs,
    and handle Delta Lake table operations.
    """
    
    # Class constants
    DEFAULT_MODE = "append"
    CHECKPOINT_DIR = "_checkpoints/log"
    VALID_MODES = ("append", "overwrite")
    
    def __init__(
        self,
        storage_account: str,
        storage_container: str,
        table_name: str,
        subfolder: Optional[str] = None,
    ) -> None:
        """Initialize the DeltaLog instance.
        
        Args:
            storage_account: The Azure storage account name
            storage_container: The storage container name
            table_name: The name of the Delta table
            
        Raises:
            DeltaValidationError: If any of the required parameters are empty or invalid
        """
        if not all([storage_account, storage_container, table_name]):
            raise DeltaValidationError("All parameters must be non-empty strings")
            
        self.delta_log: Optional[DeltaTable] = None
        #add optionalsubfoler 
        self.subfolder = subfolder
        if subfolder:
            self.log_uri = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/{subfolder}/{table_name}/"
            self.check_point_path = f"{storage_container}/{subfolder}/{table_name}/{self.CHECKPOINT_DIR}"
        else:
            self.log_uri = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/{table_name}/"
            self.check_point_path = f"{storage_container}/{table_name}/{self.CHECKPOINT_DIR}"
        self.table_name = table_name
        self.fs = Storage(cloud="azure")
        self.storage_options = self.fs._storage_options
        self._log_exists()

    def _log_exists(self) -> None:
        """Check if the Delta log exists and initialize it if found."""
        try:
            self.delta_log = DeltaTable(
                table_uri=self.log_uri, storage_options=self.storage_options
            )
        except Exception as e:
            #If its a file not found error, that is ok
            if isinstance(e, TableNotFoundError):
                L.info(f"Log does not exist for {self.table_name}: {e}")
            else:
                L.error(f"Error reading log for {self.table_name}: {e}")
                raise DeltaError(f"Error reading log: {e}")

    def table_exists(self) -> bool:
        """Check if the Delta table exists.
        
        Returns:
            bool: True if the table exists, False otherwise
        """
        return self.delta_log is not None

    def get_table_stats(self) -> Dict[str, Union[int, str]]:
        """Get basic statistics about the Delta table.
        
        Returns:
            Dict[str, Union[int, str]]: Dictionary containing table statistics
            
        Raises:
            DeltaError: If the table doesn't exist or stats can't be retrieved
        """
        if not self.table_exists():
            raise DeltaError(f"Table {self.table_name} does not exist")
            
        try:
            return {
                "version": self.delta_log.version(),
                "num_files": len(self.delta_log.files()),
                "table_uri": self.log_uri
            }
        except Exception as e:
            raise DeltaError(f"Failed to get table stats: {e}")

    def write_checkpoint(self, timestamp: int) -> None:
        """Write a checkpoint with the given timestamp.
        
        Args:
            timestamp: The timestamp to write to the checkpoint
            
        Raises:
            DeltaValidationError: If timestamp is not provided or is not an integer
            DeltaError: If writing the checkpoint fails
        """
        if not timestamp:
            L.error("Timestamp is required to write checkpoint")
            raise DeltaValidationError("Timestamp is required to write checkpoint")
        if not isinstance(timestamp, int):
            L.error("Timestamp must be an integer")
            raise DeltaValidationError("Timestamp must be an integer")
            
        log = {"elt_timestamp": [timestamp]}

        current_table = self._read_checkpoint()
        try:
            if not current_table:
                self.fs.write_parquet(self.check_point_path, pa.table(log))
            else:
                final_table = pa.concat_tables([current_table, pa.table(log)])
                self.fs.write_parquet(self.check_point_path, final_table)
        except Exception as e:
            L.error(f"Failed to write checkpoint for {self.table_name}: {e}")
            raise DeltaError(f"Failed to write checkpoint: {e}")

    def _read_checkpoint(self) -> Optional[pa.Table]:
        """Read the checkpoint file if it exists.
        
        Returns:
            Optional[pa.Table]: The checkpoint data as a PyArrow table, or None if not found
        """
        try:
            return self.fs.read_parquet(self.check_point_path)
        except FileNotFoundError:
            L.info(f"Checkpoint does not exist for {self.table_name}")
            return None
        except Exception as e:
            L.error(f"Error reading checkpoint for {self.table_name}: {e}")
            return None

    def get_latest_timestamp(self) -> int:
        """Get the latest timestamp from the checkpoint.
        
        Returns:
            int: The latest timestamp, or 0 if no checkpoint exists
        """
        #fail if there is an error reading the checkpoint
        try:
            checkpoint = self._read_checkpoint()
        except Exception as e:
            L.error(f"Error reading checkpoint for {self.table_name}: {e}")
            raise DeltaError(f"Error reading checkpoint: {e}")
        if not checkpoint:
            return 0
        try:
            return pc.max(checkpoint["elt_timestamp"]).as_py()
        except Exception as e:
            L.error(f"Error retrieving latest timestamp for {self.table_name}: {e}")
            return 0

    def remove_log(self) -> bool:
        """Remove the Delta log.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.fs.delete_dir(path=self.log_uri)
            return True
        except Exception as e:
            L.error(f"Failed to remove log for {self.table_name}: {e}")
            return False

    def remove_checkpoint(self) -> bool:
        """Remove the checkpoint file.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.fs.delete_file(path=self.check_point_path)
            return True
        except Exception as e:
            L.error(f"Failed to remove checkpoint for {self.table_name}: {e}")
            return False

    def _validate_parquet_info(self, parquet: Dict[str, Union[str, int]]) -> None:
        """Validate parquet file information.
        
        Args:
            parquet: Dictionary containing parquet file information
            
        Raises:
            DeltaValidationError: If required fields are missing or invalid
        """
        required_fields = {"path", "size", "last_modified"}
        if not all(field in parquet for field in required_fields):
            raise DeltaValidationError(f"Parquet info must contain fields: {required_fields}")
            
        if not isinstance(parquet["size"], int) or parquet["size"] <= 0:
            raise DeltaValidationError("Parquet size must be a positive integer")
            
        if not isinstance(parquet["last_modified"], int) or parquet["last_modified"] <= 0:
            raise DeltaValidationError("Last modified timestamp must be a positive integer")

    def add_transaction(
        self, 
        parquets: List[Dict[str, Union[str, int]]], 
        schema: pa.Schema, 
        mode: Literal["append", "overwrite"] = DEFAULT_MODE
    ) -> None:
        """Add a transaction to the Delta log.
        
        Args:
            parquets: List of dictionaries containing parquet file information
            schema: The PyArrow schema for the data
            mode: The write mode ("append" or "overwrite")
            
        Raises:
            DeltaValidationError: If parquet information is invalid or mode is invalid
            DeltaError: If adding the transaction fails
        """
        if mode not in self.VALID_MODES:
            raise DeltaValidationError(f"Mode must be one of {self.VALID_MODES}")
            
        if not parquets:
            raise DeltaValidationError("At least one parquet file must be provided")
            
        self._log_exists()
        actions = []
        for file in parquets:
            self._validate_parquet_info(file)
            actions.append(
                AddAction(
                    path=file["path"],
                    size=file["size"],
                    partition_values={},
                    modification_time=file["last_modified"],
                    data_change=False,
                    stats="{}",
                )
            )

        try:
            schema = Schema.from_arrow(schema)
            if self.delta_log is None:
                L.info("Creating new table")
                create_table_with_add_actions(
                    table_uri=self.log_uri,
                    schema=schema,
                    add_actions=actions,
                    mode="overwrite",
                    partition_by=[],
                    name=self.table_name,
                    storage_options=self.storage_options,
                )
            else:
                L.info("Adding to table")
                self.delta_log.create_write_transaction(
                    add_actions=actions, mode=mode, schema=schema, partition_by=[]
                )
        except Exception as e:
            L.error(f"Failed to add transaction for {self.table_name}: {e}")
            raise DeltaError(f"Failed to add transaction: {e}")
