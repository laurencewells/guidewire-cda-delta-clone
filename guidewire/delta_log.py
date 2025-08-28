from time import sleep
from deltalake.transaction import AddAction, create_table_with_add_actions,CommitProperties
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import Schema
from deltalake import DeltaTable
import pyarrow as pa
from guidewire.logging import logger as L
from guidewire.storage import Storage
from typing import List, Dict, Optional, Union, Literal
import os

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
        else:
            self.log_uri = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/{table_name}/"
        self.table_name = table_name
        self.fs = Storage(cloud="azure")
        self.storage_options = self.fs._storage_options
        self.transaction_count = 0  # Track transactions for checkpointing
        self.checkpoint_interval = os.getenv("DELTA_LOG_CHECKPOINT_INTERVAL", 100)
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
                L.debug(f"Log does not exist for {self.table_name}: {e}")
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

    def _get_watermark_from_log(self) -> dict[str, int]:
        """Get the latest watermark from the Delta log.

        Returns:
            dict[str, int]: Dictionary containing the latest watermark and schema timestamp
        """
        if not self.table_exists():
            return {"watermark": 0, "schema_timestamp": 0}
        try:
            #get the commit properties from the latest transaction
            history = self.delta_log.history(1)[0]
            watermark = int(history["watermark"])
            schema_timestamp = int(history["schema_timestamp"])
            #TODO Add a test here
            if watermark is None:
                L.error(f"Watermark is None for {self.table_name}")
                return {"watermark": -1, "schema_timestamp": -1}
            return {"watermark": watermark, "schema_timestamp": schema_timestamp}
        except Exception as e:
            L.error(f"Error retrieving latest timestamp from log for {self.table_name}: {e}")
            return {"watermark": -1, "schema_timestamp": -1}

    def _create_checkpoint(self) -> bool:
        """Create a checkpoint for the Delta table to optimize log performance.
        
        Returns:
            bool: True if checkpoint was created successfully, False otherwise
        """
        if not self.table_exists():
            L.debug(f"Cannot create checkpoint - table {self.table_name} does not exist")
            return False
            
        try:
            L.debug(f"Creating checkpoint for table {self.table_name} at version {self.delta_log.version()}")
            self.delta_log.create_checkpoint()
            L.debug(f"Successfully created checkpoint for {self.table_name}")
            return True
        except Exception as e:
            L.warning(f"Failed to create checkpoint for {self.table_name}: {e}")
            return False

    def add_transaction(
        self, 
        parquets: List[Dict[str, Union[str, int]]], 
        schema: pa.Schema, 
        watermark: int,
        schema_timestamp: int, 
        mode: Literal["append", "overwrite"] = DEFAULT_MODE,
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

        if not self.table_exists():
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
            commit_properties = CommitProperties(custom_metadata={"watermark": str(watermark), "schema_timestamp": str(schema_timestamp)})
            if self.delta_log is None:
                L.debug(f"Creating new table: {self.table_name}")
                create_table_with_add_actions(
                    table_uri=self.log_uri,
                    schema=schema,
                    add_actions=actions,
                    mode="overwrite",
                    partition_by=[],
                    name=self.table_name,
                    storage_options=self.storage_options,
                    commit_properties= commit_properties

                )
            else:
                L.debug(f"Adding to table: {self.table_name} - watermark: {watermark}")
                
                self.delta_log.create_write_transaction(
                    actions=actions, mode=mode, schema=schema, partition_by=[],
                    commit_properties=commit_properties
                )
                #This update is optional as it only refreshes the delta log reference. Will cause warning on fail but stops azure failure bringing down the pipeline
                try:
                    self.delta_log.update_incremental()
                except:
                    L.warning(f"Failed to update delta log for {self.table_name} after transaction, sleeping for some time")
                    sleep(10)
                    
            # Increment transaction counter and check for checkpoint
            self.transaction_count += 1
            if self.transaction_count % self.checkpoint_interval == 0:
                L.debug(f"Reached {self.checkpoint_interval} transactions for {self.table_name}, creating checkpoint")
                self._create_checkpoint()
                
        except Exception as e:
            L.error(f"Failed to add transaction for {self.table_name}: {e}")
            raise DeltaError(f"Failed to add transaction: {e}")
