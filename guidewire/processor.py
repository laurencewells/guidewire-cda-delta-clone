from typing import Tuple
import os
import ray
from guidewire.manifest import Manifest
from guidewire.batch import Batch
from guidewire.logging import logger as L


class Processor:
    """A class to handle table processing operations."""
    
    def __init__(self, table_names: Tuple[str, ...] = None, parallel: bool = True):
        """I
        Initialize the Processor with table names and parallel processing.
        if table_names is not provided, all tables in the manifest will be processed.
        if parallel is False, the tables will be processed sequentially.
        if parallel is True, the tables will be processed in parallel using Ray.
        
        Args:
            table_names: Tuple of table names to process
            parallel: Whether to process tables in parallel using Ray (default: True)
        """
        self.table_names = table_names
        self.parallel = parallel
        self._validate_environment()
        self.log_storage_account = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
        self.log_storage_container = os.environ["AZURE_STORAGE_ACCOUNT_CONTAINER"]
        self.manifest_location = os.environ["AWS_MANIFEST_LOCATION"]
        self.manifest = Manifest(
            location=self.manifest_location,
            table_names=self.table_names
        )
        if self.table_names is None:
            self.table_names = self.manifest.get_table_names()
        if self.table_names is None:
            raise ValueError("Table names must be provided")

    @staticmethod
    def _validate_environment() -> None:
        """Validate that all required environment variables are set."""
        required_vars = {
            "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
            "AZURE_STORAGE_ACCOUNT_CONTAINER": os.environ.get("AZURE_STORAGE_ACCOUNT_CONTAINER"),
            "AWS_MANIFEST_LOCATION": os.environ.get("AWS_MANIFEST_LOCATION")
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")


    @staticmethod
    @ray.remote
    def process_table_async(entry: str, manifest: Manifest, log_storage_account: str, log_storage_container: str) -> None:
        """
        Process a single table entry using Ray distributed computing.
        
        Args:
            entry: The table name to process
            manifest: The manifest object containing table information
            log_storage_account: Azure storage account name
            log_storage_container: Azure storage container name
        """
        try:
            L.info(f"Processing table: {entry}")
            manifest_entry = manifest.read(entry)
            if manifest_entry:
                batch = Batch(
                    table_name=entry,
                    manifest=manifest,
                    storage_account=log_storage_account,
                    storage_container=log_storage_container,
                ).process_batch()
                L.info(f"Successfully processed table: {entry}")
            else:
                L.warning(f"No manifest entry found for table: {entry}")
        except Exception as e:
            L.error(f"Error processing table {entry}: {str(e)}")
            raise
        
    @staticmethod
    def process_table(entry: str, manifest: Manifest, log_storage_account: str, log_storage_container: str) -> None:
        """
        Process a single table entry using Ray distributed computing.
        
        Args:
            entry: The table name to process
            manifest: The manifest object containing table information
            log_storage_account: Azure storage account name
            log_storage_container: Azure storage container name
        """
        try:
            L.info(f"Processing table: {entry}")
            manifest_entry = manifest.read(entry)
            if manifest_entry:
                batch = Batch(
                    table_name=entry,
                    manifest=manifest,
                    storage_account=log_storage_account,
                    storage_container=log_storage_container,
                ).process_batch()
                L.info(f"Successfully processed table: {entry}")
            else:
                L.warning(f"No manifest entry found for table: {entry}")
        except Exception as e:
            L.error(f"Error processing table {entry}: {str(e)}")
            raise
    

    def run(self) -> None:
        """Execute the table processing workflow."""
        try:
            if self.parallel:
                # Initialize Ray for parallel processing
                ray.init(ignore_reinit_error=True, log_to_driver=True)
                # Process tables in parallel
                futures = [
                    self.process_table_async.remote(entry, self.manifest, self.log_storage_account, self.log_storage_container)
                    for entry in self.table_names
                ]
                # Wait for all tasks to complete
                ray.get(futures)
                ray.shutdown()
            else:
                # Process tables sequentially
                for entry in self.table_names:
                    self.process_table(entry, self.manifest, self.log_storage_account, self.log_storage_container)
            L.info("Processed successfully")
        except Exception as e:
            L.error(f"Application error: {str(e)}")
            raise
