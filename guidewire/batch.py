import pyarrow as pa
from pyarrow.fs import FileType
from guidewire.logging import logger as L
from guidewire.delta_log import DeltaLog
from guidewire.manifest import Manifest
from typing import Optional

class Batch:
    def __init__(
        self,
        table_name: str,
        manifest: Manifest,
        storage_account: str,
        storage_container: str,
        reset: bool = False,
        subfolder: Optional[str] = None,
    ):
        """Initialize a new Batch instance.
        
        Args:
            table_name: Name of the table to process
            manifest: Manifest object containing file information
            storage_account: Azure storage account name
            storage_container: Azure storage container name
            reset: Whether to reset the processing state
            subfolder: Optional subfolder to process
        Raises:
            ValueError: If required parameters are invalid
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")
        if not storage_account or not isinstance(storage_account, str):
            raise ValueError("storage_account must be a non-empty string")
        if not storage_container or not isinstance(storage_container, str):
            raise ValueError("storage_container must be a non-empty string")
            
        self.table_name = table_name
        self.manifest = manifest
        self.entry = self.manifest.read(entry=self.table_name)
        self.cached_schema = None
        self.log_entry = DeltaLog(
            storage_account=storage_account,
            storage_container=storage_container,
            table_name=self.table_name,
            subfolder=subfolder,
        )
        self.watermark_info = self.log_entry._get_watermark_from_log()
        self.low_watermark = 0 if reset else self.watermark_info["watermark"]
        self.watermark_schema_timestamp = 0 if reset else self.watermark_info["schema_timestamp"]
        if reset:
            self.log_entry.remove_log()

    def _schema_finder(self, file_list: list[dict[str, str | int]]) -> bool:
        """Attempts to find and cache the schema from a list of files.
        
        Args:
            file_list: List of dictionaries containing file metadata with keys:
                      'relative_path', 'path', 'last_modified', 'size'
        
        Returns:
            bool: True if schema was successfully found and cached, False otherwise
        """
        self.cached_schema = None
        file_list.sort(key=lambda x: x["size"])
        L.info(f"  Found {len(file_list)} potential schema files.")

        for schema_file_info in file_list:
            file_path_to_try = schema_file_info["relative_path"]
            L.info(f"Attempting to read schema from: {file_path_to_try}")
            try:
                self.cached_schema = self._get_parquet_schema(file_path_to_try)
                L.info(
                    f"Successfully determined schema for '{self.table_name}' using file: {file_path_to_try}"
                )
                return True
            except Exception as e:
                L.warning(f"    Failed to read schema from {file_path_to_try}: {e}")
        return False

    def _get_dir_list(self, directory: str) -> tuple[bool, list[str]]:
        """
        Returns (is_part_way, directory_list).
        is_part_way = True if part of the schema has been processed (i.e., only some dirs meet watermark criteria),
        is_part_way = False if all or none meet criteria.
        directory_list is always sorted.
        """
        # Get all directory paths within the given directory, sorted
        
        listed_paths = self.manifest.fs.get_file_info(directory)
        full_list = sorted(
            path.path
            for path in listed_paths
            if path.type == FileType.Directory
        )

        # Filter for directories with base_name greater than the low watermark
        part_list = sorted(
            path.path 
            for path in listed_paths 
            if path.type == FileType.Directory
            and int(path.base_name) > self.low_watermark
        )

        if not part_list:
            L.info(
                f"No directories found in {directory} greater than low watermark {self.low_watermark}"
            )
            return True, []

        if 0 < len(part_list) < len(full_list):
            L.info(
                f"Filtered directories in {directory} to those with timestamps greater than low watermark {self.low_watermark}"
            )
            return True, part_list

        # All present (or none filtered out)
        L.info(
            f"All directories in {directory} are greater than low watermark {self.low_watermark} (or none filtered out)"
        )
        return False, full_list


    def _get_parquet_list(self, directory: str) -> list[dict]:
        """Returns a list of parquet files with metadata from the given directory."""
        return [
            {
                "relative_path": file.path,
                "path": f"s3://{file.path}",
                "last_modified": file.mtime_ns,
                "size": file.size,
            }
            for file in self.manifest.fs.get_file_info(directory)
            if file.type == FileType.File and file.path.endswith(".parquet")
        ]

    def _get_parquet_schema(self, path: str) -> pa.schema:
        """Reads and returns the schema from a parquet file.
        
        Args:
            path: Path to the parquet file
            
        Returns:
            pa.schema: PyArrow schema object
            
        Raises:
            Exception: If the parquet file cannot be read or is invalid
        """
        try:
            table = self.manifest.fs.read_parquet(path)
            if table is None or table.schema is None:
                raise ValueError(f"Invalid parquet file at {path}: no schema found")
            return table.schema
        except Exception as e:
            L.error(f"Failed to read parquet schema from {path}: {str(e)}")
            raise

    def _process_schema_history(self, item: dict) -> None:
        """Processes a single schema history item."""
        folder = item["uri"]
        schema_timestamp = item["schema_timestamp"]
        try:
            partial,timestamp_folders = self._get_dir_list(folder)
        except Exception as e:
            L.warning(f"Failed to list contents of {folder}: {e}")
            raise
        if partial:
            L.info(f"  Found partial schema history in {folder}, processing only new timestamps.")
            

        first_folder_for_schema = True
        for timestamp_folder in timestamp_folders:
            try:
                timestamp_value = int(timestamp_folder.split("/")[-1])
            except ValueError:
                L.warning(f"Skipping non-numeric timestamp folder: {timestamp_folder}")
                continue
            L.info(f"  Checking timestamp path: {timestamp_folder}")
            try:
                files_in_timestamp = self._get_parquet_list(timestamp_folder)
            except Exception as e:
                L.error(f"  Failed to list contents of {timestamp_folder}: {e}")
                continue
            
            #could use the full list lets see

            if first_folder_for_schema:
                if self._schema_finder(files_in_timestamp):
                    first_folder_for_schema = False
                    self.log_entry.add_transaction(
                        parquets=files_in_timestamp,
                        schema=self.cached_schema,
                        watermark=timestamp_value,
                        schema_timestamp=schema_timestamp,
                        mode="overwrite" if not partial else "append",
                    )
                else:
                    L.error(f"Schema not found for '{self.table_name} {folder}'")
                    raise
            else:
                self.log_entry.add_transaction(
                    parquets=files_in_timestamp,
                    schema=self.cached_schema,
                    watermark=timestamp_value,
                    schema_timestamp=schema_timestamp,
                    mode="append",
                )

    def process_batch(self):
        """Processes the batch for the current table."""
        if self.low_watermark == -1:
            L.error(
                f"Skipping batch for {self.table_name} as the low watermark is -1, indicating somethings gone wrong."
            )
            return
        if int(self.entry["lastSuccessfulWriteTimestamp"]) <= self.low_watermark:
            L.warning(
                f"Skipping batch for {self.table_name} as it matches or is older than the low watermark."
            )
            return

        L.info(f"Processing batch for {self.table_name}")
        filepath = self.entry["dataFilesPath"].lstrip("s3://")
        schema_history = self.entry["schemaHistory"]
        

        if not filepath or not schema_history:
            L.error(
                f"Missing 'dataFilesPath' or 'schemaHistory' for entry {self.table_name}"
            )
            return

        # Uses sorted to ensure the schema history is processed in order
        # makes sure to only process schema history entries that are greater than the low watermark
        # schema timestamp cannot be used here as its lower that the timestamp folders inside, its the orginal schema change time
        # need to sort the schemas by the value not the key and take higher or equal than the self.watermark_schema_timestamp
        sorted_schema_history = sorted(
            (item for item in schema_history.items() if int(item[1]) >= self.watermark_schema_timestamp),
            key=lambda kv: int(kv[1])
        )

        schema_history_list= [
            {
                "key": key,
                "uri": f"{filepath.rstrip('/')}/{key}/",
                "schema_timestamp": int(value)
            }
            for key, value in sorted_schema_history
        ]
        
        try:
            for item in schema_history_list:
                L.info(f"Processing URI: {item['uri']} for entry {self.table_name}")
                self._process_schema_history(item)
            #self.log_entry.write_checkpoint(int(self.entry["lastSuccessfulWriteTimestamp"]))
        except Exception as e:
            L.error(f"Error processing schema history for {self.table_name}: {e} processing abandoned")
            raise

