"""
Table Manager Module for HCF Bot
Provides reusable batch operations for Google Sheets tables with:
- Automatic batch size management (50KB chunks)
- Exponential backoff retry logic for rate limits
- Thread-safe API access with global lock
- Insert at top, update existing rows
- Works with any table structure
"""
import logging
import time
import sys
import threading
import asyncio
from datetime import datetime
from config import config, localnow
from google_sheets_connection import sheet

logger = logging.getLogger(config.bot_name)

# Global lock for Google Sheets API access
# Ensures only one operation accesses the API at a time
_api_lock = threading.RLock()  # Reentrant lock allows same thread to acquire multiple times

# Global ASYNC lock for trips workflow (read-modify-write cycles)
# Prevents concurrent async trips processing that could cause row misalignment
_trips_workflow_async_lock = asyncio.Lock()


class TripsWorkflowLock:
    """
    Async context manager for trips workflow lock
    Prevents concurrent async operations on Trips table
    """
    async def __aenter__(self):
        await _trips_workflow_async_lock.acquire()
        logger.debug("Acquired trips workflow lock")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        _trips_workflow_async_lock.release()
        logger.debug("Released trips workflow lock")
        return False


def acquire_trips_workflow_lock():
    """Acquire the global trips workflow lock (async-aware)
    
    Call this at the start of any async operation that:
    1. Reads the Trips table
    2. Processes data
    3. Writes back to the Trips table
    
    Use with async context manager:
        async with acquire_trips_workflow_lock():
            # Your read-modify-write workflow
            await asyncio.sleep(5)  # Can safely await
    """
    return TripsWorkflowLock()


class TableManager:
    """
    Generic table manager for batch operations on Google Sheets tables
    Handles rate limiting, batching, and efficient updates
    """
    
    # Constants
    MAX_BATCH_SIZE = 50000  # 50KB per batch
    INITIAL_RETRY_DELAY = 10  # seconds
    MAX_RETRY_DELAY = 640  # seconds (10m 40s)
    
    def __init__(self, sheet_name, required_columns, save_formula_columns=None):
        """
        Initialize TableManager
        
        Args:
            sheet_name: Name of the worksheet (e.g., "Drivers_Sheet")
            required_columns: List of column names that MUST exist in the table
                            (order doesn't matter - actual order read from sheet)
            save_formula_columns: Optional list of column names whose formulas should be 
                                preserved and restored. If None or empty, formulas won't
                                be saved. Only formulas in these columns will be protected.
                                Example: ['O', 'Status', 'NotificationMessage']
        """
        self.sheet_name = sheet_name
        self.required_columns = required_columns
        self.save_formula_columns = save_formula_columns or []
        self.worksheet = None
        self.column_map = {}
        self.actual_columns = []  # Will be populated from sheet headers
        self.formula_templates = {}  # Maps column_name -> formula from row 2
        self.retry_delay = self.INITIAL_RETRY_DELAY
        
        # State tracking - updated by refresh_table()
        self.records = []           # List of dicts (data rows, no header)
        self.num_cols = 0           # len(actual_columns)
        self.num_data_rows = 0      # len(records)
        
    def _get_worksheet(self):
        """Get the worksheet with retry logic for server errors.
        
        BLOCKING — acquires _api_lock and calls the Google Sheets API synchronously.
        Must NOT be called directly from an async function running on the event loop
        thread; use _get_worksheet_async() instead, which runs this in an executor.
        """
        try:
            logger.info(f"Google-Spreadsheet-API: Get worksheet - {self.sheet_name} - Opening worksheet")
            self.worksheet = self._api_call_with_retry(
                sheet.worksheet,
                self.sheet_name
            )
            logger.info(f"Found worksheet: {self.sheet_name}")
            return self.worksheet
        except Exception as e:
            logger.error(f"Could not find worksheet '{self.sheet_name}': {e}")
            raise

    async def _get_worksheet_async(self):
        """Async wrapper for _get_worksheet() — safe to call from the event loop.
        
        Runs _get_worksheet() in a thread-pool executor so the event loop (and
        therefore the Discord heartbeat) is never blocked while waiting for the
        Google Sheets API.  Always prefer this over _get_worksheet() when inside
        an async function.
        """
        import asyncio
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._get_worksheet)

    async def _api_call_with_retry_async(self, api_func, *args, **kwargs):
        """Async wrapper for _api_call_with_retry() — safe to call from the event loop.
        
        Runs _api_call_with_retry() in a thread-pool executor so the event loop
        (and therefore the Discord heartbeat) is never blocked.  _api_call_with_retry
        itself spawns an inner ThreadPoolExecutor for the per-call 60-second timeout;
        that inner executor is independent and does not cause double-blocking.

        Always prefer this over _api_call_with_retry() when inside an async function.
        
        Usage pattern in async code:
            # WRONG — blocks the event loop:
            result = self._api_call_with_retry(self.worksheet.batch_get, ...)
            
            # RIGHT — offloads to executor, event loop stays free:
            result = await self._api_call_with_retry_async(self.worksheet.batch_get, ...)
        """
        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._api_call_with_retry(api_func, *args, **kwargs)
        )
    
    def _build_column_map(self, force_refresh=False):
        """
        Read headers and build column name -> index mapping
        Also populates actual_columns list with the actual column order from the sheet
        
        Args:
            force_refresh: If True, rebuild even if already exists
            
        Returns: dict mapping column names to 1-based column numbers
        """
        try:
            logger.info(f"Google-Spreadsheet-API: Read row 1 - {self.sheet_name} - Reading column headers")
            headers = self._api_call_with_retry(
                self.worksheet.row_values,
                1
            )
            
            column_map = {}
            actual_columns = []
            duplicates = []
            
            for idx, header in enumerate(headers, start=1):
                if not header:
                    # Empty header - stop processing (ignore columns to the right)
                    break
                
                # Check for duplicate
                if header in column_map:
                    duplicates.append(header)
                else:
                    column_map[header] = idx
                    actual_columns.append(header)
            
            # Raise error if duplicates found
            if duplicates:
                raise ValueError(f"Duplicate column names found: {', '.join(duplicates)}")
            
            # Check if columns have changed
            if self.actual_columns and self.actual_columns != actual_columns:
                logger.info(f"Column order changed! Old: {self.actual_columns}")
                logger.info(f"                      New: {actual_columns}")
            
            # Store the actual column order from the sheet
            self.actual_columns = actual_columns
            
            logger.info(f"Read {len(actual_columns)} columns from sheet: {actual_columns}")
            
            # Verify required columns exist
            missing = [col for col in self.required_columns if col not in column_map]
            if missing:
                raise ValueError(f"Missing required columns: {', '.join(missing)}")
            
            # Read formula templates from row 2 (first data row)
            # These will be used to restore formulas after updates
            self._read_formula_templates()
            
            return column_map
        except Exception as e:
            logger.error(f"Failed to build column map: {e}")
            raise
    
    def refresh_table(self):
        """
        Refresh complete table state: columns, formulas, and all data
        
        Updates:
        - column_map, actual_columns (from headers)
        - formula_templates (from row 2)
        - records (all data rows)
        - num_cols, num_data_rows
        
        Call this at the start of a workflow to get fresh table state.
        Clears all cached state before reading.
        """
        # Refresh column information (this also clears cache)
        self.refresh_column_info()
        
        # Read all table data fresh from sheet
        self.records = self.read_all_rows_unformatted()
        
        # Update counts
        self.num_cols = len(self.actual_columns)
        self.num_data_rows = len(self.records)
        
        logger.info(f"Refreshed table state: {self.num_data_rows} data rows × {self.num_cols} columns")
    
    async def refresh_table_async(self):
        """
        Async version of refresh_table() that runs blocking API calls in executor
        
        Prevents Discord heartbeat blocking for large tables.
        Use this instead of refresh_table() when called from async context.
        """
        import asyncio
        loop = asyncio.get_running_loop()
        
        # Run the blocking refresh_table() in executor to avoid blocking event loop
        await loop.run_in_executor(None, self.refresh_table)
    
    def _read_formula_templates(self):
        """
        Read formulas from row 2 (first data row) to use as templates
        Only saves formulas for columns specified in save_formula_columns
        """
        # Skip if no columns to save
        if not self.save_formula_columns:
            logger.debug("No formula columns to save (save_formula_columns is empty)")
            self.formula_templates = {}
            return
        
        # Validate save_formula_columns against actual columns
        invalid_columns = [col for col in self.save_formula_columns if col not in self.actual_columns]
        if invalid_columns:
            logger.warning(f"save_formula_columns contains non-existent columns: {', '.join(invalid_columns)}")
        
        try:
            # Check if row 2 exists
            row2_range = f"A2:{self._column_letter(len(self.actual_columns))}2"
            
            logger.info(f"Google-Spreadsheet-API: Read range formulas - {row2_range} - Checking for formula columns in row 2")
            result = self._api_call_with_retry(
                self.worksheet.get,
                row2_range,
                value_render_option='FORMULA'
            )
            
            if not result or not result[0]:
                logger.warning("Row 2 does not exist - table has no data rows")
                self.formula_templates = {}
                return
            
            row2_formulas = result[0]
            
            # Store formula templates only for specified columns
            self.formula_templates = {}
            for idx, formula in enumerate(row2_formulas):
                if idx < len(self.actual_columns):
                    col_name = self.actual_columns[idx]
                    
                    # Only save if column is in save_formula_columns list
                    if col_name not in self.save_formula_columns:
                        continue
                    
                    if isinstance(formula, str) and formula.startswith('='):
                        self.formula_templates[col_name] = formula
                        # Use repr() to escape newlines and other special characters
                        logger.info(f"Formula template for column '{col_name}': {repr(formula[:60])}...")
            
            logger.info(f"Captured {len(self.formula_templates)} formula templates from row 2")
            
        except Exception as e:
            logger.warning(f"Could not read formula templates: {e}")
            self.formula_templates = {}
    
    def _identify_formula_columns(self):
        """
        Identify which columns contain formulas by examining the first data row (row 2)
        Returns: Set of column names that contain formulas
        """
        try:
            # Read formulas from row 2 (first data row)
            row_range = f"A2:{self._column_letter(len(self.actual_columns))}2"
            
            logger.info(f"Google-Spreadsheet-API: Read range formulas - {row_range} - Identifying formula columns in row 2")
            formula_result = self._api_call_with_retry(
                self.worksheet.spreadsheet.values_get,
                row_range,
                params={'valueRenderOption': 'FORMULA'}
            )
            row_formulas = formula_result.get('values', [[]])[0] if formula_result.get('values') else []
            
            # Identify which columns have formulas
            formula_columns = set()
            for idx, formula in enumerate(row_formulas):
                if isinstance(formula, str) and formula.startswith('='):
                    col_name = self.actual_columns[idx] if idx < len(self.actual_columns) else None
                    if col_name:
                        formula_columns.add(col_name)
            
            logger.info(f"Identified formula columns: {formula_columns}")
            return formula_columns
            
        except Exception as e:
            logger.warning(f"Could not identify formula columns: {e}")
            return set()
    
    def _ensure_column_map(self, force_refresh=False):
        """
        Ensure column map is current - refresh if needed
        
        Args:
            force_refresh: If True, always refresh from sheet
                          If False, only refresh if not yet loaded
        """
        if not self.worksheet:
            self._get_worksheet()
        
        # Only refresh if:
        # 1. We don't have a column map yet, OR
        # 2. force_refresh is True
        if not self.column_map or force_refresh:
            self.column_map = self._build_column_map()
    
    def _ensure_column_map_without_formulas(self):
        """
        Lightweight version of _ensure_column_map that skips formula reading
        Use this when you only need column positions and don't need formulas
        """
        if not self.worksheet:
            self._get_worksheet()
        
        if not self.column_map:
            self.column_map = self._build_column_map_without_formulas()
    
    def _build_column_map_without_formulas(self):
        """
        Build column name -> column number mapping without reading formulas
        Lightweight version for operations that don't need formula templates
        
        Returns:
            Dict mapping column names to column numbers (1-based)
        """
        try:
            # Read header row
            logger.info(f"Google-Spreadsheet-API: Read row 1 - {self.sheet_name} - Reading headers (lightweight mode)")
            headers = self._api_call_with_retry(
                self.worksheet.row_values,
                1  # Row 1 is the header
            )
            
            column_map = {}
            actual_columns = []
            duplicates = []
            
            for idx, header in enumerate(headers, start=1):
                if not header:
                    continue
                
                # Check for duplicates
                if header in column_map:
                    duplicates.append(header)
                else:
                    column_map[header] = idx
                    actual_columns.append(header)
            
            if duplicates:
                raise ValueError(f"Duplicate column names found: {', '.join(duplicates)}")
            
            self.actual_columns = actual_columns
            logger.info(f"Read {len(actual_columns)} columns from sheet: {actual_columns}")
            
            # Verify required columns exist
            missing = [col for col in self.required_columns if col not in column_map]
            if missing:
                raise ValueError(f"Missing required columns: {', '.join(missing)}")
            
            # Don't read formulas - that's the point of this method
            return column_map
            
        except Exception as e:
            logger.error(f"Failed to build column map: {e}")
            raise
    
    def refresh_column_info(self):
        """
        Force a refresh of column information from the sheet
        Call this at the start of a refresh cycle to detect column changes
        
        Clears cached state to force fresh read from sheet
        """
        # Clear cached state
        self.records = []
        self.num_cols = 0
        self.num_data_rows = 0
        
        self._ensure_column_map(force_refresh=True)
    
    def _column_letter(self, col_num):
        """Convert column number to letter (1=A, 2=B, etc.)"""
        letter = ''
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            letter = chr(65 + remainder) + letter
        return letter
    
    def _estimate_size(self, data):
        """
        Estimate the size in bytes of data to be written
        Args:
            data: List of rows, where each row is a list of values
        Returns: Estimated size in bytes
        """
        total_size = 0
        for row in data:
            for cell in row:
                if cell is not None:
                    total_size += len(str(cell).encode('utf-8'))
        return total_size
    
    def _split_into_batches(self, data):
        """
        Split data into batches that don't exceed MAX_BATCH_SIZE
        
        Args:
            data: List of rows
        Returns: List of batches, where each batch is a list of rows
        """
        if not data:
            return []
        
        total_size = self._estimate_size(data)
        
        if total_size <= self.MAX_BATCH_SIZE:
            return [data]  # All fits in one batch
        
        # Calculate number of batches needed
        num_batches = max(2, int(total_size / self.MAX_BATCH_SIZE) + 1)
        rows_per_batch = max(1, len(data) // num_batches)
        
        batches = []
        for i in range(0, len(data), rows_per_batch):
            batch = data[i:i + rows_per_batch]
            batches.append(batch)
        
        logger.info(f"Split {len(data)} rows into {len(batches)} batches "
                   f"(total size: {total_size} bytes)")
        
        return batches
    
    def _api_call_with_retry(self, api_func, *args, **kwargs):
        """
        Execute an API call with exponential backoff retry and global locking
        
        Retries on:
        - 429 (Rate limit)
        - 408 (Request Timeout)
        - 500 (Internal Server Error)
        - 502 (Bad Gateway)
        - 503 (Service Unavailable)
        - 504 (Gateway Timeout)
        - Timeout (60 seconds per attempt)
        
        Uses global lock to ensure only one API operation at a time (thread-safe)
        Runs sleep in background thread to avoid blocking async event loops
        
        Args:
            api_func: The API function to call
            *args, **kwargs: Arguments to pass to the function
            
        Returns: Result of the API call
        Raises: Exception if max retries exceeded or timeout
        
        IMPORTANT - ThreadPoolExecutor usage:
        We do NOT use ThreadPoolExecutor as a context manager (i.e. no `with` block).
        Python's ThreadPoolExecutor.__exit__ calls executor.shutdown(wait=True), which
        blocks until the submitted thread finishes. If the Google Sheets API call is hung,
        this blocks forever — defeating the entire purpose of the timeout. Instead we call
        executor.shutdown(wait=False) on timeout to abandon the hung thread and proceed
        immediately to the retry loop. The abandoned thread eventually dies on its own
        when the OS-level TCP connection times out (typically 2-4 minutes for Google's
        infrastructure). At most one abandoned thread exists per retry attempt (7 total),
        which is acceptable.
        """
        import time
        import threading
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
        
        # Acquire the global lock to ensure exclusive API access
        with _api_lock:
            current_delay = self.retry_delay
            
            # Retryable error codes
            retryable_codes = ['408', '429', '500', '502', '503', '504']
            
            # Timeout per API call attempt (60 seconds)
            API_TIMEOUT_SECONDS = 60
            
            while current_delay <= self.MAX_RETRY_DELAY:
                try:
                    # Execute API call with timeout using ThreadPoolExecutor.
                    # CRITICAL: Do NOT use `with ThreadPoolExecutor(...) as executor:`
                    # because __exit__ calls shutdown(wait=True), which blocks on a
                    # hung API thread and nullifies the timeout protection. Instead,
                    # create the executor manually and call shutdown(wait=False) on
                    # timeout so we can proceed to the retry loop immediately.
                    executor = ThreadPoolExecutor(max_workers=1)
                    future = executor.submit(api_func, *args, **kwargs)
                    try:
                        result = future.result(timeout=API_TIMEOUT_SECONDS)
                        # Success! Shut down cleanly and reset retry delay.
                        executor.shutdown(wait=False)
                        self.retry_delay = self.INITIAL_RETRY_DELAY
                        return result
                    except FutureTimeoutError:
                        # Abandon the hung thread — do NOT wait=True here.
                        executor.shutdown(wait=False)
                        logger.error(f"Google-Spreadsheet-API: API call timed out after {API_TIMEOUT_SECONDS}s")
                        raise Exception(f"API call timed out after {API_TIMEOUT_SECONDS}s")
                    
                except Exception as e:
                    error_str = str(e)
                    
                    # Check if it's a retryable error
                    is_retryable = any(code in error_str for code in retryable_codes) or 'timed out' in error_str.lower()
                    
                    if is_retryable:
                        # Extract error code for logging
                        if 'timed out' in error_str.lower():
                            error_code = 'timeout'
                            logger.warning(f"Google-Spreadsheet-API: API timeout. Waiting {current_delay}s before retry...")
                        else:
                            error_code = next((code for code in retryable_codes if code in error_str), 'unknown')
                            if error_code == '429':
                                logger.warning(f"Google-Spreadsheet-API: Rate limit hit (429). Waiting {current_delay}s before retry...")
                            else:
                                logger.warning(f"Google-Spreadsheet-API: Server error ({error_code}). Waiting {current_delay}s before retry...")
                        
                        # Run sleep in a daemon background thread so it doesn't block
                        # the event loop (allowing Discord heartbeats to continue) AND
                        # so it cannot prevent process exit at shutdown time.
                        # daemon=True is safe here: if the process is exiting, the retry
                        # sleep is moot — the workflow lock ensures no retry sleep can be
                        # in progress when the shutdown sequence completes its drain wait.
                        sleep_thread = threading.Thread(target=time.sleep, args=(current_delay,), daemon=True)
                        sleep_thread.start()
                        sleep_thread.join()  # Wait for it to complete (or exit if process exits)
                        
                        # Double the delay for next time
                        current_delay *= 2
                        self.retry_delay = min(current_delay, self.MAX_RETRY_DELAY)
                        
                        if current_delay > self.MAX_RETRY_DELAY:
                            logger.error(f"Google-Spreadsheet-API: Max retry delay exceeded ({self.MAX_RETRY_DELAY}s). Giving up.")
                            raise
                    else:
                        # Different error, don't retry
                        logger.error(f"Google-Spreadsheet-API: API call failed with non-retryable error: {e}")
                        raise
        
        raise Exception(f"Google-Spreadsheet-API: API call failed after retries up to {self.MAX_RETRY_DELAY}s")
    
    def read_all_rows_unformatted(self):
        """
        Read all data rows from the table as unformatted values
        Returns raw values (numbers as floats, dates as serials, not formatted strings)
        
        Automatically truncates at the last row with non-empty data in actual columns.
        Ignores extraneous data to the right of the table (beyond actual_columns).
        
        Returns: List of dicts, one per row (excluding header)
                 Empty list if no data rows
        """
        self._ensure_column_map()
        
        try:
            # Get raw values (numbers as floats, not formatted strings)
            logger.info(f"Google-Spreadsheet-API: Read all values unformatted - {self.sheet_name} - Reading entire table")
            all_values = self._api_call_with_retry(
                self.worksheet.get_all_values,
                value_render_option='UNFORMATTED_VALUE'
            )
            
            # Convert to list of dicts using actual_columns (not sheet headers)
            # This avoids issues with duplicate empty headers
            if len(all_values) < 2:
                logger.info(f"Read 0 rows from {self.sheet_name} (no data rows)")
                return []
            
            records = []
            last_nonempty_row_idx = -1  # Track last row with data
            
            for row_idx, row in enumerate(all_values[1:]):  # Skip header row
                record = {}
                has_data = False
                
                # Only process columns that are in actual_columns
                for col_idx, col_name in enumerate(self.actual_columns):
                    value = row[col_idx] if col_idx < len(row) else ''
                    record[col_name] = value
                    
                    # Check if this cell has data
                    if value != '' and value != 0:
                        has_data = True
                
                records.append(record)
                
                # Track last row with any non-empty data
                if has_data:
                    last_nonempty_row_idx = row_idx
            
            # Truncate records after the last row with data
            if last_nonempty_row_idx >= 0:
                records = records[:last_nonempty_row_idx + 1]
            else:
                # No rows had data - return empty list
                records = []
            
            logger.info(f"Read {len(records)} rows from {self.sheet_name}")
            return records
            
        except Exception as e:
            logger.error(f"Failed to read rows: {e}")
            return []
    
    def find_rows_by_key(self, key_column, key_values, value_render_option='FORMATTED_VALUE'):
        """
        Find rows by key column values
        Optimized to read all data in batch instead of row-by-row
        
        Args:
            key_column: Name of the column to search (e.g., "DiscordID")
            key_values: List of values to search for
            value_render_option: 'FORMATTED_VALUE' (default, strings like "4:15")
                               or 'UNFORMATTED_VALUE' (raw numbers like 0.177083)
            
        Returns: Dict mapping key_value -> (row_number, row_data)
        """
        self._ensure_column_map()
        
        if key_column not in self.column_map:
            logger.error(f"Key column '{key_column}' not found in table")
            return {}
        
        key_col_idx = self.column_map[key_column] - 1  # Convert to 0-indexed
        
        try:
            # Read ALL data at once (much faster than row-by-row)
            render_mode = "formatted" if value_render_option == 'FORMATTED_VALUE' else "unformatted"
            logger.info(f"Google-Spreadsheet-API: Read all values {render_mode} - {self.sheet_name} - Reading table for key column {key_column}")
            all_values = self._api_call_with_retry(
                self.worksheet.get_all_values,
                value_render_option=value_render_option
            )
            
            if len(all_values) < 2:
                logger.info("No data rows found in sheet")
                return {}
            
            # Build the mapping
            found = {}
            key_values_set = set(str(v) for v in key_values)  # Convert to set for O(1) lookup
            
            # Iterate through rows (skip header row)
            for row_idx, row in enumerate(all_values[1:], start=2):
                # Check if this row has data in the key column
                if key_col_idx < len(row):
                    key_value = str(row[key_col_idx])
                    
                    if key_value in key_values_set:
                        # Only store the FIRST occurrence of each key
                        if key_value not in found:
                            # Build row dict
                            row_dict = {}
                            for col_idx, col_name in enumerate(self.actual_columns):
                                if col_idx < len(row):
                                    row_dict[col_name] = row[col_idx] if row[col_idx] is not None else ''
                                else:
                                    row_dict[col_name] = ''
                            
                            found[key_value] = (row_idx, row_dict)
                        else:
                            logger.warning(f"Duplicate {key_column} '{key_value}' found in row {row_idx} (already found in row {found[key_value][0]}). Using first occurrence.")
            
            logger.info(f"Found {len(found)} existing rows by {key_column}")
            return found
            
        except Exception as e:
            logger.error(f"Failed to find rows: {e}")
            return {}
    
    def find_rows_in_cache(self, key_column, key_values):
        """
        Find rows by key column values using cached records (no API call)
        Much faster than find_rows_by_key when state is already loaded
        
        Args:
            key_column: Name of the column to search
            key_values: List of values to search for
            
        Returns: Dict mapping key_value -> (row_number, row_data)
        """
        if not self.records:
            logger.warning("No cached records available, use find_rows_by_key instead")
            return {}
        
        if key_column not in self.column_map:
            logger.error(f"Key column '{key_column}' not found in table")
            return {}
        
        found = {}
        key_values_set = set(str(v) for v in key_values)
        
        # Search through cached records
        # Row numbers: records[0] = row 2, records[1] = row 3, etc.
        for record_idx, record in enumerate(self.records):
            row_num = record_idx + 2  # +2 because row 1 is header, records[0] is row 2
            key_value = str(record.get(key_column, ''))
            
            if key_value in key_values_set:
                if key_value not in found:
                    found[key_value] = (row_num, record)
                else:
                    logger.warning(f"Duplicate {key_column} '{key_value}' found in row {row_num} "
                                 f"(already found in row {found[key_value][0]}). Using first occurrence.")
        
        logger.info(f"Found {len(found)} existing rows by {key_column} (from cache)")
        return found
    
    def insert_rows_at_top(self, rows_data, num_rows):
        """
        Insert blank rows at position 2 (right below header)
        Blank rows preserve dropdowns, data validation, and conditional formatting
        
        Also updates internal state (prepends blank records to self.records)
        
        Args:
            rows_data: Not used, but kept for consistency
            num_rows: Number of rows to insert
        """
        self._ensure_column_map()
        
        try:
            logger.info(f"Inserting {num_rows} blank rows at position 2")
            
            # Insert blank rows - one empty list per row
            # This preserves dropdowns and data validation from adjacent rows
            # DO NOT MODIFY THIS BLOCK - it works perfectly as-is
            blank_rows = [[] for _ in range(num_rows)]
            logger.info(f"Google-Spreadsheet-API: Insert rows - {num_rows} rows at position 2 - Inserting blank rows")
            self._api_call_with_retry(
                self.worksheet.insert_rows,
                blank_rows,
                2,  # Start at position 2 (below header)
                value_input_option='RAW',
                inherit_from_before=False
            )
            
            logger.info(f"Inserted {num_rows} blank rows successfully in 1 API call")
            
            # Update internal state - prepend blank records
            blank_record = {col: '' for col in self.actual_columns}
            for i in range(num_rows):
                self.records.insert(0, blank_record.copy())  # Prepend at start
            self.num_data_rows += num_rows
            
            logger.debug(f"Updated state: num_data_rows now {self.num_data_rows}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert rows: {e}")
            return False
    
    def batch_update_rows(self, updates):
        """
        Update multiple rows in batches
        Always uses RAW value input to preserve datetime/numeric formatting
        Formulas should be restored separately using restore_formulas_to_all_rows()
        
        Args:
            updates: List of tuples (row_number, row_dict)
                    where row_dict maps column_name -> value
        """
        self._ensure_column_map()
        
        if not updates:
            logger.info("No rows to update")
            return True
        
        try:
            logger.info(f"Preparing to update {len(updates)} rows")
            
            # Build list of range updates
            updates_by_row = {}
            
            for update_tuple in updates:
                row_num, row_dict = update_tuple
                if row_num not in updates_by_row:
                    updates_by_row[row_num] = {}
                updates_by_row[row_num].update(row_dict)
            
            # Build range-based updates
            range_updates = []
            for row_num, row_dict in updates_by_row.items():
                # Only update columns that have actual values
                # This preserves dropdowns and data validation in empty cells
                for col_name, value in row_dict.items():
                    if col_name in self.column_map:
                        col_num = self.column_map[col_name]
                        col_letter = self._column_letter(col_num)
                        range_name = f"{col_letter}{row_num}"
                        
                        range_updates.append({
                            'range': range_name,
                            'values': [[value]]
                        })
                
                logger.debug(f"Row {row_num}: updating {len(row_dict)} columns")
            
            # Split into batches based on size
            all_values = [update['values'][0] for update in range_updates]
            batches_data = self._split_into_batches(all_values)
            
            # Execute batches with retry
            for batch_idx, batch_data in enumerate(batches_data, 1):
                logger.info(f"Updating batch {batch_idx}/{len(batches_data)} ({len(batch_data)} cell ranges)")
                
                # Rebuild range_updates for this batch
                batch_updates = range_updates[:len(batch_data)]
                range_updates = range_updates[len(batch_data):]
                
                # Always use RAW to preserve datetime/numeric formatting
                # Formulas will be restored separately
                logger.info(f"Google-Spreadsheet-API: Batch write - {len(batch_updates)} ranges - Writing data (batch {batch_idx}/{len(batches_data)})")
                self._api_call_with_retry(
                    self.worksheet.batch_update,
                    batch_updates,
                    value_input_option='RAW'
                )
            
            logger.info(f"Updated {len(updates)} rows successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to batch update: {e}")
            return False
    
    def batch_write_rows(self, start_row, rows_data):
        """
        Write multiple rows of data starting at a given row
        
        Args:
            start_row: Row number to start writing (e.g., 2 for first data row)
            rows_data: List of dicts, where each dict maps column_name -> value
        """
        self._ensure_column_map()
        
        if not rows_data:
            logger.info("No rows to write")
            return True
        
        try:
            logger.info(f"Preparing to write {len(rows_data)} rows starting at row {start_row}")
            
            # Convert dicts to lists using actual column order from sheet
            rows_as_lists = []
            for row_dict in rows_data:
                row_list = []
                for col_name in self.actual_columns:
                    value = row_dict.get(col_name, '')
                    row_list.append(value)
                rows_as_lists.append(row_list)
            
            # Split into batches
            batches = self._split_into_batches(rows_as_lists)
            
            # Write each batch
            current_row = start_row
            for batch_idx, batch in enumerate(batches, 1):
                logger.info(f"Writing batch {batch_idx}/{len(batches)} "
                           f"({len(batch)} rows)")
                
                # Calculate range using actual column count
                first_col = self._column_letter(1)
                last_col = self._column_letter(len(self.actual_columns))
                end_row = current_row + len(batch) - 1
                range_name = f"{first_col}{current_row}:{last_col}{end_row}"
                
                # Write batch
                logger.info(f"Google-Spreadsheet-API: Batch write range - {range_name} ({len(batch)} rows) - Restoring formulas (batch {batch_idx}/{len(batches)})")
                self._api_call_with_retry(
                    self.worksheet.update,
                    range_name,
                    batch,
                    value_input_option='RAW'
                )
                
                current_row = end_row + 1
            
            logger.info(f"Wrote {len(rows_data)} rows successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to batch write: {e}")
            return False
    
    def restore_formulas_to_all_rows(self):
        """
        Restore formulas from templates to ALL data rows (row 2 onward)
        This should be called AFTER all inserts and updates are complete
        Uses the formula templates captured from row 2
        
        Strategy: Write formula to row 2, then use copyPaste API to copy down
        This preserves relative references correctly
        """
        if not self.formula_templates:
            logger.info("No formula templates to restore")
            return True
        
        self._ensure_column_map()
        
        try:
            # Get the current last row with data
            logger.info(f"Google-Spreadsheet-API: Read all values - {self.sheet_name} - Getting row count for formula restore")
            all_values = self._api_call_with_retry(self.worksheet.get_all_values)
            last_row = len(all_values)
            
            if last_row < 2:
                logger.warning("Table has no data rows")
                return True
            
            logger.info(f"Restoring formulas to columns {list(self.formula_templates.keys())} for rows 2-{last_row}")
            
            # Get sheet ID for copyPaste requests
            sheet_id = self.worksheet._properties['sheetId']
            
            # Build batch of copyPaste requests for all formula columns
            requests = []
            
            for col_name, formula_template in self.formula_templates.items():
                col_num = self.column_map[col_name]
                col_letter = self._column_letter(col_num)
                
                # Step 1: Write formula to row 2
                # Use repr() to escape newlines and other special characters
                logger.info(f"Writing formula {repr(formula_template[:60])}... to {col_letter}2")
                logger.info(f"Google-Spreadsheet-API: Write cell - {col_letter}2 - Writing formula template")
                self._api_call_with_retry(
                    self.worksheet.update_acell,
                    f"{col_letter}2",
                    formula_template
                )
                
                # Step 2: Build copyPaste request to copy from row 2 to rows 3-last_row
                # This preserves relative references
                requests.append({
                    "copyPaste": {
                        "source": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,  # Row 2 (0-indexed)
                            "endRowIndex": 2,    # Exclusive, so just row 2
                            "startColumnIndex": col_num - 1,  # Convert to 0-indexed
                            "endColumnIndex": col_num
                        },
                        "destination": {
                            "sheetId": sheet_id,
                            "startRowIndex": 2,  # Row 3 (0-indexed)
                            "endRowIndex": last_row,  # Exclusive, ends after last_row
                            "startColumnIndex": col_num - 1,
                            "endColumnIndex": col_num
                        },
                        "pasteType": "PASTE_FORMULA"
                    }
                })
            
            # Execute all copyPaste requests in a single batch
            if requests:
                logger.info(f"Copying formulas down for {len(requests)} columns")
                body = {"requests": requests}
                logger.info(f"Google-Spreadsheet-API: Batch update - {len(requests)} copyPaste requests - Copying formulas to rows 3-{last_row}")
                self._api_call_with_retry(
                    self.worksheet.spreadsheet.batch_update,
                    body
                )
            
            logger.info(f"Successfully restored formulas to {len(self.formula_templates)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore formulas: {e}")
            return False
    
    def sort_table(self, sort_specs):
        """
        Sort the data rows in the table by specified columns
        
        Sorts only data rows (row 2 onward), preserving the header row.
        Uses instance attributes (num_data_rows, num_cols) for table geometry.
        
        Args:
            sort_specs: List of dicts with 'column' (name) and 'ascending' (bool)
                       Example: [
                           {'column': 'O', 'ascending': True},
                           {'column': 'FirstStopPAT', 'ascending': True}
                       ]
        
        Returns:
            bool: True if successful, False otherwise
        """
        self._ensure_column_map()
        
        if not sort_specs:
            logger.warning("No sort specifications provided")
            return True
        
        try:
            # Initialize state if needed
            if self.num_data_rows == 0 or self.num_cols == 0:
                logger.info("Table state not initialized, refreshing...")
                self.refresh_table()
            
            # Check if there's anything to sort
            if self.num_data_rows < 1:
                logger.info("No data rows to sort (table is empty)")
                return True
            
            # Validate sort columns exist
            for spec in sort_specs:
                col_name = spec.get('column')
                if col_name not in self.column_map:
                    raise ValueError(f"Sort column '{col_name}' not found in table")
            
            # Calculate last row: header (row 1) + data rows
            last_table_row = self.num_data_rows + 1
            
            logger.info(f"Sorting {self.num_data_rows} data rows by {[s['column'] for s in sort_specs]}")
            
            # Build sort spec for gspread (uses column indices, not names)
            gspread_sort_specs = []
            for spec in sort_specs:
                col_name = spec['column']
                col_index = self.column_map[col_name]  # 1-based
                ascending = spec.get('ascending', True)
                gspread_sort_specs.append((col_index, 'asc' if ascending else 'des'))
            
            # Sort using gspread's sort method on the data range
            # Range: row 2 to last_table_row, columns A to last actual column
            sort_range = f'A2:{self._column_letter(self.num_cols)}{last_table_row}'
            
            logger.info(f"Google-Spreadsheet-API: Sort range - {sort_range} - Sorting by {[s['column'] for s in sort_specs]}")
            self._api_call_with_retry(
                self.worksheet.sort,
                *gspread_sort_specs,
                range=sort_range
            )
            
            logger.info(f"✓ Successfully sorted table by {[s['column'] for s in sort_specs]}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sort table: {e}")
            return False
    
    # ===== ASYNC WRAPPERS =====
    # These run blocking Google Sheets API calls in an executor to prevent
    # blocking the Discord event loop and causing heartbeat timeouts
    
    async def batch_update_rows_async(self, updates):
        """Async version of batch_update_rows() - runs in executor"""
        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.batch_update_rows, updates)
    
    async def restore_formulas_to_all_rows_async(self):
        """Async version of restore_formulas_to_all_rows() - runs in executor"""
        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.restore_formulas_to_all_rows)
    
    async def sort_table_async(self, sort_specs):
        """Async version of sort_table() - runs in executor"""
        import asyncio
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.sort_table, sort_specs)


# No global instance - create instances as needed for different tables
