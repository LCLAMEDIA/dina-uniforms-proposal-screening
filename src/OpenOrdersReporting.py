import pandas as pd
import os
import io
import logging
from datetime import datetime
import csv
from typing import Dict, List, Tuple, Any
import re # Added for regex operations
import difflib
from rapidfuzz import fuzz

from AzureOperations import AzureOperations
from SharePointOperations import SharePointOperations
from  ConfigurationReader import ConfigurationReader
class OpenOrdersReporting:
    """
    A class for processing Open Order Reports and saving them to SharePoint.
    Based on the original OOR processing script with SharePoint integration.
    """

    # Define constants for derived column names for clarity
    NORMALIZED_ITEM_DESC_COL = '_NormalizedItemDescription'
    PARSED_NOTE_ID_COL = '_ParsedNoteID'

    # Define base columns for the composite key. This can be expanded.
    # QID, PurchaseNumber, itemDescription (normalized), Note (parsed) will be added if present.
    COMPOSITE_KEY_BASE_COLS = ['Order', 'ProductNum']

    # Define required headers for OOR Excel files
    REQUIRED_OOR_HEADERS = [
        'Order', 'DateIssued', 'VendorPO', 'customerPO', 'Requestor', 'ProductNum', 'barcodeupc',
        'itemDescription', 'Vendors', 'StockOnHand', 'TaskQueue', 'QueueDate', 'DaysInQueue',
        'QtyOrdered', 'QtyPacked', 'QtyRemaining', 'Note', 'itemNote', 'ETADate', 'ShipName',
        'ShipAddress', 'ShipCity', 'ShipState', 'ShipPostCode', 'TrackingNum', 'DaysOpen',
        'PurchaseNumber', 'DatePurchase', 'Suppliers', 'OurRef', 'QID', 'QIDDate'
    ]

    def __init__(self):
        logging.info("[OpenOrdersReporting] Initializing OpenOrdersReporting")
        # Initialize Azure and SharePoint connections
        self.azure_ops = AzureOperations()
        access_token = self.azure_ops.get_access_token()
        self.sharepoint_ops = SharePointOperations(access_token=access_token)

        # Load dynamic configuration
        self.config_reader = ConfigurationReader(sharepoint_ops=self.sharepoint_ops)
        config_loaded = self.config_reader.load_configuration()
        logging.info(f"[OpenOrdersReporting] Configuration loaded: {config_loaded}")

        # Get configuration values
        self.official_brands = self.config_reader.get_official_brands()
        self.product_num_mapping = self.config_reader.get_product_num_mapping()
        self.separate_file_customers = self.config_reader.get_separate_file_customers()

        # Create processing rules from configuration data
        self.processing_rules = {}
        for product_code, customer_name in self.product_num_mapping.items():
            # Normalize the product code key by trimming whitespace
            normalized_key = str(product_code).strip()
            if not normalized_key:  # Skip empty keys
                logging.warning(f"[OpenOrdersReporting] Skipping empty product code key")
                continue
                
            # Use the normalized key for the processing rules
            self.processing_rules[normalized_key] = {
                'customer_name': customer_name,
                'create_separate_file': normalized_key in self.separate_file_customers or product_code in self.separate_file_customers
            }   

        # Validate no overlapping prefixes for separate files
        self._validate_product_codes()

        # Configure folder paths based on environment variables
        self.oor_input_prefix = os.environ.get('OOR_INPUT_PREFIX', 'OOR')
        self.oor_input_path = os.environ.get('OOR_INPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Upload')
        self.oor_output_path = os.environ.get('OOR_OUTPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed')

        # Log all configuration values in one place
        logging.info("[OpenOrdersReporting] Loaded configuration values:")
        logging.info(f"[OpenOrdersReporting] - Official brands: {self.official_brands}")
        logging.info(f"[OpenOrdersReporting] - Product number mappings: {self.product_num_mapping}")
        logging.info(f"[OpenOrdersReporting] - Separate file customers: {self.separate_file_customers}")
        logging.info(f"[OpenOrdersReporting] - Processing rules: {self.processing_rules}")
        logging.info(f"[OpenOrdersReporting] - Input path: {self.oor_input_path}")
        logging.info(f"[OpenOrdersReporting] - Output path: {self.oor_output_path}")

    def validate_oor_file(self, file_bytes: bytes, filename: str) -> tuple:
        """
        Validates if the file is an acceptable OOR Excel file for processing.
        - Checks if the file is an Excel file (by extension and by reading with pandas)
        - Checks if the required headers are present in the Excel file using fuzzy matching
        Returns (True, '') if valid, else (False, reason)
        """

        # 1. Check if the file is an Excel file by extension
        ext = os.path.splitext(filename)[1].lower()
        if ext not in ['.xlsx', '.xls']:
            return False, "File is not an Excel file (.xlsx or .xls)"

        # 3. Try to read the Excel file
        try:
            excel_file = io.BytesIO(file_bytes)
            df = pd.read_excel(excel_file)
        except Exception as e:
            return False, f"File could not be read as an Excel file: {str(e)}"

        # 4. Check if required headers are present using fuzzy matching
        header_mapping, missing_headers = self._create_header_mapping(df.columns.tolist(), self.REQUIRED_OOR_HEADERS)
        if missing_headers:
            return False, f"Missing required headers: {', '.join(missing_headers)}"

        # Store the header mapping for later use in processing
        self.current_header_mapping = header_mapping
        
        return True, ''

    def _normalize_string(self, value: Any) -> str:
        """
        Normalizes a string value by stripping whitespace and converting to uppercase.
        Handles non-string inputs by converting them to string first.
        """
        if pd.isna(value):
            return "N/A_VAL" # Consistent placeholder for NaN to be part of a key
        if not isinstance(value, str):
            value = str(value)
        return value.strip().upper()

    def _create_header_mapping(self, excel_headers: List[str], required_headers: List[str]) -> Tuple[Dict[str, str], List[str]]:
        """
        Creates a mapping between required headers and actual Excel headers using fuzzy matching.
        Returns (header_mapping_dict, missing_headers_list)
        
        Args:
            excel_headers: List of headers from the Excel file
            required_headers: List of required headers for processing
            
        Returns:
            Tuple containing:
            - Dict mapping required header -> actual Excel header
            - List of headers that couldn't be matched (missing)
        """
        header_mapping = {}
        missing_headers = []
        
        # Minimum similarity threshold for fuzzy matching (70%)
        SIMILARITY_THRESHOLD = 70
        
        for required_header in required_headers:
            # First try exact match (case-insensitive)
            exact_match = None
            for excel_header in excel_headers:
                if required_header.lower() == excel_header.lower():
                    exact_match = excel_header
                    break
            
            if exact_match:
                header_mapping[required_header] = exact_match
                continue
            
            # If no exact match, try fuzzy matching using difflib
            best_match_header = None
            best_match_score = 0
            
            for excel_header in excel_headers:
                # Calculate similarity ratio (0.0 to 1.0)
                similarity = difflib.SequenceMatcher(None, required_header.lower(), excel_header.lower()).ratio()
                similarity_percentage = similarity * 100
                
                if similarity_percentage >= SIMILARITY_THRESHOLD and similarity_percentage > best_match_score:
                    best_match_header = excel_header
                    best_match_score = similarity_percentage
            
            if best_match_header:
                header_mapping[required_header] = best_match_header
                logging.info(f"[OpenOrdersReporting] Fuzzy matched '{required_header}' -> '{best_match_header}' (score: {best_match_score:.1f}%)")
            else:
                missing_headers.append(required_header)
                logging.warning(f"[OpenOrdersReporting] Could not match required header: '{required_header}'")
        
        return header_mapping, missing_headers

    def _get_column(self, df: pd.DataFrame, required_header: str) -> pd.Series:
        """
        Gets a column from the dataframe using the header mapping.
        Falls back to original header name if mapping doesn't exist.
        
        Args:
            df: The dataframe to get the column from
            required_header: The required header name
            
        Returns:
            The column as a pandas Series
        """
        if hasattr(self, 'current_header_mapping') and required_header in self.current_header_mapping:
            actual_header = self.current_header_mapping[required_header]
            return df[actual_header]
        else:
            # Fallback to original header (for backward compatibility)
            return df[required_header]

    def _column_exists(self, df: pd.DataFrame, required_header: str) -> bool:
        """
        Checks if a column exists in the dataframe using the header mapping.
        
        Args:
            df: The dataframe to check
            required_header: The required header name
            
        Returns:
            True if the column exists (either mapped or original), False otherwise
        """
        if hasattr(self, 'current_header_mapping') and required_header in self.current_header_mapping:
            actual_header = self.current_header_mapping[required_header]
            return actual_header in df.columns
        else:
            return required_header in df.columns

    def _get_actual_column_name(self, required_header: str) -> str:
        """
        Gets the actual column name from the header mapping.
        
        Args:
            required_header: The required header name
            
        Returns:
            The actual column name (mapped or original)
        """
        if hasattr(self, 'current_header_mapping') and required_header in self.current_header_mapping:
            return self.current_header_mapping[required_header]
        else:
            return required_header

    def _parse_note_for_id(self, note_text: Any) -> str:
        """
        Parses a note string to extract a relevant identifier.
        This is a simplified example; real-world parsing might be more complex.
        Normalizes the extracted ID.
        """
        if pd.isna(note_text) or not isinstance(note_text, str) or not note_text.strip():
            return "N/A_NOTE_ID" # Consistent placeholder for missing/empty notes

        # Example: take the part before the first comma or pipe, then normalize.
        # This regex needs to be tailored to the actual data patterns in 'Note'.
        match = re.match(r"([^,|]+)", note_text.strip())
        if match:
            return self._normalize_string(match.group(1))
        return self._normalize_string(note_text.strip()) # Fallback if no delimiter found

    def _apply_vendor_filtering(self, df: pd.DataFrame, product_code: str) -> pd.DataFrame:
        """
        Apply hardcoded GENERIC vendor filtering to keep only PNW vendors.
        
        Args:
            df: DataFrame to filter
            product_code: Product code to check for GENERIC filtering
            
        Returns:
            DataFrame with GENERIC-PNW filtering applied if applicable
        """
        if df.empty:
            return df
        
        # Only apply vendor filtering to GENERIC products
        if product_code != "GENERIC":
            return df
        
        # Check if Vendors column exists
        if not self._column_exists(df, 'Vendors'):
            logging.warning(f"[OpenOrdersReporting._apply_vendor_filtering] 'Vendors' column not found for GENERIC filtering")
            return df
        
        initial_count = len(df)
        logging.info(f"[OpenOrdersReporting._apply_vendor_filtering] Applying GENERIC vendor filtering: keep only 'PNW' vendors")
        
        # Special logic for GENERIC-SAMPLE-N/A-O/S products
        sample_mask = self._get_column(df, 'ProductNum').astype(str).str.contains('GENERIC-SAMPLE-N/A-O/S', na=False)
        sample_df = df[sample_mask].copy()
        non_sample_df = df[~sample_mask].copy()
        
        if not sample_df.empty:
            # Filter sample products to keep only PNW vendor
            vendor_match_mask = self._get_column(sample_df, 'Vendors').astype(str).str.contains('PNW', case=False, na=False)
            filtered_sample_df = sample_df[vendor_match_mask].copy()
            
            logging.info(f"[OpenOrdersReporting._apply_vendor_filtering] GENERIC-SAMPLE products: {len(sample_df)} -> {len(filtered_sample_df)} (removed {len(sample_df) - len(filtered_sample_df)} non-PNW vendors)")
            
            # Keep PNW vendor name (don't remove it to avoid empty vendors column)
            logging.info(f"[OpenOrdersReporting._apply_vendor_filtering] Kept PNW vendor names for GENERIC products")
            
            # Combine filtered sample data with non-sample data
            result_df = pd.concat([filtered_sample_df, non_sample_df], ignore_index=True)
        else:
            result_df = non_sample_df
        
        final_count = len(result_df)
        removed_count = initial_count - final_count
        
        if removed_count > 0:
            logging.info(f"[OpenOrdersReporting._apply_vendor_filtering] GENERIC vendor filtering removed {removed_count} rows (kept {final_count})")
        else:
            logging.info(f"[OpenOrdersReporting._apply_vendor_filtering] No GENERIC rows filtered")
        
        return result_df.reset_index(drop=True)

    def _apply_vendor_filtering_by_customer(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies hardcoded GENERIC vendor filtering to keep only PNW vendors.

        Args:
            df: DataFrame to process.

        Returns:
            DataFrame with GENERIC-PNW filtering applied if applicable.
        """
        if df.empty:
            return df

        # Check if 'ProductNum' column exists, essential for this logic
        if not self._column_exists(df, 'ProductNum'):
            logging.warning("[OpenOrdersReporting._apply_vendor_filtering_by_customer] 'ProductNum' column not found. Skipping vendor filtering.")
            return df

        # Only filter GENERIC products
        product_codes_requiring_filtering = ["GENERIC"]

        logging.info(f"[OpenOrdersReporting._apply_vendor_filtering_by_customer] Performing GENERIC vendor filtering")

        # Create a combined mask for all rows that belong to products needing vendor filtering
        combined_mask_for_filtering_products = pd.Series(False, index=df.index)
        for product_code in product_codes_requiring_filtering:
            # Match exact product code or product code as a prefix (e.g., GENERIC and GENERIC-*)
            exact_match = self._get_column(df, 'ProductNum') == product_code
            prefix_match = self._get_column(df, 'ProductNum').astype(str).str.startswith(f"{product_code}-", na=False)
            current_product_mask = exact_match | prefix_match
            combined_mask_for_filtering_products = combined_mask_for_filtering_products | current_product_mask

        # Split DataFrame into parts: one that needs vendor filtering, one that doesn't
        df_to_filter_segment = df[combined_mask_for_filtering_products].copy()
        df_no_filtering_needed_segment = df[~combined_mask_for_filtering_products].copy()

        filtered_segment = pd.DataFrame()  # Initialize empty DataFrame for the filtered part

        if not df_to_filter_segment.empty:
            logging.info(f"[OpenOrdersReporting._apply_vendor_filtering_by_customer] Applying vendor filtering to {len(df_to_filter_segment)} rows from configured products.")
            
            # Apply vendor filtering for each product code
            vendor_filtered_segments = []
            for product_code in product_codes_requiring_filtering:
                # Get data for this specific product code
                exact_match = self._get_column(df_to_filter_segment, 'ProductNum') == product_code
                prefix_match = self._get_column(df_to_filter_segment, 'ProductNum').astype(str).str.startswith(f"{product_code}-", na=False)
                product_mask = exact_match | prefix_match
                product_segment = df_to_filter_segment[product_mask].copy()
                
                if not product_segment.empty:
                    # Apply vendor filtering
                    vendor_filtered_segment = self._apply_vendor_filtering(product_segment, product_code)
                    vendor_filtered_segments.append(vendor_filtered_segment)
            
            # Combine all vendor-filtered segments
            if vendor_filtered_segments:
                filtered_segment = pd.concat(vendor_filtered_segments, ignore_index=True)
            else:
                filtered_segment = df_to_filter_segment.copy()
            
            logging.info(f"[OpenOrdersReporting._apply_vendor_filtering_by_customer] Rows in segment after vendor filtering: {len(filtered_segment)}. "
                         f"Original: {len(df_to_filter_segment)}, After filtering: {len(filtered_segment)}")
        else:
            logging.info("[OpenOrdersReporting._apply_vendor_filtering_by_customer] No rows found for products configured for vendor filtering.")

        # Concatenate the filtered segment (if any) with the segment that didn't need filtering
        if not filtered_segment.empty:
            final_df = pd.concat([filtered_segment, df_no_filtering_needed_segment], ignore_index=True)
        else:  # If filtered_segment is empty
            final_df = df_no_filtering_needed_segment.copy()

        logging.info(f"[OpenOrdersReporting._apply_vendor_filtering_by_customer] Total rows after vendor filtering: {len(final_df)}")
        return final_df.reset_index(drop=True)  # Reset index for clean DataFrame

    def process_excel_file(self, excel_file_bytes: bytes, filename: str = None) -> Dict[str, Any]:
        """
        Process an Excel file containing an Open Order Report.
        Returns statistics about the processing and uploads files to SharePoint.
        
        Parameters:
        - excel_file_bytes: The bytes of the Excel file to process
        - filename: The name of the input file
        
        Returns:
        - Dict with processing statistics or error information
        """
        
        logging.info(f"[OpenOrdersReporting] Processing file: {filename}")
        logging.info(f"[OpenOrdersReporting] Data size: {len(excel_file_bytes)} bytes")
        
        if len(excel_file_bytes) > 10:
            logging.info(f"[OpenOrdersReporting] First 10 bytes: {excel_file_bytes[:10].hex()}")   
        
        # Statistics tracking
        stats = {
            'input_file': filename,
            'total_rows': 0,
            'filtered_brand_rows': 0,
            'duplicate_rows_removed_by_customer_logic': 0,
            'output_files': {},
            'product_counts': {},
            'start_time': datetime.now(),
        }
        
        # Validate the file first
        is_valid, validation_message = self.validate_oor_file(excel_file_bytes, filename)
        if not is_valid:
            logging.warning(f"[OpenOrdersReporting] File validation failed: {validation_message}")
            stats['success'] = False
            stats['error_message'] = validation_message
            stats['error_type'] = 'validation_error'
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            return stats
            
        try:
            # Read the Excel file from bytes
            excel_file = io.BytesIO(excel_file_bytes)
            logging.info(f"[OpenOrdersReporting] Created BytesIO object, attempting to read with pandas")
            
            # Read Excel file without creating an index
            df = pd.read_excel(excel_file, engine='openpyxl', index_col=None)
            
            # Log the column names to verify
            logging.info(f"[OpenOrdersReporting] Excel columns: {list(df.columns)}")
            
            # Clean up any 'Unnamed' columns if they exist
            unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
            if unnamed_cols:
                logging.info(f"[OpenOrdersReporting] Removing unnamed columns: {unnamed_cols}")
                df = df.drop(columns=unnamed_cols)
            
            stats['total_rows'] = len(df)
            logging.info(f"[OpenOrdersReporting] Successfully read Excel file with {len(df)} rows")
        
            # Main dataframe to process
            main_df = df.copy().reset_index(drop=True)  # Ensure clean sequential index
            product_num_column = self._get_actual_column_name('ProductNum')  # Key column for filtering
            
            # Check required columns exist to avoid errors later
            required_columns = ['ProductNum', 'DateIssued', 'Order']
            missing_columns = [col for col in required_columns if not self._column_exists(main_df, col)]
            if missing_columns:
                logging.warning(f"[OpenOrdersReporting] Missing required columns: {missing_columns}. Processing may be incomplete.")
            
            # 1. FIRST - Apply vendor filtering based on product code configuration
            rows_before_vendor_filtering = len(main_df)
            main_df = self._apply_vendor_filtering_by_customer(main_df)
            main_df = main_df.reset_index(drop=True)  # Clean index after filtering
            rows_after_vendor_filtering = len(main_df)
            stats['duplicate_rows_removed_by_customer_logic'] = rows_before_vendor_filtering - rows_after_vendor_filtering
            logging.info(f"[OpenOrdersReporting] Total rows removed by vendor filtering: {stats['duplicate_rows_removed_by_customer_logic']}")
            
            # 2. ENHANCED - Filter out brands with validation
            if self._column_exists(main_df, 'ProductNum') and self.official_brands:
                logging.info(f"[OpenOrdersReporting] Starting brand filtering. Initial rows: {len(main_df)}")
                
                # Build list of rows to remove (more reliable than compound masking)
                rows_to_remove = []
                brand_removal_stats = {}
                
                for brand in self.official_brands:
                    if not brand or pd.isna(brand):
                        logging.warning(f"[OpenOrdersReporting] Skipping invalid brand: {brand}")
                        continue
                        
                    # Normalize brand to uppercase for case-insensitive comparison
                    brand = str(brand).strip().upper()
                    if not brand:
                        continue
                        
                    # Create fresh mask for this brand with normalized comparison
                    brand_prefix = f"{brand}-"
                    
                    current_mask = (
                        main_df[product_num_column].astype(str).str.strip().str.upper().str.startswith(brand_prefix, na=False)
                    )
                    
                    # Get matching rows
                    matching_rows = main_df[current_mask]
                    if not matching_rows.empty:
                        brand_removal_stats[brand] = len(matching_rows)
                        rows_to_remove.extend(matching_rows.index.tolist())
                        logging.info(f"[OpenOrdersReporting] Brand '{brand}': found {len(matching_rows)} rows to remove")
                
                # Remove duplicates from removal list
                rows_to_remove = list(set(rows_to_remove))
                
                if rows_to_remove:
                    # Validate before deletion
                    initial_count = len(main_df)
                    
                    # Perform deletion
                    main_df = main_df.drop(index=rows_to_remove).reset_index(drop=True)
                    
                    # Validate after deletion
                    final_count = len(main_df)
                    expected_count = initial_count - len(rows_to_remove)
                    
                    if final_count != expected_count:
                        logging.error(f"[OpenOrdersReporting] DELETION VALIDATION FAILED! Expected {expected_count} rows, got {final_count}")
                        logging.error(f"[OpenOrdersReporting] Initial: {initial_count}, Removed: {len(rows_to_remove)}, Final: {final_count}")
                        raise Exception("Brand filtering deletion failed validation")
                    
                    stats['filtered_brand_rows'] = len(rows_to_remove)
                    logging.info(f"[OpenOrdersReporting] Successfully removed {len(rows_to_remove)} rows from brands: {list(brand_removal_stats.keys())}")
                    logging.info(f"[OpenOrdersReporting] Brand removal breakdown: {brand_removal_stats}")
                else:
                    stats['filtered_brand_rows'] = 0
                    logging.info("[OpenOrdersReporting] No rows found matching filtered brands")
            else:
                stats['filtered_brand_rows'] = 0
                logging.info("[OpenOrdersReporting] No brand filtering needed - no official brands or ProductNum column missing")
            
            # 3. THIRD - Add standard columns to the main dataframe
            main_df = self._add_checking_customer_columns(main_df)
            
            # 4. PRODUCT-BASED - Extract orders by ProductNum for CreateSeparateFile=Yes customers
            product_dataframes = {}
            remaining_df = main_df.copy().reset_index(drop=True)
            
            initial_remaining_count = len(remaining_df)
            logging.info(f"[OpenOrdersReporting] Starting product-based file separation. Initial rows: {initial_remaining_count}")
            
            # First, extract orders by ProductNum for customers with CreateSeparateFile=Yes
            product_allocated_dataframes, remaining_df = self._apply_product_based_separation(remaining_df)
            
            # Add product-based allocated dataframes to product_dataframes
            for customer_code, customer_df in product_allocated_dataframes.items():
                if not customer_df.empty:
                    customer_df = self._apply_processing(customer_df, self.processing_rules.get(customer_code, {}).get('customer_name', customer_code))
                    product_dataframes[customer_code] = customer_df
                    stats['product_counts'][customer_code] = len(customer_df)
                    logging.info(f"[OpenOrdersReporting] Product-based separation allocated {len(customer_df)} rows to {customer_code}")
            
            total_product_allocated = sum(len(df) for df in product_allocated_dataframes.values())
            logging.info(f"[OpenOrdersReporting] Product-based separation completed: {total_product_allocated} rows allocated to {len(product_allocated_dataframes)} customer files")
            
            # 5. UNIVERSAL - Apply universal pattern matching to remaining orders
            logging.info(f"[OpenOrdersReporting] Starting universal pattern matching on remaining {len(remaining_df)} orders")
            
            # Apply universal pattern matching to allocate remaining orders to customer files
            allocated_dataframes, remaining_df = self._apply_universal_pattern_matching(remaining_df)
            
            # Add universal pattern matching results to product_dataframes
            for customer_code, customer_df in allocated_dataframes.items():
                if not customer_df.empty:
                    customer_df = self._apply_processing(customer_df, self.processing_rules.get(customer_code, {}).get('customer_name', customer_code))
                    if customer_code in product_dataframes:
                        # Combine with existing product-based allocation
                        combined_df = pd.concat([product_dataframes[customer_code], customer_df], ignore_index=True)
                        product_dataframes[customer_code] = combined_df
                        stats['product_counts'][customer_code] = len(combined_df)
                        logging.info(f"[OpenOrdersReporting] Combined {len(customer_df)} universal matching rows with existing {customer_code} (total: {len(combined_df)})")
                    else:
                        # New customer file from universal matching
                        product_dataframes[customer_code] = customer_df
                        stats['product_counts'][customer_code] = len(customer_df)
                        logging.info(f"[OpenOrdersReporting] Universal matching allocated {len(customer_df)} rows to {customer_code}")
            
            total_rows_allocated = sum(len(df) for df in allocated_dataframes.values())
            logging.info(f"[OpenOrdersReporting] Universal pattern matching completed: {total_rows_allocated} rows allocated to {len(allocated_dataframes)} customer files")
            
            # 6. SIXTH - Process the remaining data
            # Apply customer/checking note processing to remaining dataframe
            remaining_df = self._apply_processing(remaining_df, "OTHERS")
            stats['remaining_rows'] = len(remaining_df)
            
            # 7. SEVENTH - Prepare for output
            # Create structured filename components with robust date handling
            # Use local timezone-aware datetime to ensure correct date
            current_time = datetime.now().astimezone()
            
            # Log current time for debugging date issues  
            logging.info(f"[OpenOrdersReporting] Current local time: {current_time}")
            logging.info(f"[OpenOrdersReporting] Timezone: {current_time.tzinfo}")
            logging.info(f"[OpenOrdersReporting] UTC offset: {current_time.utcoffset()}")
            
            today_date_fmt = current_time.strftime("%Y%m%d")
            time_fmt = current_time.strftime("%H%M")
            today_folder_fmt = current_time.strftime("%d-%m-%y")
            
            # Log the generated date formats for verification
            logging.info(f"[OpenOrdersReporting] Generated folder date: {today_folder_fmt} (DD-MM-YY format)")
            logging.info(f"[OpenOrdersReporting] Generated file date: {today_date_fmt} (YYYYMMDD format)")
            
            # Fix path formatting for SharePoint
            processed_date_dir = os.path.join(self.oor_output_path, today_folder_fmt).replace('\\', '/')
            
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            
            # 7. SEVENTH - Upload main file if it's not empty
            if not remaining_df.empty:
                # Extract meaningful metadata for the filename
                row_count = len(remaining_df)
                
                # Check if we're generating separate files or just one main file
                if product_dataframes:
                    others_filename = f"OTHERS_OOR_{today_date_fmt}_{time_fmt}_rows{row_count}.csv"
                else:
                    others_filename = f"OOR_{today_date_fmt}_{time_fmt}_rows{row_count}.csv"
                
                # Extract source filename (if available) for reference in logs
                source_reference = f" from {filename}" if filename else ""
                logging.info(f"[OpenOrdersReporting] Creating structured file: {others_filename}{source_reference}")
                    
                # Sanitize the filename to avoid filesystem issues
                others_filename = self._sanitize_filename(others_filename)
                    
                others_path = f"{processed_date_dir}/{others_filename}"
                others_bytes = self._dataframe_to_csv_bytes(remaining_df)
                
                # Upload to SharePoint
                self.sharepoint_ops.upload_file_to_path(
                    drive_id=drive_id,
                    file_path=others_path,
                    file_name=others_filename,
                    file_bytes=others_bytes,
                    content_type="text/csv"
                )
                stats['output_files']['main_or_others'] = others_filename
            
            # 8. EIGHTH - Upload product-specific files
            for product_code, product_df in product_dataframes.items():
                if product_df.empty:
                    continue
                    
                # Get customer name from processing rules, handling NRM split variants
                if product_code in self.processing_rules:
                    customer_name = self.processing_rules[product_code].get('customer_name', product_code)
                elif product_code.startswith('NRM-'):
                    # Handle NRM split variants (NRM-NRMA, NRM-NRMPR, NRM-DC)
                    customer_name = product_code  # Use the split variant name as customer name
                else:
                    customer_name = product_code
                
                # Extract meaningful metadata for the filename
                row_count = len(product_df)
                product_filename = f"{product_code}_OOR_{today_date_fmt}_{time_fmt}_rows{row_count}.csv"
                
                # Extract source filename (if available) for reference in logs
                source_reference = f" from {filename}" if filename else ""
                logging.info(f"[OpenOrdersReporting] Creating customer file: {product_filename}{source_reference}")
                
                # Sanitize the filename to avoid filesystem issues
                product_filename = self._sanitize_filename(product_filename)
                
                product_path = f"{processed_date_dir}/{product_filename}"
                product_bytes = self._dataframe_to_csv_bytes(product_df)
                
                # Upload to SharePoint
                self.sharepoint_ops.upload_file_to_path(
                    drive_id=drive_id,
                    file_path=product_path,
                    file_name=product_filename,
                    file_bytes=product_bytes, 
                    content_type="text/csv"
                )
                stats['output_files'][customer_name] = product_filename
            
            # Finalize stats
            stats['success'] = True
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            logging.info(f"[OpenOrdersReporting] Processing completed in {stats['duration']:.2f} seconds. Stats: {stats}")
            
            return stats
            
        except pd.errors.EmptyDataError as e:
            logging.error(f"[OpenOrdersReporting] Empty data error: {str(e)}", exc_info=True)
            stats['success'] = False
            stats['error_message'] = "The Excel file contains no data or only header information."
            stats['error_type'] = 'empty_data_error'
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            return stats
            
        except pd.errors.ParserError as e:
            logging.error(f"[OpenOrdersReporting] Excel parser error: {str(e)}", exc_info=True)
            stats['success'] = False
            stats['error_message'] = "Unable to parse the Excel file. The file may be corrupted or in an unsupported format."
            stats['error_type'] = 'parser_error'
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            return stats
            
        except Exception as e:
            logging.error(f"[OpenOrdersReporting] Error processing file: {str(e)}", exc_info=True)
            stats['success'] = False
            stats['error_message'] = str(e)
            stats['error_type'] = 'processing_error'
            stats['end_time'] = datetime.now()
            if 'start_time' in stats:
                stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            return stats

    def _dataframe_to_csv_bytes(self, df: pd.DataFrame) -> bytes:
        """Convert a pandas DataFrame to CSV bytes with proper quoting"""
        # Make sure to reset any potential index that might have been set
        df_to_save = df.reset_index(drop=True)

        # Create a buffer and save without index
        buffer = io.StringIO()
        df_to_save.to_csv(buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)

        # Get the string buffer content and encode to bytes
        csv_content = buffer.getvalue()

        # Log the first line to verify header structure
        if '\n' in csv_content:
            header_line = csv_content.split('\n', 1)[0]
            logging.info(f"[OpenOrdersReporting] CSV header line for export: {header_line}")

        return csv_content.encode('utf-8')

    def _add_checking_customer_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add 'CHECKING NOTE' and 'CUSTOMER' columns to the dataframe if they don't exist, ensuring correct order."""
        modified_df = df.copy()

        # Define desired column order for the first two columns
        desired_first_cols = ['CHECKING NOTE', 'CUSTOMER']

        # Get existing columns
        current_cols = list(modified_df.columns)

        # Columns to prepend if they are missing
        cols_to_add_at_front = []

        # Check for CUSTOMER (should be second)
        if 'CUSTOMER' not in current_cols:
            modified_df.insert(0, 'CUSTOMER', '') # Insert temporarily at 0, will be pushed by CHECKING NOTE
            current_cols.insert(0, 'CUSTOMER') # Reflect in current_cols for next check

        # Check for CHECKING NOTE (should be first)
        if 'CHECKING NOTE' not in current_cols:
            modified_df.insert(0, 'CHECKING NOTE', '')

        # Reorder to ensure CHECKING NOTE is first, CUSTOMER is second, then others
        final_cols_order = []
        if 'CHECKING NOTE' in modified_df.columns:
            final_cols_order.append('CHECKING NOTE')
        if 'CUSTOMER' in modified_df.columns:
            final_cols_order.append('CUSTOMER')

        for col in modified_df.columns:
            if col not in final_cols_order:
                final_cols_order.append(col)

        modified_df = modified_df[final_cols_order]

        return modified_df

    def _apply_processing(self, df_to_process: pd.DataFrame, df_name: str) -> pd.DataFrame:
        """Applies customer name and checking note logic"""
        if df_to_process.empty:
            return df_to_process

        # Ensure 'CUSTOMER' and 'CHECKING NOTE' columns exist from _add_checking_customer_columns
        # df_to_process = self._add_checking_customer_columns(df_to_process) # Already called before this in main flow

        product_num_column = self._get_actual_column_name('ProductNum')

        # --- Customer Name Population ---
        # df_name is now the target customer name for specific files (e.g. "CALVARY") or "OTHERS"
        if df_name != "OTHERS" and df_name in self.product_num_mapping.values(): # If df_name is a mapped customer name
             df_to_process['CUSTOMER'] = df_name
        elif df_name == "FORMER CUSTOMERS": # This case might be redundant if official_brands are filtered out earlier
            if self._column_exists(df_to_process, 'ProductNum'):
                for index, row in df_to_process.iterrows():
                    product_num_val = row.get(product_num_column)
                    if pd.notna(product_num_val):
                        product_num_str = str(product_num_val)
                        brand_prefix = product_num_str.split('-')[0] if '-' in product_num_str else product_num_str
                        if brand_prefix in self.official_brands: # official_brands contains product codes like 'BIS'
                             df_to_process.at[index, 'CUSTOMER'] = self.product_num_mapping.get(brand_prefix, brand_prefix)
        elif self._column_exists(df_to_process, 'ProductNum'):  # For OTHERS, try to map based on ProductNum
            for index, row in df_to_process.iterrows():
                # Only attempt to fill CUSTOMER if it's currently empty or matches a generic placeholder
                current_customer = str(row.get('CUSTOMER', '')).strip()
                if current_customer == '' or current_customer.startswith('N/A'):
                    product_num_val = row.get(product_num_column)
                    if pd.notna(product_num_val):
                        product_num_str = str(product_num_val)
                        # Exact match for ProductNum in mapping
                        if product_num_str in self.product_num_mapping:
                            df_to_process.at[index, 'CUSTOMER'] = self.product_num_mapping[product_num_str]
                            continue
                        # Prefix match for ProductNum in mapping (e.g. SAK-123 matches SAK)
                        matched_by_prefix = False
                        for prefix_key, customer_value in self.product_num_mapping.items():
                            if product_num_str.startswith(prefix_key + "-") or product_num_str == prefix_key:
                                df_to_process.at[index, 'CUSTOMER'] = customer_value
                                matched_by_prefix = True
                                break

        return df_to_process

    def _extract_product_code(self, product_num: str) -> str:
        """
        Extract the product code from a product number with validation.
        
        Args:
            product_num: Product number string (e.g., 'SAK-XYZ-123')
            
        Returns:
            Product code prefix (e.g., 'SAK') or 'UNKNOWN' for invalid inputs
        """
        if not product_num or pd.isna(product_num):
            return "UNKNOWN"
        
        if not isinstance(product_num, str):
            product_num = str(product_num)
        
        product_num = product_num.strip()
        if not product_num:
            return "UNKNOWN"
        
        # Get prefix before first hyphen
        if "-" in product_num:
            prefix = product_num.split("-")[0].strip().upper()
            return prefix if prefix else "UNKNOWN"
        
        # Handle products without hyphens
        return product_num.upper()

    def _validate_product_codes(self):
        """
        Validate product codes to ensure no overlapping prefixes exist
        that could cause incorrect file separation.
        """
        separate_file_codes = sorted([pc for pc in self.processing_rules
                                     if self.processing_rules[pc].get('create_separate_file', False)],
                                     key=len, reverse=True)

        if not separate_file_codes:
            logging.info("[OpenOrdersReporting] No product codes configured for separate files.")
            return

        # Check for overlapping prefixes
        for i, code1 in enumerate(separate_file_codes):
            for code2 in separate_file_codes[i+1:]:
                if code2.startswith(code1) or code1.startswith(code2):
                    logging.warning(f"[OpenOrdersReporting] Found potentially overlapping product codes: '{code1}' and '{code2}'. This may cause unexpected file separation behavior.")

        logging.info(f"[OpenOrdersReporting] Validated {len(separate_file_codes)} product codes for separate files")

    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize a filename to ensure it's valid for the filesystem.
        Removes/replaces invalid characters.
        """
        # Define characters to replace
        invalid_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
        sanitized = filename

        # Replace invalid characters with underscores
        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')

        # Ensure filename doesn't exceed max length (255 is common limit)
        if len(sanitized) > 250:  # Use 250 to be safe
            base, ext = os.path.splitext(sanitized)
            sanitized = base[:250-len(ext)] + ext

        return sanitized

    def _apply_product_based_separation(self, df: pd.DataFrame) -> tuple:
        """
        Extract orders by ProductNum for customers with CreateSeparateFile=Yes.
        Searches for exact matches in Order, ProductNum, ShipAddress, and OurRef headers with normalization.
        This runs BEFORE universal pattern matching to ensure product-based allocation.
        
        Args:
            df: DataFrame containing all orders to process
            
        Returns:
            tuple: (allocated_orders_dict, remaining_df)
                - allocated_orders_dict: {customer_code: DataFrame} for each customer with allocated orders
                - remaining_df: DataFrame with orders that couldn't be allocated by product code
        """
        if df.empty:
            return {}, pd.DataFrame()
            
        logging.info(f"[OpenOrdersReporting] Starting product-based separation for {len(df)} orders")
        
        allocated_orders = {}
        unallocated_mask = pd.Series([True] * len(df), index=df.index)
        
        # Define columns to search for customer codes
        search_columns = []
        for col in ['Order', 'ProductNum', 'ShipAddress', 'OurRef']:
            if self._column_exists(df, col):
                search_columns.append((col, self._get_actual_column_name(col)))
        
        if not search_columns:
            logging.warning(f"[OpenOrdersReporting] No searchable columns found (Order, ProductNum, ShipAddress, OurRef), skipping product-based separation")
            return {}, df.copy()
        
        logging.info(f"[OpenOrdersReporting] Searching in columns: {[col[0] for col in search_columns]}")
        
        # For each customer that can have separate files
        for customer_code, rule in self.processing_rules.items():
            # Only consider customers with separate files enabled
            if not rule.get('create_separate_file', False):
                continue
            
            # Normalize customer code for comparison
            normalized_customer_code = self._normalize_string(customer_code)
            customer_mask = pd.Series([False] * len(df), index=df.index)
            
            # Check each search column for exact matches
            for col_name, actual_col_name in search_columns:
                # Normalize column values and check for exact matches
                normalized_col = df[actual_col_name].apply(lambda x: self._normalize_string(x) if pd.notna(x) else "")
                
                # Exact match
                exact_match = normalized_col == normalized_customer_code
                
                # Prefix match for ProductNum (e.g., GENERIC-SAMPLE-123)
                if col_name == 'ProductNum':
                    prefix_match = normalized_col.str.startswith(f"{normalized_customer_code}-", na=False)
                    col_match = exact_match | prefix_match
                else:
                    col_match = exact_match
                
                if col_match.any():
                    customer_mask |= col_match
                    logging.info(f"[OpenOrdersReporting] Found {col_match.sum()} exact matches for '{customer_code}' in {col_name}")
            
            # Add fuzzy matching for CustomerName and Code in ShipAddress and OurRef with 90% threshold
            customer_name = rule.get('customer_name', customer_code)
            fuzzy_matches = pd.Series([False] * len(df), index=df.index)
            
            # Define columns to check for fuzzy matching
            fuzzy_columns = []
            if self._column_exists(df, 'ShipAddress'):
                fuzzy_columns.append(('ShipAddress', self._get_actual_column_name('ShipAddress')))
            if self._column_exists(df, 'OurRef'):
                fuzzy_columns.append(('OurRef', self._get_actual_column_name('OurRef')))
            
            # Check fuzzy matching for customer code (requires word boundaries)
            for col_name, actual_col_name in fuzzy_columns:
                for idx, value in enumerate(df[actual_col_name]):
                    if unallocated_mask.iloc[idx] and pd.notna(value) and isinstance(value, str):
                        value_upper = value.upper()
                        code_upper = customer_code.upper()
                        
                        # Code matching: Check if code appears with word boundaries (space, slash, start/end)
                        import re
                        code_pattern = r'\b' + re.escape(code_upper) + r'\b|/' + re.escape(code_upper) + r'[\s/]|[\s/]' + re.escape(code_upper) + r'/'
                        if re.search(code_pattern, value_upper):
                            fuzzy_matches.iloc[idx] = True
            
            # Check fuzzy matching for customer name (partial matching allowed)
            if customer_name and customer_name.strip():
                for col_name, actual_col_name in fuzzy_columns:
                    for idx, value in enumerate(df[actual_col_name]):
                        if unallocated_mask.iloc[idx] and pd.notna(value) and isinstance(value, str):
                            # CustomerName matching: Use partial ratio for full names
                            similarity = fuzz.partial_ratio(customer_name.upper(), value.upper())
                            if similarity >= 90:
                                fuzzy_matches.iloc[idx] = True
            
            if fuzzy_matches.any():
                customer_mask |= fuzzy_matches
                patterns = [customer_code]
                if customer_name and customer_name.strip():
                    patterns.append(customer_name)
                logging.info(f"[OpenOrdersReporting] Found {fuzzy_matches.sum()} fuzzy matches for patterns {patterns} in {[col[0] for col in fuzzy_columns]} (Code: word boundaries, Name: ≥90%)")
            
            if customer_mask.any():
                customer_orders = df[customer_mask & unallocated_mask].copy()
                if not customer_orders.empty:
                    allocated_orders[customer_code] = customer_orders
                    unallocated_mask &= ~customer_mask  # Remove allocated orders from unallocated
                    
                    customer_name = rule.get('customer_name', customer_code)
                    logging.info(f"[OpenOrdersReporting] Product-based separation allocated {len(customer_orders)} orders to {customer_code} ({customer_name})")
        
        # Orders that couldn't be allocated by product code
        remaining_orders = df[unallocated_mask].copy()
        
        total_allocated = sum(len(df) for df in allocated_orders.values())
        logging.info(f"[OpenOrdersReporting] Product-based separation complete: {total_allocated} allocated to {len(allocated_orders)} customers, {len(remaining_orders)} remain for universal pattern matching")
        
        return allocated_orders, remaining_orders

    def _apply_universal_pattern_matching(self, df: pd.DataFrame) -> tuple:
        """
        Apply universal pattern matching for ALL orders based on ShipAddress and OurRef columns.
        Allocates orders to customer files based on patterns found in ProductMapping configuration.
        
        Args:
            df: DataFrame containing all orders to process
            
        Returns:
            tuple: (allocated_orders_dict, remaining_orders_df)
                - allocated_orders_dict: {customer_code: allocated_dataframe}
                - remaining_orders_df: DataFrame of orders that couldn't be allocated
        """
        if df.empty:
            return {}, pd.DataFrame()
            
        logging.info(f"[OpenOrdersReporting] Starting universal pattern matching for {len(df)} orders")
        
        allocated_orders = {}
        unallocated_mask = pd.Series([True] * len(df), index=df.index)
        
        # Get columns to check for patterns (ShipAddress and OurRef - columns W and AF)
        check_columns = []
        if self._column_exists(df, 'ShipAddress'):
            check_columns.append(('ShipAddress', self._get_actual_column_name('ShipAddress')))
        if self._column_exists(df, 'OurRef'):
            check_columns.append(('OurRef', self._get_actual_column_name('OurRef')))
        
        # Build patterns from configuration (customer codes and names)
        allocation_patterns = {}
        for customer_code, rule in self.processing_rules.items():
            # Only consider customers with separate files enabled
            if not rule.get('create_separate_file', False):
                continue
                
            patterns = []
            # Add the customer code itself as a pattern
            patterns.append(customer_code)
            
            # Add customer name as pattern (if different from code)
            customer_name = rule.get('customer_name', '')
            if customer_name and customer_name != customer_code:
                patterns.append(customer_name)
                # Also add parts of customer name (e.g., "CALVARY" from "Calvary (Little Company...)")
                if '(' in customer_name:
                    base_name = customer_name.split('(')[0].strip()
                    if base_name and base_name != customer_code:
                        patterns.append(base_name)
                if ')' in customer_name:
                    parts = customer_name.replace('(', '').replace(')', '').split()
                    for part in parts:
                        if len(part) > 3:  # Only add meaningful words
                            patterns.append(part)
            
            allocation_patterns[customer_code] = patterns
        
        logging.info(f"[OpenOrdersReporting] Built universal allocation patterns from configuration: {allocation_patterns}")
        
        # For each customer that can have separate files
        for target_code, patterns in allocation_patterns.items():
            customer_mask = pd.Series([False] * len(df), index=df.index)
            
            # Check each pattern against all relevant columns
            for pattern in patterns:
                if len(pattern.strip()) < 2:  # Skip very short patterns
                    continue
                    
                for col_name, actual_col_name in check_columns:
                    pattern_match = df[actual_col_name].astype(str).str.contains(pattern, case=False, na=False)
                    if pattern_match.any():
                        customer_mask |= pattern_match
                        logging.info(f"[OpenOrdersReporting] Found {pattern_match.sum()} orders matching '{pattern}' in {col_name} for {target_code}")
            
            # Extract orders for this customer
            if customer_mask.any():
                customer_orders = df[customer_mask & unallocated_mask].copy()
                if not customer_orders.empty:
                    allocated_orders[target_code] = customer_orders
                    unallocated_mask &= ~customer_mask  # Remove allocated orders from unallocated
                    
                    customer_name = self.processing_rules.get(target_code, {}).get('customer_name', target_code)
                    logging.info(f"[OpenOrdersReporting] Allocated {len(customer_orders)} orders to {target_code} ({customer_name})")
        
        # Orders that couldn't be allocated to any customer
        remaining_orders = df[unallocated_mask].copy()
        
        total_allocated = sum(len(df) for df in allocated_orders.values())
        logging.info(f"[OpenOrdersReporting] Universal pattern matching complete: {total_allocated} allocated to {len(allocated_orders)} customers, {len(remaining_orders)} remain for OTHERS")
        
        return allocated_orders, remaining_orders
