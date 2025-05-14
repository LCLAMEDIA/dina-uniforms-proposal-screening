import pandas as pd
import os
import io
import logging
from datetime import datetime
import csv
from typing import Dict, List, Tuple, Any
import re # Added for regex operations

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
        self.dedup_customers = self.config_reader.get_dedup_customers()
        
        # Create processing rules from configuration data
        self.processing_rules = {}
        for product_code, customer_name in self.product_num_mapping.items():
            self.processing_rules[product_code] = {
                'customer_name': customer_name,
                'create_separate_file': product_code in self.separate_file_customers,
                'remove_duplicates': product_code in self.dedup_customers
            }
        # Add detailed configuration logging
        logging.info("[OpenOrdersReporting] Loaded configuration values:")
        logging.info(f"[OpenOrdersReporting] - Official brands: {self.official_brands}")
        logging.info(f"[OpenOrdersReporting] - Product number mappings: {self.product_num_mapping}")
        logging.info(f"[OpenOrdersReporting] - Separate file customers: {self.separate_file_customers}")
        logging.info(f"[OpenOrdersReporting] - Deduplication customers: {self.dedup_customers}")
        
        # Configure folder paths based on environment variables
        self.oor_input_prefix = os.environ.get('OOR_INPUT_PREFIX', 'OOR')
        self.oor_input_path = os.environ.get('OOR_INPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Upload')
        self.oor_output_path = os.environ.get('OOR_OUTPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed')
        
        # After loading configuration values
        logging.info(f"[OpenOrdersReporting] Loaded configuration values:")
        logging.info(f"[OpenOrdersReporting] - Official brands: {self.official_brands}")
        logging.info(f"[OpenOrdersReporting] - Product number mappings: {self.product_num_mapping}")
        logging.info(f"[OpenOrdersReporting] - Separate file customers: {self.separate_file_customers}")
        logging.info(f"[OpenOrdersReporting] - Deduplication customers: {self.dedup_customers}")
        logging.info(f"[OpenOrdersReporting] - Processing rules: {self.processing_rules}")
    
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

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhanced duplicate removal using a composite key, normalization, and sorting.
        Keeps the latest record based on 'DateIssued' if duplicates are found.
        
        Args:
            df: DataFrame to process.
            
        Returns:
            DataFrame with duplicates removed based on the enhanced logic.
        """
        if df.empty:
            logging.info("[OpenOrdersReporting._remove_duplicates] Input DataFrame is empty, skipping deduplication.")
            return df
            
        before_count = len(df)
        logging.info(f"[OpenOrdersReporting._remove_duplicates] Starting enhanced deduplication. Rows before: {before_count}")
        
        # Create a working copy for adding normalized/parsed columns
        processed_df = df.copy()
        
        # --- Prepare columns for composite key ---
        actual_composite_key_cols = list(self.COMPOSITE_KEY_BASE_COLS) # Start with base columns

        # Normalize 'itemDescription' if it exists
        if 'itemDescription' in processed_df.columns:
            processed_df[self.NORMALIZED_ITEM_DESC_COL] = processed_df['itemDescription'].apply(self._normalize_string)
            actual_composite_key_cols.append(self.NORMALIZED_ITEM_DESC_COL)
        else:
            logging.warning("[OpenOrdersReporting._remove_duplicates] 'itemDescription' column not found. Deduplication might be less accurate.")

        # Parse 'Note' field if it exists
        if 'Note' in processed_df.columns:
            processed_df[self.PARSED_NOTE_ID_COL] = processed_df['Note'].apply(self._parse_note_for_id)
            actual_composite_key_cols.append(self.PARSED_NOTE_ID_COL)
        else:
            logging.warning("[OpenOrdersReporting._remove_duplicates] 'Note' column not found. Deduplication might be less accurate.")
        
        # Add other optional key columns if they exist
        for col_name in ['QID', 'PurchaseNumber', 'barcodeupc']: # barcodeupc can also be useful
            if col_name in processed_df.columns:
                actual_composite_key_cols.append(col_name)
            else:
                logging.info(f"[OpenOrdersReporting._remove_duplicates] Optional key column '{col_name}' not found. Skipping for composite key.")
        
        # Ensure all columns in actual_composite_key_cols exist in processed_df before using them
        # This is more of a safeguard; they should exist if added above or are base.
        final_key_cols_for_drop = [col for col in actual_composite_key_cols if col in processed_df.columns]
        if not final_key_cols_for_drop:
            logging.warning("[OpenOrdersReporting._remove_duplicates] No valid key columns found for deduplication. Returning original DataFrame.")
            return df
        
        logging.info(f"[OpenOrdersReporting._remove_duplicates] Using composite key columns: {final_key_cols_for_drop}")

        # --- Sort to keep the latest record ---
        # Sort by 'DateIssued' (descending) if available. Add other tie-breakers if needed.
        # Pandas handles NaT in sorting.
        if 'DateIssued' in processed_df.columns:
            logging.info("[OpenOrdersReporting._remove_duplicates] Sorting by 'DateIssued' (descending) to keep the latest record among duplicates.")
            # Ensure DateIssued is datetime for proper sorting, handle errors gracefully
            processed_df['DateIssued'] = pd.to_datetime(processed_df['DateIssued'], errors='coerce')
            processed_df.sort_values(by=['DateIssued'], ascending=[False], inplace=True, na_position='last')
        else:
            logging.warning("[OpenOrdersReporting._remove_duplicates] 'DateIssued' column not found. Cannot sort to keep latest; will keep first encountered.")

        # --- Drop duplicates based on the composite key ---
        # `keep='first'` on the sorted DataFrame retains the latest entry.
        # Pandas' drop_duplicates handles NaN values in subset columns correctly (NaN == NaN is true for this purpose).
        df_deduped_processed = processed_df.drop_duplicates(subset=final_key_cols_for_drop, keep='first')
        
        after_count = len(df_deduped_processed)
        removed_count = before_count - after_count # Note: this counts based on original 'before_count' of the input df slice
        
        if removed_count > 0:
            logging.info(f"[OpenOrdersReporting._remove_duplicates] Enhanced deduplication removed {removed_count} rows.")
        else:
            logging.info("[OpenOrdersReporting._remove_duplicates] No duplicate rows removed by enhanced deduplication.")
        logging.info(f"[OpenOrdersReporting._remove_duplicates] Rows after enhanced deduplication: {after_count}")
        
        # Return original rows corresponding to the kept indices from the processed DataFrame
        # This preserves original casing and formatting for non-key columns.
        original_indices_to_keep = df_deduped_processed.index
        result_df = df.loc[original_indices_to_keep].copy() # Use .loc for safety with potentially non-unique indices if df wasn't reset
        
        return result_df

    def _remove_duplicates_by_customer(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Identifies data segments for customers configured for deduplication and applies
        the enhanced deduplication logic to those segments.
        
        Args:
            df: DataFrame to process.
            
        Returns:
            DataFrame with duplicates removed for configured product codes/customers.
        """
        if df.empty:
            return df
        
        # Check if 'ProductNum' column exists, essential for this logic
        if 'ProductNum' not in df.columns:
            logging.warning("[OpenOrdersReporting._remove_duplicates_by_customer] 'ProductNum' column not found. Skipping customer-specific deduplication.")
            return df
        
        # Identify product codes that are configured for deduplication
        # self.dedup_customers stores product codes (e.g., 'SAK') that require deduplication
        product_codes_requiring_dedup = [pc for pc in self.dedup_customers if pc in self.processing_rules and self.processing_rules[pc].get('remove_duplicates', False)]

        if not product_codes_requiring_dedup:
            logging.info("[OpenOrdersReporting._remove_duplicates_by_customer] No customers/product codes configured for deduplication. Skipping.")
            return df
        
        logging.info(f"[OpenOrdersReporting._remove_duplicates_by_customer] Performing deduplication for product codes: {product_codes_requiring_dedup}")
        
        # Create a combined mask for all rows that belong to products needing deduplication
        combined_mask_for_dedup_products = pd.Series(False, index=df.index)
        for product_code in product_codes_requiring_dedup:
            # Match exact product code or product code as a prefix (e.g., SAK and SAK-123)
            exact_match = df['ProductNum'] == product_code
            prefix_match = df['ProductNum'].astype(str).str.startswith(f"{product_code}-", na=False)
            current_product_mask = exact_match | prefix_match
            combined_mask_for_dedup_products = combined_mask_for_dedup_products | current_product_mask
        
        # Split DataFrame into parts: one that needs deduplication, one that doesn't
        df_to_deduplicate_segment = df[combined_mask_for_dedup_products].copy()
        df_no_deduplication_needed_segment = df[~combined_mask_for_dedup_products].copy()
        
        deduplicated_segment = pd.DataFrame() # Initialize empty DataFrame for the deduplicated part

        if not df_to_deduplicate_segment.empty:
            logging.info(f"[OpenOrdersReporting._remove_duplicates_by_customer] Applying enhanced deduplication to {len(df_to_deduplicate_segment)} rows from configured products.")
            # Apply the enhanced _remove_duplicates method to the identified segment
            deduplicated_segment = self._remove_duplicates(df_to_deduplicate_segment)
            logging.info(f"[OpenOrdersReporting._remove_duplicates_by_customer] Rows in segment after deduplication: {len(deduplicated_segment)}. "
                         f"Removed: {len(df_to_deduplicate_segment) - len(deduplicated_segment)}")
        else:
            logging.info("[OpenOrdersReporting._remove_duplicates_by_customer] No rows found for products configured for deduplication.")
            
        # Concatenate the deduplicated segment (if any) with the segment that didn't need deduplication
        if not deduplicated_segment.empty:
            final_df = pd.concat([deduplicated_segment, df_no_deduplication_needed_segment], ignore_index=True)
        else: # If deduplicated_segment is empty (either no rows to dedup or all were deduped to empty)
            final_df = df_no_deduplication_needed_segment.copy()

        logging.info(f"[OpenOrdersReporting._remove_duplicates_by_customer] Total rows after customer-specific deduplication pass: {len(final_df)}")
        return final_df.reset_index(drop=True) # Reset index for clean DataFrame

    def process_excel_file(self, excel_file_bytes: bytes, filename: str = None) -> Dict[str, Any]:
        """
        Process an Excel file containing an Open Order Report.
        Returns statistics about the processing and uploads files to SharePoint.
        
        Parameters:
        - excel_file_bytes: The bytes of the Excel file to process
        - filename: The name of the input file
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
            'duplicate_rows_removed_by_customer_logic': 0, # Renamed for clarity
            'output_files': {},
            'product_counts': {},
            'start_time': datetime.now(),
        }
        
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
            main_df = df.copy()
            product_num_column = 'ProductNum'  # Key column for filtering
            
            # 1. FIRST - Apply deduplication based on product code configuration
            # This now calls the _remove_duplicates_by_customer which in turn calls the enhanced _remove_duplicates
            rows_before_customer_dedup = len(main_df)
            main_df = self._remove_duplicates_by_customer(main_df)
            rows_after_customer_dedup = len(main_df)
            stats['duplicate_rows_removed_by_customer_logic'] = rows_before_customer_dedup - rows_after_customer_dedup
            logging.info(f"[OpenOrdersReporting] Total duplicates removed by customer-specific logic: {stats['duplicate_rows_removed_by_customer_logic']}")
            
            # 2. SECOND - Filter out brands from the official brands list
            if product_num_column in main_df.columns:
                former_customers_mask = pd.Series(False, index=main_df.index)
                for brand in self.official_brands:
                    brand_prefix = f"{brand}-"
                    # Ensure ProductNum is string for startswith
                    prefix_mask = main_df[product_num_column].astype(str).str.startswith(brand_prefix, na=False)
                    if prefix_mask.any():
                        former_customers_mask = former_customers_mask | prefix_mask
                
                if former_customers_mask.any():
                    former_customers_df = main_df[former_customers_mask].copy()
                    stats['filtered_brand_rows'] = len(former_customers_df)
                    main_df = main_df[~former_customers_mask].copy()
                    logging.info(f"[OpenOrdersReporting] Removed {len(former_customers_df)} filtered brand rows")
            
            # 3. THIRD - Add standard columns to the main dataframe
            main_df = self._add_checking_customer_columns(main_df)
            
            # 4. FOURTH - Split data by product code based on configuration
            product_dataframes = {}
            remaining_df = main_df.copy()
            
            if product_num_column in remaining_df.columns:
                # Process each product code that has processing rules
                for product_code, rule in self.processing_rules.items():
                    # Skip if not configured to create a separate file
                    if not rule.get('create_separate_file', False):
                        continue
                    
                    # Create product code mask
                    exact_match = remaining_df[product_num_column] == product_code
                    prefix_match = remaining_df[product_num_column].astype(str).str.startswith(f"{product_code}-", na=False)
                    product_mask = exact_match | prefix_match
                    
                    if product_mask.any():
                        # Extract matching rows to a separate dataframe
                        product_df = remaining_df[product_mask].copy()
                        product_df = self._add_checking_customer_columns(product_df) # Add columns before specific processing
                        
                        # Apply customer name mapping
                        product_df = self._apply_processing(product_df, rule.get('customer_name', product_code)) # Use mapped name
                        
                        # Add to the product dataframes dictionary
                        product_dataframes[product_code] = product_df
                        
                        # Update stats
                        stats['product_counts'][product_code] = len(product_df)
                        
                        # Remove from the main dataframe
                        remaining_df = remaining_df[~product_mask].copy()
                        logging.info(f"[OpenOrdersReporting] Separated {len(product_df)} rows for {product_code} (Customer: {rule.get('customer_name', product_code)})")
            
            # 5. FIFTH - Process the remaining data
            # Apply customer/checking note processing to remaining dataframe
            remaining_df = self._apply_processing(remaining_df, "OTHERS") # Pass "OTHERS" as df_name
            stats['remaining_rows'] = len(remaining_df)
            
            # 6. SIXTH - Prepare for output
            # Save and upload CSV files
            today_filename_fmt = datetime.now().strftime("%Y%m%d")
            today_folder_fmt = datetime.now().strftime("%d-%m-%y")
            
            # Fix path formatting for SharePoint
            processed_date_dir = os.path.join(self.oor_output_path, today_folder_fmt).replace('\\', '/')
            
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            
            # 7. SEVENTH - Upload main file if it's not empty
            if not remaining_df.empty:
                # Check if we're generating separate files or just one main file
                if product_dataframes: # If product_dataframes has entries, this is the "OTHERS" file
                    others_filename = f"OTHERS OOR {today_filename_fmt}.csv"
                else: # If no product_dataframes, this is the main consolidated file
                    others_filename = f"OOR {today_filename_fmt}.csv"
                    
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
                stats['output_files']['main_or_others'] = others_filename # Clarified key
            
            # 8. EIGHTH - Upload product-specific files
            for product_code, product_df in product_dataframes.items():
                if product_df.empty:
                    continue
                    
                # Get customer name from processing rules
                customer_name = self.processing_rules[product_code].get('customer_name', product_code)
                product_filename = f"{customer_name} OOR {today_filename_fmt}.csv" # Added OOR for consistency
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
                stats['output_files'][customer_name] = product_filename # Use customer_name as key for stats
            
            # Finalize stats
            stats['success'] = True
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            logging.info(f"[OpenOrdersReporting] Processing completed in {stats['duration']:.2f} seconds. Stats: {stats}")
            
            return stats
            
        except Exception as e:
            logging.error(f"[OpenOrdersReporting] Error processing file: {str(e)}", exc_info=True) # Added exc_info for traceback
            # Populate stats with error info
            stats['success'] = False
            stats['error_message'] = str(e)
            stats['end_time'] = datetime.now()
            if 'start_time' in stats: # Ensure start_time was set
                stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            raise # Re-raise the exception after logging
    
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

        product_num_column = 'ProductNum'
        date_issued_column = 'DateIssued'
        
        # --- Customer Name Population ---
        # df_name is now the target customer name for specific files (e.g. "CALVARY") or "OTHERS"
        if df_name != "OTHERS" and df_name in self.product_num_mapping.values(): # If df_name is a mapped customer name
             df_to_process['CUSTOMER'] = df_name
        elif df_name == "FORMER CUSTOMERS": # This case might be redundant if official_brands are filtered out earlier
            if product_num_column in df_to_process.columns:
                for index, row in df_to_process.iterrows():
                    product_num_val = row.get(product_num_column)
                    if pd.notna(product_num_val):
                        product_num_str = str(product_num_val)
                        brand_prefix = product_num_str.split('-')[0] if '-' in product_num_str else product_num_str
                        if brand_prefix in self.official_brands: # official_brands contains product codes like 'BIS'
                             df_to_process.at[index, 'CUSTOMER'] = self.product_num_mapping.get(brand_prefix, brand_prefix)
        elif product_num_column in df_to_process.columns:  # For OTHERS, try to map based on ProductNum
            for index, row in df_to_process.iterrows():
                # Only attempt to fill CUSTOMER if it's currently empty or matches a generic placeholder
                current_customer = str(row.get('CUSTOMER', '')).strip()
                if current_customer == '' or current_customer == 'OTHERS' or current_customer.startswith('N/A'):
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
                        # Fallback for SAK if not specifically mapped but ProductNum starts with SAK-
                        if not matched_by_prefix and product_num_str.startswith('SAK-') and 'SAK' not in self.product_num_mapping:
                             df_to_process.at[index, 'CUSTOMER'] = 'SHARKS AT KARELLA' # Default if SAK is not in mapping
                        elif not matched_by_prefix and current_customer == '': # If still no match, mark as OTHERS explicitly
                             df_to_process.at[index, 'CUSTOMER'] = 'OTHERS'


        # --- Add < 5 DAYS OLD checking note based on DateIssued ---
        if date_issued_column in df_to_process.columns:
            today = datetime.now().date() # Use .now().date() for date comparison
            
            for index, row in df_to_process.iterrows():
                date_issued_val = row.get(date_issued_column)
                
                if pd.notna(date_issued_val):
                    try:
                        # Attempt to parse date_issued_val if it's a string
                        if isinstance(date_issued_val, str):
                            # Try common formats; extend this list if necessary
                            parsed_date = None
                            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
                                try:
                                    parsed_date = datetime.strptime(date_issued_val, fmt).date()
                                    break
                                except ValueError:
                                    continue
                            if parsed_date is None and hasattr(pd.to_datetime(date_issued_val, errors='coerce'), 'date'): # Pandas to_datetime as fallback
                                parsed_date = pd.to_datetime(date_issued_val, errors='coerce').date()

                        elif hasattr(date_issued_val, 'date'): # If it's already a datetime-like object (Timestamp)
                            parsed_date = date_issued_val.date()
                        elif isinstance(date_issued_val, datetime.date): # If it's already a date object
                            parsed_date = date_issued_val
                        else:
                            parsed_date = None
                            logging.warning(f"[OpenOrdersReporting._apply_processing] DateIssued '{date_issued_val}' is of unhandled type {type(date_issued_val)}.")

                        if parsed_date and isinstance(parsed_date, datetime.date): # Check if parsed_date is a valid date object
                            days_diff = (today - parsed_date).days
                            if days_diff < 5 and days_diff >= 0: # Also ensure it's not a future date
                                current_note = str(row.get('CHECKING NOTE', '')).strip()
                                new_suffix = "< 5 DAYS OLD"
                                if new_suffix not in current_note: # Avoid duplicate suffixes
                                    if current_note:
                                        df_to_process.at[index, 'CHECKING NOTE'] = f"{current_note} {new_suffix}"
                                    else:
                                        df_to_process.at[index, 'CHECKING NOTE'] = new_suffix
                        elif parsed_date is None and isinstance(date_issued_val, str) : # if original was string and parsing failed
                             logging.warning(f"[OpenOrdersReporting._apply_processing] Could not parse DateIssued string '{date_issued_val}' for < 5 DAYS OLD check.")

                    except Exception as e: # Catch broader exceptions during date processing
                        logging.warning(f"[OpenOrdersReporting._apply_processing] Error processing DateIssued '{date_issued_val}' for < 5 DAYS OLD check: {str(e)}")
        
        return df_to_process
    
    def _extract_product_code(self, product_num: str) -> str:
        """Extract the product code from a product number (e.g., 'SAK' from 'SAK-XYZ')."""
        # This method is kept for potential utility but is not directly used by the new deduplication logic.
        if not product_num or not isinstance(product_num, str):
            return ""
        
        # If there's a hyphen, get everything before it
        if "-" in product_num:
            return product_num.split("-")[0]
        
        # Otherwise return the whole string
        return product_num

