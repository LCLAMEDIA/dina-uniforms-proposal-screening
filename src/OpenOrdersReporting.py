import pandas as pd
import os
import io
import logging
from datetime import datetime
import csv
from typing import Dict, List, Tuple, Any

from AzureOperations import AzureOperations
from SharePointOperations import SharePointOperations
from  ConfigurationReader import ConfigurationReader

class OpenOrdersReporting:
    """
    A class for processing Open Order Reports and saving them to SharePoint.
    Based on the original OOR processing script with SharePoint integration.
    """

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
        
    # Update to the process_excel_file method to use the enhanced deduplication

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
            'duplicate_rows_removed': 0,
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
            before_dedup_count = len(main_df)
            main_df = self._remove_duplicates_by_customer(main_df)
            after_dedup_count = len(main_df)
            stats['duplicate_rows_removed'] = before_dedup_count - after_dedup_count
            logging.info(f"[OpenOrdersReporting] Total duplicates removed: {stats['duplicate_rows_removed']}")
            
            # 2. SECOND - Filter out brands from the official brands list
            if product_num_column in main_df.columns:
                former_customers_mask = pd.Series(False, index=main_df.index)
                for brand in self.official_brands:
                    brand_prefix = f"{brand}-"
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
                        product_df = self._add_checking_customer_columns(product_df)
                        
                        # Apply customer name mapping
                        product_df = self._apply_processing(product_df, product_code)
                        
                        # Add to the product dataframes dictionary
                        product_dataframes[product_code] = product_df
                        
                        # Update stats
                        stats['product_counts'][product_code] = len(product_df)
                        
                        # Remove from the main dataframe
                        remaining_df = remaining_df[~product_mask].copy()
                        logging.info(f"[OpenOrdersReporting] Separated {len(product_df)} {product_code} rows")
            
            # 5. FIFTH - Process the remaining data
            # Apply customer/checking note processing to remaining dataframe
            remaining_df = self._apply_processing(remaining_df, "OTHERS")
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
                if product_dataframes:
                    others_filename = f"OTHERS OOR {today_filename_fmt}.csv"
                else:
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
                stats['output_files']['main'] = others_filename
            
            # 8. EIGHTH - Upload product-specific files
            for product_code, product_df in product_dataframes.items():
                if product_df.empty:
                    continue
                    
                # Get customer name from processing rules
                customer_name = self.processing_rules[product_code].get('customer_name', product_code)
                product_filename = f"{customer_name} {today_filename_fmt}.csv"
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
                stats['output_files'][product_code] = product_filename
            
            # Finalize stats
            stats['success'] = True
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            logging.info(f"[OpenOrdersReporting] Processing completed in {stats['duration']:.2f} seconds")
            
            return stats
            
        except Exception as e:
            logging.error(f"[OpenOrdersReporting] Error processing file: {str(e)}")
            raise
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhanced duplicate removal that properly compares all columns.
        This considers all columns after stripping whitespace to determine if rows are truly duplicates.
        
        Args:
            df: DataFrame to process
            
        Returns:
            DataFrame with duplicates removed
        """
        if df.empty:
            return df
            
        # Get the row count before deduplication
        before_count = len(df)
        
        # Create a copy to avoid modifying the original
        processed_df = df.copy()
        
        # Process string columns to strip whitespace
        for col in processed_df.columns:
            if processed_df[col].dtype == 'object':  # Only process string columns
                processed_df[col] = processed_df[col].astype(str).str.strip()
        
        # Drop exact duplicates (comparing all columns)
        # This will only remove rows that are exact duplicates across ALL columns
        df_deduped = processed_df.drop_duplicates(keep='first')
        
        # Get row count after deduplication
        after_count = len(df_deduped)
        removed_count = before_count - after_count
        
        if removed_count > 0:
            logging.info(f"[OpenOrdersReporting] Removed {removed_count} exact duplicate rows (all columns compared)")
        
        # Map back the indices to the original dataframe
        # We need to preserve the original data (not the whitespace-stripped version)
        original_indices = df_deduped.index
        result_df = df.iloc[original_indices].copy()
        
        return result_df

    def _remove_duplicates_by_customer(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate orders based on customer configuration.
        Only applies to product codes configured for deduplication.
        Compares all columns to determine if rows are truly duplicates.
        
        Args:
            df: DataFrame to process
            
        Returns:
            DataFrame with duplicates removed for configured product codes
        """
        if df.empty or 'ProductNum' not in df.columns:
            return df
        
        # Track product codes that need deduplication
        dedup_product_codes = []
        for code, rule in self.processing_rules.items():
            if rule.get('remove_duplicates', False):
                dedup_product_codes.append(code)
        
        # Skip if no products need deduplication
        if not dedup_product_codes:
            return df
        
        logging.info(f"[OpenOrdersReporting] Performing deduplication for product codes: {dedup_product_codes}")
        
        # Create masks for each product code that needs deduplication
        dedup_masks = {}
        for code in dedup_product_codes:
            # Match both exact code and code-prefixed values
            exact_match = df['ProductNum'] == code
            prefix_match = df['ProductNum'].astype(str).str.startswith(f"{code}-", na=False)
            dedup_masks[code] = exact_match | prefix_match
        
        # Combine all product code masks
        combined_mask = pd.Series(False, index=df.index)
        for mask in dedup_masks.values():
            combined_mask = combined_mask | mask
        
        # Split dataframe into parts that need deduplication and parts that don't
        to_dedup_df = df[combined_mask].copy()
        no_dedup_df = df[~combined_mask].copy()
        
        # Apply deduplication to the part that needs it
        if not to_dedup_df.empty:
            deduped_df = self._remove_duplicates(to_dedup_df)
            logging.info(f"[OpenOrdersReporting] Removed {len(to_dedup_df) - len(deduped_df)} duplicates from products configured for deduplication")
            
            # Combine the deduped part with the part that didn't need deduplication
            result_df = pd.concat([deduped_df, no_dedup_df])
            return result_df
        
        # If nothing to deduplicate, return original dataframe
        return df
    
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
        header_line = csv_content.split('\n')[0] if '\n' in csv_content else csv_content
        logging.info(f"[OpenOrdersReporting] CSV header line: {header_line}")
        
        return csv_content.encode('utf-8')
    
    def _add_checking_customer_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add 'CHECKING NOTE' and 'CUSTOMER' columns to the dataframe."""
        modified_df = df.copy()
        
        # Remove any Unnamed columns that might have been created
        unnamed_cols = [col for col in modified_df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            logging.info(f"[OpenOrdersReporting] Removing unnamed columns before adding CHECKING NOTE and CUSTOMER: {unnamed_cols}")
            modified_df = modified_df.drop(columns=unnamed_cols)
        
        # Ensure CHECKING NOTE is the first column
        if 'CHECKING NOTE' not in modified_df.columns:
            modified_df.insert(0, 'CHECKING NOTE', '')
        elif list(modified_df.columns).index('CHECKING NOTE') != 0:
            cols = list(modified_df.columns)
            cols.remove('CHECKING NOTE')
            cols.insert(0, 'CHECKING NOTE')
            modified_df = modified_df[cols]
        
        # Ensure CUSTOMER is the second column
        if 'CUSTOMER' not in modified_df.columns:
            modified_df.insert(1, 'CUSTOMER', '')
        elif list(modified_df.columns).index('CUSTOMER') != 1:
            cols = list(modified_df.columns)
            cols.remove('CUSTOMER')
            cols.insert(1, 'CUSTOMER')
            modified_df = modified_df[cols]
        
        return modified_df
    
    def _apply_processing(self, df_to_process: pd.DataFrame, df_name: str) -> pd.DataFrame:
        """Applies customer name and checking note logic"""
        if df_to_process.empty:
            return df_to_process
        
        product_num_column = 'ProductNum'
        # taskqueue_column = 'TaskQueue'  # Commented out as not needed
        date_issued_column = 'DateIssued'
        
        # --- Customer Name Population ---
        if df_name == "CALVARY":
            df_to_process['CUSTOMER'] = 'CALVARY'
        elif df_name == "FORMER CUSTOMERS":
            if product_num_column in df_to_process.columns:
                for index, row in df_to_process.iterrows():
                    product_num_val = row.get(product_num_column)
                    if pd.notna(product_num_val):
                        product_num = str(product_num_val)
                        brand_prefix = product_num.split('-')[0] if '-' in product_num else product_num
                        if brand_prefix in self.official_brands:
                            df_to_process.at[index, 'CUSTOMER'] = self.product_num_mapping.get(brand_prefix, brand_prefix)
        elif product_num_column in df_to_process.columns:  # For OTHERS and GENERIC
            for index, row in df_to_process.iterrows():
                product_num_val = row.get(product_num_column)
                if pd.notna(product_num_val):
                    product_num = str(product_num_val)
                    if product_num in self.product_num_mapping:
                        df_to_process.at[index, 'CUSTOMER'] = self.product_num_mapping[product_num]
                        continue
                    matched = False
                    for prefix, value in self.product_num_mapping.items():
                        if product_num.startswith(prefix + '-') or product_num.startswith(prefix):
                            df_to_process.at[index, 'CUSTOMER'] = value
                            matched = True
                            break
                    if not matched and product_num.startswith('SAK-'):
                        df_to_process.at[index, 'CUSTOMER'] = 'SHARKS AT KARELLA'
        
        # --- CHECKING NOTE Population (TaskQueue ONLY) --- 
        # Commented out as not needed
        # if taskqueue_column in df_to_process.columns:
        #     for task_value, note_value in self.taskqueue_mapping.items():
        #         mask = df_to_process[taskqueue_column].astype(str) == str(task_value)
        #         if mask.any():
        #             # Apply note based on TaskQueue match
        #             df_to_process.loc[mask, 'CHECKING NOTE'] = note_value
        
        # --- Add < 5 DAYS OLD checking note based on DateIssued ---
        if date_issued_column in df_to_process.columns:
            today = datetime.now().date()
            
            for index, row in df_to_process.iterrows():
                date_issued_val = row.get(date_issued_column)
                
                if pd.notna(date_issued_val):
                    try:
                        # Parse the date in DD/MM/YYYY format
                        if isinstance(date_issued_val, str):
                            date_issued = datetime.strptime(date_issued_val, "%d/%m/%Y").date()
                        else:
                            # If it's already a datetime or timestamp
                            date_issued = date_issued_val.date() if hasattr(date_issued_val, 'date') else date_issued_val
                        
                        # Calculate days difference
                        days_diff = (today - date_issued).days
                        
                        # Apply the checking note if less than 5 days old
                        if days_diff < 5:
                            current_note = row.get('CHECKING NOTE', '')
                            if current_note:
                                df_to_process.at[index, 'CHECKING NOTE'] = f"{current_note} < 5 DAYS OLD"
                            else:
                                df_to_process.at[index, 'CHECKING NOTE'] = "< 5 DAYS OLD"
                    
                    except (ValueError, TypeError) as e:
                        # Log but don't raise exception for date parsing errors
                        logging.warning(f"[OpenOrdersReporting] Error parsing date '{date_issued_val}': {str(e)}")
        
        return df_to_process
    
    def _extract_product_code(self, product_num: str) -> str:
        """Extract the product code from a product number."""
        if not product_num or not isinstance(product_num, str):
            return ""
        
        # If there's a hyphen, get everything before it
        if "-" in product_num:
            return product_num.split("-")[0]
        
        # Otherwise return the whole string
        return product_num

    def _remove_duplicates_by_customer(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate orders based on customer configuration."""
        if df.empty or 'Order' not in df.columns or 'ProductNum' not in df.columns:
            return df
        
        # Skip if no customers need deduplication
        if not self.dedup_customers:
            return df
        
        logging.info(f"[OpenOrdersReporting] Performing deduplication for customers: {self.dedup_customers}")
        
        # Track seen orders per customer
        seen_orders = {}
        rows_to_drop = []
        
        # Group by Order to find duplicates
        order_groups = df.groupby('Order')
        
        for order_id, order_group in order_groups:
            if len(order_group) <= 1:
                continue  # No duplicates for this order
            
            # Keep track of customer codes seen for this order
            order_customer_codes = set()
            
            # Process each row in this order group
            for idx, row in order_group.iterrows():
                product_num = row.get('ProductNum', '')
                product_code = self._extract_product_code(str(product_num))
                
                # If customer needs deduplication and we've seen this customer-order combo
                if product_code in self.dedup_customers:
                    if product_code in order_customer_codes:
                        rows_to_drop.append(idx)
                    else:
                        order_customer_codes.add(product_code)
        
        # Drop identified duplicate rows
        if rows_to_drop:
            result_df = df.drop(rows_to_drop)
            logging.info(f"[OpenOrdersReporting] Removed {len(rows_to_drop)} duplicate orders")
            return result_df
        
        return df