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
        
    def process_excel_file(self, excel_file_bytes: bytes, filename: str = None, require_full_reporting: bool = True, split_calvary: bool = True) -> Dict[str, Any]:
        """
        Process an Excel file containing an Open Order Report.
        Returns statistics about the processing and uploads files to SharePoint.
        
        Parameters:
        - excel_file_bytes: The bytes of the Excel file to process
        - filename: The name of the input file
        - require_full_reporting: If True, keep all data in one file except removed duplicates. 
                                   If False, split into separate files.
        - split_calvary: If True, split Calvary records even when doing full reporting
                        (used during Calvary's first rollout period)
        """
        
        logging.info(f"[OpenOrdersReporting] Processing file: {filename}")
        logging.info(f"[OpenOrdersReporting] Full reporting required: {require_full_reporting}")
        logging.info(f"[OpenOrdersReporting] Split Calvary: {split_calvary}")
        logging.info(f"[OpenOrdersReporting] Data size: {len(excel_file_bytes)} bytes")
        
        if len(excel_file_bytes) > 10:
              logging.info(f"[OpenOrdersReporting] First 10 bytes: {excel_file_bytes[:10].hex()}")   
        
        # Statistics tracking
        stats = {
            'input_file': filename,
            'total_rows': 0,
            'generic_rows': 0,
            'calvary_rows': 0,
            'filtered_brand_rows': 0,
            'duplicate_orders_removed': 0,
            'remaining_rows': 0,
            'output_files': {},
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
            
            # Initialize dataframes that will be used for different processing paths
            calvary_df = pd.DataFrame()
            former_customers_df = pd.DataFrame()
            
            # 1. FIRST - Remove duplicates based on Order column
            if 'Order' in main_df.columns and not main_df.empty and self.dedup_customers:
                before_count = len(main_df)
                main_df = self._remove_duplicates_by_customer(main_df)
                after_count = len(main_df)
                stats['duplicate_orders_removed'] = before_count - after_count
                logging.info(f"[OpenOrdersReporting] Removed {stats['duplicate_orders_removed']} duplicate orders")

            
            # 2. SECOND - Remove former customers (official brands) as they don't need open order reporting
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
                    logging.info(f"[OpenOrdersReporting] Removed {len(former_customers_df)} former customer rows")
            
            # 3. THIRD - Add standard columns to the main dataframe
            main_df = self._add_checking_customer_columns(main_df)
            
            # 4. FOURTH - Handle generic sample processing without separating them
            # - We'll note the count for statistics only
            if product_num_column in main_df.columns:
                generic_exact_mask = main_df[product_num_column] == "GENERIC"
                generic_sample_mask = main_df[product_num_column].astype(str).str.contains("GENERIC-SAMPLE", case=False, na=False)
                
                generic_count = generic_exact_mask.sum()
                generic_sample_count = generic_sample_mask.sum()
                stats['generic_rows'] = generic_count + generic_sample_count
                
                if generic_count > 0 or generic_sample_count > 0:
                    logging.info(f"[OpenOrdersReporting] Found {generic_count} GENERIC and {generic_sample_count} GENERIC-SAMPLE rows (kept in main dataframe)")
            
            # 5. FIFTH - Split Calvary if required
            separate_customer_dfs = {}
            for customer_code in self.separate_file_customers:
                if product_num_column in main_df.columns:
                    # Match both prefix and exact matches
                    prefix_mask = main_df[product_num_column].astype(str).str.startswith(f"{customer_code}-", na=False)
                    exact_match_mask = main_df[product_num_column].astype(str) == customer_code
                    customer_mask = prefix_mask | exact_match_mask
                    
                    if customer_mask.any():
                        # Create a copy for this customer
                        customer_df = main_df[customer_mask].copy()
                        customer_df = self._add_checking_customer_columns(customer_df)
                        customer_df = self._apply_processing(customer_df, customer_code)
                        
                        # Store customer-specific dataframe
                        separate_customer_dfs[customer_code] = customer_df
                        
                        # Update statistics for specific customers
                        if customer_code in ['CLY', 'CAL']:
                            stats['calvary_rows'] += len(customer_df)
                        
                        # Remove these rows from main dataframe
                        main_df = main_df[~customer_mask].copy()
                        logging.info(f"[OpenOrdersReporting] Separated {len(customer_df)} {customer_code} rows")
            
            # Apply customer/checking note processing to main dataframe
            main_df = self._apply_processing(main_df, "OTHERS")
            stats['remaining_rows'] = len(main_df)
            
            # Save and upload CSV files
            today_filename_fmt = datetime.now().strftime("%Y%m%d")
            today_folder_fmt = datetime.now().strftime("%d-%m-%y")
            
            # Fix path formatting for SharePoint
            processed_date_dir = os.path.join(self.oor_output_path, today_folder_fmt).replace('\\', '/')
            
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            
            # Upload each file to SharePoint
            if not main_df.empty:
                # Name differs based on whether this is a full report or split files
                if require_full_reporting:
                    others_filename = f"OOR {today_filename_fmt}.csv"
                else:
                    others_filename = f"OTHERS OOR {today_filename_fmt}.csv"
                    
                others_path = f"{processed_date_dir}/{others_filename}"
                others_bytes = self._dataframe_to_csv_bytes(main_df)
                
                # Upload to SharePoint
                self.sharepoint_ops.upload_file_to_path(
                    drive_id=drive_id,
                    file_path=others_path,
                    file_name=others_filename,
                    file_bytes=others_bytes,
                    content_type="text/csv"
                )
                stats['output_files']['main'] = others_filename
            
            
            
            # Upload separate customer files
            for customer_code, customer_df in separate_customer_dfs.items():
                if not customer_df.empty:
                    customer_filename = f"{customer_code} OOR {today_filename_fmt}.csv"
                    customer_path = f"{processed_date_dir}/{customer_filename}"
                    customer_bytes = self._dataframe_to_csv_bytes(customer_df)
                    
                    # Upload to SharePoint
                    self.sharepoint_ops.upload_file_to_path(
                        drive_id=drive_id,
                        file_path=customer_path,
                        file_name=customer_filename,
                        file_bytes=customer_bytes,
                        content_type="text/csv"
                    )
                    stats['output_files'][customer_code.lower()] = customer_filename
                    logging.info(f"[OpenOrdersReporting] Uploaded separate file for {customer_code}: {customer_filename}")
                        
            # # Upload Calvary file if it was split out
            # if not calvary_df.empty:
            #     calvary_filename = f"CALVARY {today_filename_fmt}.csv"
            #     calvary_path = f"{processed_date_dir}/{calvary_filename}"
            #     calvary_bytes = self._dataframe_to_csv_bytes(calvary_df)
                
            #     # Upload to SharePoint
            #     self.sharepoint_ops.upload_file_to_path(
            #         drive_id=drive_id,
            #         file_path=calvary_path,
            #         file_name=calvary_filename,
            #         file_bytes=calvary_bytes, 
            #         content_type="text/csv"
            #     )
            #     stats['output_files']['calvary'] = calvary_filename
            
            # Note: Former customers data is intentionally not saved
            
            # Finalize stats
            stats['success'] = True
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            logging.info(f"[OpenOrdersReporting] Processing completed in {stats['duration']:.2f} seconds")
            
            return stats
            
        except Exception as e:
            logging.error(f"[OpenOrdersReporting] Error processing file: {str(e)}")
            raise
    
    # def _remove_order_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """Remove duplicate orders, keeping the first occurrence of each order"""
    #     if df.empty or 'Order' not in df.columns:
    #         return df
    #     
    #     logging.info(f"[OpenOrdersReporting] Removing duplicates based on Order column from dataframe with {len(df)} rows")
    #     
    #     # Get count before deduplication
    #     before_count = len(df)
    #     
    #     # Remove duplicates based on Order column
    #     df = df.drop_duplicates(subset=['Order'], keep='first')
    #     
    #     # Get count after deduplication
    #     after_count = len(df)
    #     
    #     logging.info(f"[OpenOrdersReporting] Removed {before_count - after_count} duplicate orders")
    #     
    #     return df
    
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