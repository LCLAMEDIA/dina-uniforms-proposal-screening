import pandas as pd
import os
import io
import logging
from datetime import datetime
import csv
from typing import Dict, List, Tuple, Any

from AzureOperations import AzureOperations
from SharePointOperations import SharePointOperations
from SharePointConfigReader import SharePointConfigReader

class OpenOrdersReporting:
    """
    A class for processing Open Order Reports and saving them to SharePoint.
    Now fully driven by dynamic configuration without hard-coded business rules.
    """

    def __init__(self):
        logging.info("[OpenOrdersReporting] Initializing OpenOrdersReporting")
        # Initialize Azure and SharePoint connections
        self.azure_ops = AzureOperations()
        access_token = self.azure_ops.get_access_token()
        logging.info("[OpenOrdersReporting] Successfully obtained Azure access token")
        
        self.sharepoint_ops = SharePointOperations(access_token=access_token)
        logging.info("[OpenOrdersReporting] Initialized SharePoint operations")
        
        # Configure folder paths based on environment variables
        self.oor_input_prefix = os.environ.get('OOR_INPUT_PREFIX', 'OOR')
        self.oor_input_path = os.environ.get('OOR_INPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Upload')
        self.oor_output_path = os.environ.get('OOR_OUTPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed')
        self.config_prefix = os.environ.get('OOR_CONFIG_PREFIX', 'OOR_CONFIG')
        self.config_path = os.environ.get('OOR_CONFIG_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/')
        
        logging.info(f"[OpenOrdersReporting] Using config prefix: {self.config_prefix}")
        logging.info(f"[OpenOrdersReporting] Using config path: {self.config_path}")
        
        # Load configuration from SharePoint
        self.config_reader = SharePointConfigReader(self.sharepoint_ops, self.config_prefix)
        site_id = self.sharepoint_ops.get_site_id()
        drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
        logging.info(f"[OpenOrdersReporting] Got SharePoint site_id: {site_id} and drive_id: {drive_id}")
        
        config_loaded = self.config_reader.load_config(drive_id)
        logging.info(f"[OpenOrdersReporting] Configuration loaded successfully: {config_loaded}")
        
        # Load configurations or use defaults
        if config_loaded:
            self.official_brands = self.config_reader.get_official_brands()
            self.product_num_mapping = self.config_reader.get_product_num_mapping()
            self.taskqueue_mapping = self.config_reader.get_taskqueue_mapping()
            self.processing_rules = self.config_reader.get_processing_rules()
            logging.info(f"[OpenOrdersReporting] Loaded {len(self.official_brands)} official brands")
            logging.info(f"[OpenOrdersReporting] Loaded {len(self.product_num_mapping)} product mappings")
            logging.info(f"[OpenOrdersReporting] Loaded {len(self.taskqueue_mapping)} task queue mappings")
            logging.info(f"[OpenOrdersReporting] Loaded {len(self.processing_rules)} processing rules")
        else:
            logging.warning("[OpenOrdersReporting] Using default configuration values")
            # Use default values as fallback
            self.official_brands = [
                'COA', 'BUP', 'CSR', 'CNR', 'BUS', 'CAL', 'IMB', 'JET',
                'JETSTAR', 'JS', 'NRMA', 'MTS', 'SCENTRE', 'SYD', 'RFDS', 'RFL'
            ]
            
            self.product_num_mapping = {
                'SAK': 'SHARKS AT KARELLA', 'BW': 'Busways', 'CS': 'Coal Services',
                'CAL': 'CALVARY', 'IMB': 'IMB', 'DC': 'Dolphins',
                'SG': 'ST George', 'CCC': 'CCC', 'DNA': 'DNATA', 'DOLP': 'DOLPHINS'
            }
            
            self.taskqueue_mapping = {
                'Data Entry CHK': 'DATA ENTRY CHECK', 'CS HOLDING ORDERS': 'CS HOLD Q!',
                'CAL ROLLOUT DATES': 'CALL ROLLOUT DATE', 'CAL DISPATCH BY LOCATION': 'CAL DISPATCH BY LOCATION Q'
            }
            
            # Default processing rules based on hard-coded values
            self.processing_rules = {}
            # Add default rules for product codes
            for code, name in self.product_num_mapping.items():
                self.processing_rules[code] = {
                    'customer_name': name,
                    'create_separate_file': False,
                    'remove_duplicates': False
                }
            
            # Set a couple of special cases
            if 'CAL' in self.processing_rules:
                self.processing_rules['CAL']['create_separate_file'] = True
                self.processing_rules['CAL']['remove_duplicates'] = True
                
            if 'GENERIC' not in self.processing_rules:
                self.processing_rules['GENERIC'] = {
                    'customer_name': 'GENERIC',
                    'create_separate_file': False,
                    'remove_duplicates': False
                }

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
        
        try:
            # Read the Excel file from bytes
            excel_file = io.BytesIO(excel_file_bytes)
            logging.info(f"[OpenOrdersReporting] Created BytesIO object, attempting to read with pandas")
            
            # Read Excel file without creating an index
            df = pd.read_excel(excel_file, engine='openpyxl', index_col=None)
            
            # Log the column names to verify
            logging.info(f"[OpenOrdersReporting] Excel columns: {list(df.columns)}")
            
            # Log first few rows for debugging
            logging.info(f"[OpenOrdersReporting] First 3 rows of data:\n{df.head(3).to_string()}")
            
            # Clean up any 'Unnamed' columns if they exist
            unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
            if unnamed_cols:
                logging.info(f"[OpenOrdersReporting] Removing unnamed columns: {unnamed_cols}")
                df = df.drop(columns=unnamed_cols)
            
            stats = {
                'input_file': filename,
                'total_rows': 0,
                'filtered_brand_rows': 0,
                'duplicate_rows_removed': 0,
                'output_files': {},
                'product_counts': {},
                'start_time': datetime.now(),
            }
            
            stats['total_rows'] = len(df)
            logging.info(f"[OpenOrdersReporting] Successfully read Excel file with {len(df)} rows")
        
            # Main dataframe to process
            main_df = df.copy()
            product_num_column = 'ProductNum'  # Key column for filtering
            
            # 1. FIRST - Filter out brands from the official brands list
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
            
            # 2. SECOND - Add standard columns to the main dataframe
            main_df = self._add_checking_customer_columns(main_df)
            
            # 3. THIRD - Split data by product code based on configuration
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
                        
                        # Apply customer name and remove duplicates if configured
                        product_df = self._apply_processing(
                            product_df, 
                            product_code, 
                            remove_duplicates=rule.get('remove_duplicates', False)
                        )
                        
                        # Add to the product dataframes dictionary
                        product_dataframes[product_code] = product_df
                        
                        # Update stats
                        stats['product_counts'][product_code] = len(product_df)
                        
                        # Remove from the main dataframe
                        remaining_df = remaining_df[~product_mask].copy()
                        logging.info(f"[OpenOrdersReporting] Separated {len(product_df)} {product_code} rows")
            
            # 4. FOURTH - Process the remaining data
            # Apply customer/checking note processing to remaining dataframe
            remaining_df = self._apply_processing(remaining_df, "OTHERS")
            stats['remaining_rows'] = len(remaining_df)
            
            # 5. FIFTH - Prepare for output
            # Save and upload CSV files
            today_filename_fmt = datetime.now().strftime("%Y%m%d")
            today_folder_fmt = datetime.now().strftime("%d-%m-%y")
            
            # Fix path formatting for SharePoint
            processed_date_dir = os.path.join(self.oor_output_path, today_folder_fmt).replace('\\', '/')
            
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            
            # 6. SIXTH - Upload main file if it's not empty
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
            
            # 7. SEVENTH - Upload product-specific files
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
        Remove exact duplicate rows from the DataFrame.
        This considers all columns to determine if rows are truly duplicates.
        
        Args:
            df: DataFrame to process
            
        Returns:
            DataFrame with duplicates removed
        """
        if df.empty:
            return df
            
        # Get the row count before deduplication
        before_count = len(df)
        
        # Drop exact duplicates (using all columns)
        df_deduped = df.drop_duplicates(keep='first')
        
        # Get row count after deduplication
        after_count = len(df_deduped)
        removed_count = before_count - after_count
        
        if removed_count > 0:
            logging.info(f"[OpenOrdersReporting] Removed {removed_count} exact duplicate rows")
            
        return df_deduped
    
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
    
    def _apply_processing(self, df_to_process: pd.DataFrame, product_code: str, remove_duplicates: bool = False) -> pd.DataFrame:
        """
        Applies customer name, checking note logic, and optional duplicate removal.
        
        Args:
            df_to_process: DataFrame to process
            product_code: The product code to process
            remove_duplicates: If True, removes exact duplicates from the DataFrame
            
        Returns:
            Processed DataFrame
        """
        if df_to_process.empty:
            return df_to_process
        
        product_num_column = 'ProductNum'
        date_issued_column = 'DateIssued'
        
        # First apply duplicate removal if requested
        if remove_duplicates:
            initial_count = len(df_to_process)
            df_to_process = self._remove_duplicates(df_to_process)
            removed_count = initial_count - len(df_to_process)
            if removed_count > 0:
                logging.info(f"[OpenOrdersReporting] Removed {removed_count} duplicates for {product_code}")
        
        # --- Customer Name Population ---
        if product_code in self.processing_rules:
            # For specific product codes with rules
            rule = self.processing_rules[product_code]
            df_to_process['CUSTOMER'] = rule['customer_name']
        elif product_code == "OTHERS" and product_num_column in df_to_process.columns:
            # For rows without a specific product code match, try to map based on product number prefix
            for index, row in df_to_process.iterrows():
                product_num_val = row.get(product_num_column)
                if pd.notna(product_num_val):
                    product_num = str(product_num_val)
                    # First check exact match
                    if product_num in self.product_num_mapping:
                        df_to_process.at[index, 'CUSTOMER'] = self.product_num_mapping[product_num]
                        continue
                    
                    # Then check prefix match
                    code_matched = False
                    for code, name in self.product_num_mapping.items():
                        if product_num.startswith(f"{code}-"):
                            df_to_process.at[index, 'CUSTOMER'] = name
                            code_matched = True
                            break
                    
                    # For product numbers that don't match any known prefix, use the product number itself
                    if not code_matched:
                        if '-' in product_num:
                            prefix = product_num.split('-')[0]
                            df_to_process.at[index, 'CUSTOMER'] = prefix
                        else:
                            df_to_process.at[index, 'CUSTOMER'] = product_num
        
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