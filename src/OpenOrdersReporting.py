import pandas as pd
import os
import io
import logging
from datetime import datetime
import csv
from typing import Dict, List, Tuple, Any

from AzureOperations import AzureOperations
from SharePointOperations import SharePointOperations

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
        
        # Configure folder paths based on environment variables
        self.oor_input_prefix = os.environ.get('OOR_INPUT_PREFIX', 'OOR')
        self.oor_input_path = os.environ.get('OOR_INPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Upload')
        self.oor_output_path = os.environ.get('OOR_OUTPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed')
        
        # Define brand and mapping rules from original script
        self.official_brands = [
            'COA', 'BUP', 'CSR', 'CNR', 'BUS', 'CAL', 'IMB', 'JET',
            'JETSTAR', 'JS', 'NRMA', 'MTS', 'SCENTRE', 'SYD', 'RFDS', 'RFL'
        ]
        
        self.product_num_mapping = {
            'SAK': 'SHARKS AT KARELLA', 'BW': 'Busways', 'CS': 'Coal Services',
            'CAL': 'CALVARY', 'IMB': 'IMB', 'DC': 'Dolphins',
            'SG': 'ST George', 'CCC': 'CCC', 'DNA': 'DNATA', 'DOLP': 'DOLPHINS',
            'END': 'ESHS', 'GCL': 'GROWTH CIVIL LANDSCAPES', 'GYM': 'GYMEA TRADES',
            'RHH': 'REDHILL', 'RPA': 'REGAL REXNORR', 'SEL': 'SEASONS LIVING',
            'STAR': 'STAR AVIATION', 'YAE': 'YOUNG ACADEMICS', 'ZAM': 'ZAMBARERO',
            'STG': 'DRAGONS', 'KGT': 'KNIGHTS', 'SEL-SEASON': 'SEASON LIVING',
            'SGL': 'ST GEORGE LEAGUES', 'RRA': 'REGAL REXNORD', 'CRAIG SMITH': 'CRAIG SMITH',
            'TRADES GOLF CLUB': 'TRADES GOLF CLUB', 'MYTILENIAN': 'HOUSE',
            'BUS': 'BUSWAYS',     # Updated mapping
            'COA': 'Coal Services' # Updated mapping
        }
        
        self.taskqueue_mapping = {
            'Data Entry CHK': 'DATA ENTRY CHECK', 'CS HOLDING ORDERS': 'CS HOLD Q!',
            'CAL ROLLOUT DATES': 'CALL ROLLOUT DATE', 'CAL DISPATCH BY LOCATION': 'CAL DISPATCH BY LOCATION Q',
            'CANCEL ORDERS 2B DEL': 'CANCEL Q'
        }

    def process_excel_file(self, excel_file_bytes: bytes, filename: str = None) -> Dict[str, Any]:
        """
        Process an Excel file containing an Open Order Report.
        Returns statistics about the processing and uploads files to SharePoint.
        """
        
        logging.info(f"[OpenOrdersReporting] Processing file: {filename}")
        logging.info(f"[OpenOrdersReporting] Received data type: {type(excel_file_bytes)}")
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
            'remaining_rows': 0,
            'output_files': {},
            'start_time': datetime.now(),
        }
        
        try:
            # Read the Excel file from bytes
            excel_file = io.BytesIO(excel_file_bytes)
            logging.info(f"[OpenOrdersReporting] Created BytesIO object, attempting to read with pandas")
            df = pd.read_excel(excel_file, engine='openpyxl')
            stats['total_rows'] = len(df)
            logging.info(f"[OpenOrdersReporting] Successfully read Excel file with {len(df)} rows")
        
            
            # Prepare DataFrames for separation
            generic_df = pd.DataFrame()
            calvary_df = pd.DataFrame()
            former_customers_df = pd.DataFrame()
            main_df = df.copy()
            
            product_num_column = 'ProductNum'  # Key column for filtering
            
            # 1. Extract GENERIC orders
            if product_num_column in df.columns:
                generic_exact_mask = df[product_num_column] == "GENERIC"
                generic_sample_mask = df[product_num_column].astype(str).str.contains("GENERIC-SAMPLE", case=False, na=False)
                generic_mask = generic_exact_mask | generic_sample_mask
                if generic_mask.any():
                    generic_df = df[generic_mask].copy()
                    stats['generic_rows'] = len(generic_df)
                    main_df = main_df[~generic_mask].copy()
            else:
                logging.warning(f"Column '{product_num_column}' not found, skipping GENERIC extraction")
            
            # 2. Extract CAL orders
            if product_num_column in main_df.columns:
                cal_mask = main_df[product_num_column].astype(str).str.startswith('CAL-', na=False)
                if cal_mask.any():
                    calvary_df = main_df[cal_mask].copy()
                    stats['calvary_rows'] = len(calvary_df)
                    main_df = main_df[~cal_mask].copy()
            
            # 3. Extract official brands (to FORMER CUSTOMERS)
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
            
            stats['remaining_rows'] = len(main_df)
            
            # 4. Add standard columns to all DataFrames
            main_df = self._add_checking_customer_columns(main_df)
            generic_df = self._add_checking_customer_columns(generic_df)
            calvary_df = self._add_checking_customer_columns(calvary_df)
            former_customers_df = self._add_checking_customer_columns(former_customers_df)
            
            # 5. Apply processing to each DataFrame
            calvary_df = self._apply_processing(calvary_df, "CALVARY")
            main_df = self._apply_processing(main_df, "OTHERS")
            generic_df = self._apply_processing(generic_df, "GENERIC")
            former_customers_df = self._apply_processing(former_customers_df, "FORMER CUSTOMERS")
            
            # 6. Save and upload CSV files
            today_filename_fmt = datetime.now().strftime("%Y%m%d")
            today_folder_fmt = datetime.now().strftime("%d-%m-%y")
            
            # Fix path formatting for SharePoint
            processed_date_dir = os.path.join(self.oor_output_path, today_folder_fmt).replace('\\', '/')
            
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            
            # Upload each file to SharePoint
            if not main_df.empty:
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
                stats['output_files']['others'] = others_filename
            
            if not generic_df.empty:
                generic_filename = f"GENERIC SAMPLES {today_filename_fmt}.csv"
                generic_path = f"{processed_date_dir}/{generic_filename}"
                generic_bytes = self._dataframe_to_csv_bytes(generic_df)
                
                # Upload to SharePoint
                self.sharepoint_ops.upload_file_to_path(
                    drive_id=drive_id,
                    file_path=generic_path,
                    file_name=generic_filename,
                    file_bytes=generic_bytes,
                    content_type="text/csv"
                )
                stats['output_files']['generic'] = generic_filename
            
            if not calvary_df.empty:
                calvary_filename = f"CALVARY {today_filename_fmt}.csv"
                calvary_path = f"{processed_date_dir}/{calvary_filename}"
                calvary_bytes = self._dataframe_to_csv_bytes(calvary_df)
                
                # Upload to SharePoint
                self.sharepoint_ops.upload_file_to_path(
                    drive_id=drive_id,
                    file_path=calvary_path,
                    file_name=calvary_filename,
                    file_bytes=calvary_bytes, 
                    content_type="text/csv"
                )
                stats['output_files']['calvary'] = calvary_filename
            
            if not former_customers_df.empty:
                former_filename = f"FORMER CUSTOMERS {today_filename_fmt}.csv"
                former_path = f"{processed_date_dir}/{former_filename}"
                former_bytes = self._dataframe_to_csv_bytes(former_customers_df)
                
                # Upload to SharePoint
                self.sharepoint_ops.upload_file_to_path(
                    drive_id=drive_id,
                    file_path=former_path,
                    file_name=former_filename,
                    file_bytes=former_bytes,
                    content_type="text/csv"
                )
                stats['output_files']['former_customers'] = former_filename
            
            # Finalize stats
            stats['success'] = True
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            logging.info(f"[OpenOrdersReporting] Processing completed in {stats['duration']:.2f} seconds")
            
            return stats
            
        except Exception as e:
            logging.error(f"[OpenOrdersReporting] Error processing file: {str(e)}")
            raise
    
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
        taskqueue_column = 'TaskQueue'
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
        if taskqueue_column in df_to_process.columns:
            for task_value, note_value in self.taskqueue_mapping.items():
                mask = df_to_process[taskqueue_column].astype(str) == str(task_value)
                if mask.any():
                    # Apply note based on TaskQueue match
                    df_to_process.loc[mask, 'CHECKING NOTE'] = note_value
        
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