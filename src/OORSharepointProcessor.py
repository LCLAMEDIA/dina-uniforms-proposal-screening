import pandas as pd
import os
import logging
from datetime import datetime, timedelta
import io
import csv
import sys

# Import the SharePoint and Azure operations classes
from AzureOperations import AzureOperations
from SharePointOperations import SharePointOperations

def setup_logging(log_file=None):
    """Set up logging configuration to output to both console and file."""
    if log_file is None:
        today = datetime.now().strftime("%Y%m%d")
        log_file = f"OOR_PROCESS_{today}.log"

    # Create logger
    logger = logging.getLogger('OOR_Processor')
    logger.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Clear any existing handlers
    logger.handlers = []

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger, log_file

def add_checking_customer_columns(df):
    """Add 'CHECKING NOTE' and 'CUSTOMER' columns to the dataframe."""
    # Create a copy of the dataframe to avoid modifying the original
    modified_df = df.copy()

    # Add CHECKING NOTE column if it doesn't exist or move it to first position
    if 'CHECKING NOTE' not in modified_df.columns:
        modified_df.insert(0, 'CHECKING NOTE', '')
    elif list(modified_df.columns).index('CHECKING NOTE') != 0:
        cols = list(modified_df.columns)
        cols.remove('CHECKING NOTE')
        cols.insert(0, 'CHECKING NOTE')
        modified_df = modified_df[cols]

    # Add CUSTOMER column if it doesn't exist or move it to second position
    if 'CUSTOMER' not in modified_df.columns:
        modified_df.insert(1, 'CUSTOMER', '')
    elif list(modified_df.columns).index('CUSTOMER') != 1:
        cols = list(modified_df.columns)
        cols.remove('CUSTOMER')
        cols.insert(1, 'CUSTOMER')
        modified_df = modified_df[cols]

    return modified_df

class OORSharePointProcessor:
    """Class to process OOR files from SharePoint"""

    def __init__(self):
        self.logger, self.log_file = setup_logging()
        self.logger.info("Initializing OOR SharePoint processor")
        
        # Initialize Azure Operations to get access token
        self.azure_ops = AzureOperations()
        access_token = self.azure_ops.get_access_token()
        
        if not access_token:
            self.logger.error("Failed to obtain access token. Exiting.")
            sys.exit(1)
            
        # Initialize SharePoint Operations with the access token
        self.sharepoint_ops = SharePointOperations(access_token=access_token)
        
        # Get required environment variables
        self.oor_input_prefix = os.environ.get('OOR_INPUT_PREFIX', 'OOR')
        self.oor_input_path = os.environ.get('OOR_INPUT_PATH', '/Inputs')
        self.oor_output_path = os.environ.get('OOR_OUTPUT_PATH', '/Processed')

    def get_latest_oor_file(self):
        """Get the latest OOR file from SharePoint"""
        self.logger.info("Getting the latest OOR file from SharePoint")
        
        try:
            # Get site and drive IDs
            site_id = self.sharepoint_ops.get_site_id()
            if not site_id:
                self.logger.error("Failed to get SharePoint site ID")
                return None
                
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            if not drive_id:
                self.logger.error("Failed to get SharePoint drive ID")
                return None
            
            # Set input path in the SharePoint operations object
            self.sharepoint_ops.ssr_input_filepath = self.oor_input_path
            
            # Get the latest file with OOR prefix
            file_bytes = self.sharepoint_ops.get_bytes_for_latest_file_with_prefix(
                prefix=self.oor_input_prefix, 
                drive_id=drive_id
            )
            
            if not file_bytes:
                self.logger.error(f"No files found with prefix {self.oor_input_prefix}")
                return None
                
            return file_bytes
            
        except Exception as e:
            self.logger.error(f"Error getting latest OOR file: {str(e)}")
            return None

    def process_oor_file(self, file_bytes):
        """Process OOR Excel file from bytes"""
        self.logger.info("Processing OOR file")
        
        # Track processing statistics
        stats = {
            'total_rows': 0,
            'generic_rows': 0,
            'calvary_rows': 0,
            'filtered_brand_rows': 0,
            'remaining_rows': 0,
            'brands_found': {},
            'output_files': {},
            'start_time': datetime.now(),
            'taskqueue_matches': {},
            'date_checks': {'recent': 0}
        }
        
        try:
            # Convert bytes to Excel dataframe
            excel_file = io.BytesIO(file_bytes)
            df = pd.read_excel(excel_file)
            stats['total_rows'] = len(df)
            self.logger.info(f"Read {stats['total_rows']} rows from input file")
            
            # Create separate dataframes for different outputs
            generic_df = pd.DataFrame()
            calvary_df = pd.DataFrame()
            main_df = df.copy()
            
            # Define all excluded brands (for main sheet)
            official_brands = [
                'COA', 'BUP', 'CSR', 'CNR', 'BUS', 'CAL', 'IMB', 'JET', 
                'JETSTAR', 'JS', 'NRMA', 'MTS', 'SCENTRE', 'SYD', 'RTDS', 'RFL'
            ]
            
            # 1. Extract GENERIC orders
            product_num_column = 'ProductNum'
            if product_num_column in df.columns:
                # Find rows where ProductNum is "GENERIC" or contains "GENERIC-SAMPLE"
                generic_exact_mask = df[product_num_column] == "GENERIC"
                generic_sample_mask = df[product_num_column].astype(str).str.contains("GENERIC-SAMPLE", case=False, na=False)
                generic_mask = generic_exact_mask | generic_sample_mask

                if generic_mask.any():
                    generic_df = df[generic_mask].copy()
                    stats['generic_rows'] = len(generic_df)
                    # Remove these rows from main dataframe
                    main_df = main_df[~generic_mask].copy()
                    self.logger.info(f"Extracted {stats['generic_rows']} GENERIC orders")
            else:
                self.logger.warning(f"Column '{product_num_column}' not found, skipping GENERIC extraction")
                
            # 2. Extract CAL orders (for CALVARY file)
            if product_num_column in main_df.columns:
                cal_mask = main_df[product_num_column].astype(str).str.startswith('CAL-', na=False)

                if cal_mask.any():
                    calvary_df = main_df[cal_mask].copy()
                    stats['calvary_rows'] = len(calvary_df)
                    # Remove these rows from main dataframe
                    main_df = main_df[~cal_mask].copy()
                    self.logger.info(f"Extracted {stats['calvary_rows']} CAL orders")
                    
            # 3. Filter out the rest of the specified brands from main sheet
            if product_num_column in main_df.columns:
                original_count = len(main_df)

                # Check each brand prefix and filter out
                keep_mask = pd.Series(True, index=main_df.index)

                for brand in official_brands:
                    brand_prefix = f"{brand}-"
                    prefix_mask = main_df[product_num_column].astype(str).str.startswith(brand_prefix, na=False)

                    brand_count = prefix_mask.sum()
                    if brand_count > 0:
                        stats['brands_found'][brand] = brand_count
                        keep_mask = keep_mask & (~prefix_mask)

                # Apply the filter
                main_df = main_df[keep_mask]
                filtered_count = original_count - len(main_df)
                stats['filtered_brand_rows'] = filtered_count
                self.logger.info(f"Filtered out {filtered_count} rows with official brand prefixes")

            stats['remaining_rows'] = len(main_df)
            
            # 4. Add 'CHECKING NOTE' and 'CUSTOMER' columns to all dataframes
            main_df = add_checking_customer_columns(main_df)
            generic_df = add_checking_customer_columns(generic_df)
            calvary_df = add_checking_customer_columns(calvary_df)
            
            # Define product mapping for customer names
            product_num_mapping = {
                'SAK': 'SHARKS AT KARELLA',
                'BW': 'Busways',
                'CS': 'Coal Services',
                'CLY': 'Calvary',
                'CAL': 'CALVARY',
                'IMB': 'IMB',
                'DC': 'Dolphins',
                'SG': 'ST George',
                'CCC': 'CCC',
                'DNA': 'DNATA',
                'DOLP': 'DOLPHINS',
                'END': 'ESHS',
                'GCL': 'GROWTH CIVIL LANDSCAPES',
                'GYM': 'GYMEA TRADES',
                'RHH': 'REDHILL',
                'RPA': 'REGAL REXNORR',
                'SEL': 'SEASONS LIVING',
                'STAR': 'STAR AVIATION',
                'YAE': 'YOUNG ACADEMICS',
                'ZAM': 'ZAMBARERO',
                'STG': 'DRAGONS',
                'KGT': 'KNIGHTS',
                'SEL-SEASON': 'SEASON LIVING',
                'SGL': 'ST GEORGE LEAGUES',
                'RRA': 'REGAL REXNORD',
                'CRAIG SMITH': 'CRAIG SMITH',
                'TRADES GOLF CLUB': 'TRADES GOLF CLUB',
                'MYTILENIAN': 'HOUSE'
            }
            
            # Define TaskQueue value mappings for CHECKING NOTE
            taskqueue_mapping = {
                'Data Entry CHK': 'DATA ENTRY CHECK',
                'CS HOLDING ORDERS': 'CS HOLD Q!',
                'CAL ROLLOUT DATES': 'CALL ROLLOUT DATE',
                'CAL DISPATCH BY LOCATION': 'CAL DISPATCH BY LOCATION Q',
                'CANCEL ORDERS 2B DEL': 'CANCEL Q'
            }
            
            # 5. Process CALVARY dataframe - special case
            if not calvary_df.empty:
                # Set CUSTOMER to CALVARY for all rows
                calvary_df['CUSTOMER'] = 'CALVARY'

                # Process TaskQueue values
                taskqueue_column = 'TaskQueue'
                if taskqueue_column in calvary_df.columns:
                    for task_value, note_value in taskqueue_mapping.items():
                        mask = calvary_df[taskqueue_column] == task_value
                        if mask.any():
                            calvary_df.loc[mask, 'CHECKING NOTE'] = note_value
                            stats['taskqueue_matches'][task_value] = mask.sum()

                # Process DateIssued values
                date_issued_column = 'DateIssued'
                if date_issued_column in calvary_df.columns:
                    today = datetime.now().date()

                    # Convert DateIssued to datetime if needed
                    if not pd.api.types.is_datetime64_dtype(calvary_df[date_issued_column]):
                        try:
                            calvary_df[date_issued_column] = pd.to_datetime(calvary_df[date_issued_column])
                        except Exception as e:
                            self.logger.warning(f"Could not convert DateIssued to datetime: {str(e)}")

                    try:
                        # Extract date part
                        calvary_df['DateIssuedDate'] = calvary_df[date_issued_column].dt.date

                        # Find rows with recent dates (< 5 days old)
                        recent_date_mask = calvary_df['DateIssuedDate'] > (today - timedelta(days=5))

                        if recent_date_mask.any():
                            recent_count = recent_date_mask.sum()
                            calvary_df.loc[recent_date_mask, 'CHECKING NOTE'] = '< 5 DAYS OLD'
                            stats['date_checks']['recent'] += recent_count

                        # Drop temporary column
                        calvary_df = calvary_df.drop('DateIssuedDate', axis=1)
                    except Exception as e:
                        self.logger.warning(f"Error processing dates: {str(e)}")
            
            # 6. Process main and GENERIC dataframes for customer mapping and task queue notes
            for df_name, df_to_process in [("main", main_df), ("GENERIC", generic_df)]:
                if not df_to_process.empty:
                    # Fill CUSTOMER column based on ProductNum
                    if product_num_column in df_to_process.columns:
                        updated_rows = 0

                        # Process each row
                        for index, row in df_to_process.iterrows():
                            product_num = str(row[product_num_column])

                            # Check for exact match
                            if product_num in product_num_mapping:
                                df_to_process.at[index, 'CUSTOMER'] = product_num_mapping[product_num]
                                updated_rows += 1
                                continue

                            # Check for prefix match
                            matched = False
                            for prefix, value in product_num_mapping.items():
                                if product_num.startswith(prefix + '-') or product_num.startswith(prefix):
                                    df_to_process.at[index, 'CUSTOMER'] = value
                                    matched = True
                                    updated_rows += 1
                                    break

                            # Special case for SAK
                            if not matched and product_num.startswith('SAK-'):
                                df_to_process.at[index, 'CUSTOMER'] = 'SHARKS AT KARELLA'
                                updated_rows += 1

                        self.logger.info(f"Updated {updated_rows} CUSTOMER values in {df_name} dataframe")

                    # Set CHECKING NOTE based on TaskQueue
                    taskqueue_column = 'TaskQueue'
                    if taskqueue_column in df_to_process.columns:
                        for task_value, note_value in taskqueue_mapping.items():
                            mask = df_to_process[taskqueue_column] == task_value
                            if mask.any():
                                df_to_process.loc[mask, 'CHECKING NOTE'] = note_value
                                if task_value in stats['taskqueue_matches']:
                                    stats['taskqueue_matches'][task_value] += mask.sum()
                                else:
                                    stats['taskqueue_matches'][task_value] = mask.sum()

                    # Set CHECKING NOTE based on DateIssued
                    date_issued_column = 'DateIssued'
                    if date_issued_column in df_to_process.columns:
                        today = datetime.now().date()

                        # Convert DateIssued to datetime if needed
                        if not pd.api.types.is_datetime64_dtype(df_to_process[date_issued_column]):
                            try:
                                df_to_process[date_issued_column] = pd.to_datetime(df_to_process[date_issued_column])
                            except Exception as e:
                                self.logger.warning(f"Could not convert DateIssued to datetime: {str(e)}")

                        try:
                            # Extract date part
                            df_to_process['DateIssuedDate'] = df_to_process[date_issued_column].dt.date

                            # Find rows with recent dates (< 5 days old)
                            recent_date_mask = df_to_process['DateIssuedDate'] > (today - timedelta(days=5))

                            if recent_date_mask.any():
                                recent_count = recent_date_mask.sum()
                                df_to_process.loc[recent_date_mask, 'CHECKING NOTE'] = '< 5 DAYS OLD'
                                stats['date_checks']['recent'] += recent_count

                            # Drop temporary column
                            df_to_process = df_to_process.drop('DateIssuedDate', axis=1)
                        except Exception as e:
                            self.logger.warning(f"Error processing dates: {str(e)}")

                    # Update the dataframe reference
                    if df_name == "main":
                        main_df = df_to_process
                    else:
                        generic_df = df_to_process
            
            # Prepare for uploading to SharePoint
            today = datetime.now().strftime("%Y%m%d")
            
            # Create a SharePoint site ID and drive ID for upload
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            
            # Set output path in SharePoint operations
            self.sharepoint_ops.ssr_output_filepath = self.oor_output_path
            
            # 7. Upload each file to SharePoint
            if not main_df.empty:
                others_filename = f"OTHERS OOR {today}.csv"
                # Create a BytesIO object for the CSV data
                others_buffer = io.BytesIO()
                # Write CSV to the buffer
                main_df.to_csv(others_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
                # Reset buffer position to the beginning
                others_buffer.seek(0)
                # Upload file to SharePoint
                self.sharepoint_ops.upload_excel_file(
                    drive_id=drive_id, 
                    excel_filename=others_filename, 
                    file_bytes=others_buffer.getvalue()
                )
                stats['output_files']['others'] = others_filename
                self.logger.info(f"Uploaded {len(main_df)} rows to: {others_filename}")
                
            if not generic_df.empty:
                generic_filename = f"GENERIC SAMPLES {today}.csv"
                # Create a BytesIO object for the CSV data
                generic_buffer = io.BytesIO()
                # Write CSV to the buffer
                generic_df.to_csv(generic_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
                # Reset buffer position to the beginning
                generic_buffer.seek(0)
                # Upload file to SharePoint
                self.sharepoint_ops.upload_excel_file(
                    drive_id=drive_id, 
                    excel_filename=generic_filename, 
                    file_bytes=generic_buffer.getvalue()
                )
                stats['output_files']['generic'] = generic_filename
                self.logger.info(f"Uploaded {len(generic_df)} rows to: {generic_filename}")
                
            if not calvary_df.empty:
                calvary_filename = f"CALVARY {today}.csv"
                # Create a BytesIO object for the CSV data
                calvary_buffer = io.BytesIO()
                # Write CSV to the buffer
                calvary_df.to_csv(calvary_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)
                # Reset buffer position to the beginning
                calvary_buffer.seek(0)
                # Upload file to SharePoint
                self.sharepoint_ops.upload_excel_file(
                    drive_id=drive_id, 
                    excel_filename=calvary_filename, 
                    file_bytes=calvary_buffer.getvalue()
                )
                stats['output_files']['calvary'] = calvary_filename
                self.logger.info(f"Uploaded {len(calvary_df)} rows to: {calvary_filename}")
                
            # 8. Create and upload summary report
            summary_filename = f"OOR_SUMMARY_{today}.txt"
            summary_content = self.create_summary_report(stats)
            
            # Create a BytesIO object for the summary content
            summary_buffer = io.BytesIO(summary_content.encode('utf-8'))
            
            # Upload summary report to SharePoint
            self.sharepoint_ops.upload_excel_file(
                drive_id=drive_id, 
                excel_filename=summary_filename, 
                file_bytes=summary_buffer.getvalue()
            )
            stats['output_files']['summary'] = summary_filename
            
            # Update success status and duration
            stats['success'] = True
            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            self.logger.info(f"Processing completed in {stats['duration']:.2f} seconds")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error processing OOR file: {str(e)}")
            return None

    def create_summary_report(self, stats):
        """Create a summary report for the processing job"""
        summary = ""
        summary += "=" * 70 + "\n"
        summary += "OOR PROCESSING SUMMARY REPORT\n"
        summary += "=" * 70 + "\n\n"

        summary += f"Processed on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        if 'duration' in stats:
            summary += f"Processing time: {stats.get('duration', 0):.2f} seconds\n\n"

        # Row counts
        summary += "-" * 70 + "\n"
        summary += "ROW COUNTS\n"
        summary += "-" * 70 + "\n"
        summary += f"Total rows in input file: {stats.get('total_rows', 0)}\n"
        summary += f"Rows extracted to GENERIC SAMPLES: {stats.get('generic_rows', 0)}\n"
        summary += f"Rows extracted to CALVARY: {stats.get('calvary_rows', 0)}\n"
        summary += f"Rows filtered out (official brands): {stats.get('filtered_brand_rows', 0)}\n"
        summary += f"Remaining rows in OTHERS OOR: {stats.get('remaining_rows', 0)}\n\n"

        # TaskQueue matches
        if stats.get('taskqueue_matches'):
            summary += "-" * 70 + "\n"
            summary += "TASKQUEUE MATCHES\n"
            summary += "-" * 70 + "\n"
            for task, count in sorted(stats.get('taskqueue_matches', {}).items(), key=lambda x: x[1], reverse=True):
                summary += f"{task}: {count} rows\n"
            summary += "\n"

        # Date checks
        if stats.get('date_checks', {}).get('recent', 0) > 0:
            summary += "-" * 70 + "\n"
            summary += "DATE CHECKS\n"
            summary += "-" * 70 + "\n"
            summary += f"Rows with recent dates (< 5 days old): {stats.get('date_checks', {}).get('recent', 0)}\n\n"

        # Output files
        summary += "-" * 70 + "\n"
        summary += "OUTPUT FILES\n"
        summary += "-" * 70 + "\n"

        for key, path in stats.get('output_files', {}).items():
            if key != 'summary':
                summary += f"{key.upper()}: {path}\n"

        summary += "\n" + "=" * 70 + "\n"
        if stats.get('success', False):
            summary += "✓ Processing completed successfully\n"
        else:
            summary += f"⚠ Processing failed: {stats.get('error', 'Unknown error')}\n"
        summary += "=" * 70 + "\n"
        
        return summary
                  
# def main():
#     """Main function to run the OOR processing from SharePoint"""
#     processor = OORSharePointProcessor()
    
#     # Get the latest OOR file from SharePoint
#     file_bytes = processor.get_latest_oor_file()
    
#     if file_bytes:
#         # Process the OOR file
#         stats = processor.process_oor_file(file_bytes)
        
#         if stats and stats.get('success', False):
#             processor.logger.info("OOR processing completed successfully")
#             processor.logger.info("=" * 70)
#             processor.logger.info("PROCESSING COMPLETE")
#             processor.logger.info("=" * 70)
            
#             processor.logger.info("Output files:")
#             for key, path in stats['output_files'].items():
#                 if key != 'summary':
#                     processor.logger.info(f"  - {key.upper()}: {path}")
            
#             processor.logger.info(f"Summary report: {stats['output_files'].get('summary')}")
#             processor.logger.info(f"Log file: {processor.log_file}")
#         else:
#             processor.logger.error("OOR processing failed")
#     else:
#         processor.logger.error("No OOR file found to process")

# if __name__ == "__main__":
#     main()