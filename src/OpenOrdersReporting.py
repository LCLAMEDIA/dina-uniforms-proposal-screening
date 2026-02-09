import pandas as pd
import os
import io
import logging
from datetime import datetime, timedelta
from dateutil.parser import parse
import csv
from typing import Dict, List, Tuple, Any, Optional, Callable
import re # Added for regex operations
import difflib
import pytz
from concurrent.futures import ThreadPoolExecutor
from xlsxwriter.utility import xl_col_to_name

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

        inventory_loaded = self.config_reader.load_stock_inventory_file()
        logging.info(f"[OpenOrdersReporting] Inventory loaded: {inventory_loaded}")

        # Get configuration values
        self.customer_mapping_dict = self.config_reader.get_customer_mapping_dict()
        self.customer_mapping_fields = self.config_reader.get_customer_mapping_fields()

        # Get SOH robot mapping
        self.robot_soh_lookup = self.config_reader.get_robot_soh_lookup()

        # Load Australian time
        self.australia_now = datetime.now(pytz.timezone('Australia/Sydney'))

        # Configure folder paths based on environment variables
        self.oor_input_prefix = os.environ.get('OOR_INPUT_PREFIX', 'OOR')
        self.oor_input_path = os.environ.get('OOR_INPUT_PATH', '/KNOWLEDGE BASE/AUTOMATIONS/OPEN ORDER REPORTING (OOR)/Upload')
        self.oor_output_path = os.environ.get('OOR_OUTPUT_PATH', '/KNOWLEDGE BASE/AUTOMATIONS/OPEN ORDER REPORTING (OOR)/Processed')

        # Define new columns
        self.new_columns = ["ACTIONED", "CHECKING NOTES", "CUSTOMER", "ROBOT SOH"]

        # Set color mapping
        self.color_mapping = {
            "PO OVERDUE": "#fae2d5",
            "PO OK": "#ffffcc",
            "DECO OK": "#ccccff",
            "DECO OVERDUE": "#9999ff",
            "SHOULD SHIP THIS WEEK": "#d9f2d0",
            "PO RECEIVED PLEASE SHIP": "#92d050",
            "DIRECT PO RECEIVED PLEASE SHIP": "#ffc000"
        }

        # Log all configuration values in one place
        logging.info("[OpenOrdersReporting] Loaded configuration values:")
        logging.info(f"[OpenOrdersReporting] - Official brands: {self.customer_mapping_dict}")
        logging.info(f"[OpenOrdersReporting] - Product number mappings: {self.customer_mapping_fields}")
        logging.info(f"[OpenOrdersReporting] - Input path: {self.oor_input_path}")
        logging.info(f"[OpenOrdersReporting] - Output path: {self.oor_output_path}")

    def _load_prerequisites(self):
        # Load dynamic configuration

        with ThreadPoolExecutor() as executor:
            configuration_result = executor.submit(self.config_reader.load_configuration)
            stock_inventory_result = executor.submit(self.config_reader.load_stock_inventory_file)

            config_loaded = configuration_result.result()

            logging.info(f"[OpenOrdersReporting] Configuration loaded: {config_loaded}")

            inventory_loaded = stock_inventory_result.result()

            logging.info(f"[OpenOrdersReporting] Inventory loaded: {inventory_loaded}")


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
            pd.read_excel(excel_file)
        except Exception as e:
            return False, f"File could not be read as an Excel file: {str(e)}"
        
        return True, ''


    def process_excel_file(self, excel_file_bytes: bytes, filename: Optional[str] = None) -> Dict[str, Any]:
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
            'output_files': {},
            'start_time': self.australia_now,
        }
        
        # Validate the file first
        is_valid, validation_message = self.validate_oor_file(excel_file_bytes, filename)
        if not is_valid:
            logging.warning(f"[OpenOrdersReporting] File validation failed: {validation_message}")
            stats['success'] = False
            stats['error_message'] = validation_message
            stats['error_type'] = 'validation_error'
            stats['end_time'] = datetime.now(pytz.timezone('Australia/Sydney'))
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
              
            total_rows = int(main_df.shape[0])

            # Remove returns
            main_df = self._remove_returns(main_df=main_df)

            # Remove generic non PNW vendors
            main_df = self._remove_generic_non_pnw(main_df=main_df)

            removed_records = total_rows - int(main_df.shape[0])

            # Insert new columns
            main_df = self._insert_new_columns(main_df=main_df)

            # Start automation
            main_df = self.automate_oor_labels(main_df=main_df)

            # Convert Final OOR file to excel and set row colors
            oor_bytes = self.convert2xlsx_set_color(main_df=main_df)

            remaining_rows = int(main_df.shape[0])

            # Get rows without customer label
            rows_wo_labels = int(main_df[main_df["CUSTOMER"] == ""].shape[0])

            # Get rows without customer label
            rows_wo_checking_notes = int(main_df[main_df["CHECKING NOTES"] == ""].shape[0])

            # Build file name
            aust_time_now_str = self.australia_now.strftime("%Y%m%d_%H%M%S")
            oor_processed_filename = f"PROCESSED_OOR_{aust_time_now_str}_row_{remaining_rows}.xlsx"

            # Upload to SharePoint
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id) 
            self.sharepoint_ops.upload_file_to_path(
                drive_id=drive_id,
                file_path=self.oor_output_path,
                file_name=oor_processed_filename,
                file_bytes=oor_bytes,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            stats['output_file'] = oor_processed_filename
            
            # Finalize stats
            stats['success'] = True
            stats['end_time'] = datetime.now(pytz.timezone('Australia/Sydney'))
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            
            stats['remaining_rows'] = remaining_rows
            stats['removed_records'] = removed_records
            stats['rows_wo_labels'] = rows_wo_labels
            stats['rows_wo_checking_notes'] = rows_wo_checking_notes

            logging.info(f"[OpenOrdersReporting] Processing completed in {stats['duration']:.2f} seconds. Stats: {stats}")
            
            return stats
            
        except pd.errors.EmptyDataError as e:
            logging.error(f"[OpenOrdersReporting] Empty data error: {str(e)}", exc_info=True)
            stats['success'] = False
            stats['error_message'] = "The Excel file contains no data or only header information."
            stats['error_type'] = 'empty_data_error'
            stats['end_time'] = datetime.now(pytz.timezone('Australia/Sydney'))
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            return stats
            
        except pd.errors.ParserError as e:
            logging.error(f"[OpenOrdersReporting] Excel parser error: {str(e)}", exc_info=True)
            stats['success'] = False
            stats['error_message'] = "Unable to parse the Excel file. The file may be corrupted or in an unsupported format."
            stats['error_type'] = 'parser_error'
            stats['end_time'] = datetime.now(pytz.timezone('Australia/Sydney'))
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
            return stats
            
        except Exception as e:
            logging.error(f"[OpenOrdersReporting] Error processing file: {str(e)}", exc_info=True)
            stats['success'] = False
            stats['error_message'] = str(e)
            stats['error_type'] = 'processing_error'
            stats['end_time'] = datetime.now(pytz.timezone('Australia/Sydney'))
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

   
    def _remove_returns(self, main_df: pd.DataFrame) -> pd.DataFrame:
        logging.info("[OpenOrdersReporting] Removing returns from OOR")

        # Ensure QtyOrdered is integer
        main_df["QtyOrdered"] = main_df["QtyOrdered"].astype(int)
        
        # Keep only rows where QtyOrdered >= 0
        main_df = main_df[main_df["QtyOrdered"] >= 0]
        
        return main_df.reset_index(drop=True).copy(deep=True)


    def _remove_generic_non_pnw(self, main_df: pd.DataFrame) -> pd.DataFrame:
        logging.info("[OpenOrdersReporting] Removing generic Non PNW vendors")

        is_generic = main_df["ProductNum"].astype(str).str.startswith("GENERIC-SAMPLE")
        is_not_vendor_pnw = main_df["Vendors"].astype(str).str.strip() != "PNW"

        main_df = main_df.drop(
            main_df[
                is_generic
                & (is_not_vendor_pnw)
            ].index
        )

        return main_df.reset_index(drop=True).copy(deep=True)


    def _insert_new_columns(self, main_df: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"[OpenOrdersReporting] Inserting new columns {self.new_columns}")

        for idx, new_column in enumerate(self.new_columns):

            if new_column == "ROBOT SOH":
                col_idx = int(main_df.columns.get_loc("StockOnHand")) + 1

                main_df.insert(col_idx, "ROBOT SOH", [""] * len(main_df))
            else:
                main_df.insert(idx, new_column, "")

        return main_df.copy(deep=True)
    

    def _set_customer_label(self, idx, main_df: pd.DataFrame, row: pd.Series):
        logging.info(f"[OpenOrdersReporting] Setting customer label for row {idx}")

        for field in self.customer_mapping_fields:
                value: str = row[field]

                for customer_label, criteria in self.customer_mapping_dict.items():
                    patterns = criteria.get(field, []) or []

                    match = None

                    if field.strip() == "ProductNum":
                        match = next(filter(lambda pattern: str(value).upper().startswith(f"{str(pattern).upper()}-") , patterns), None)
                        
                        if match is None:
                            match = next(filter(lambda pattern: str(value).upper().startswith(f"{str(pattern).upper()} -") , patterns), None)

                    elif field.strip() == "OurRef":
                        match = next(filter(lambda pattern: f"{str(pattern).upper()}-" in str(value).upper(), patterns), None)

                        if match is None:
                            match = next(filter(lambda pattern: f"{str(pattern).upper()} -" in str(value).upper(), patterns), None)

                    else:
                        match = next(filter(lambda pattern: str(pattern).lower() in str(value).lower(), patterns), None)

                    if match is not None:
                        main_df.at[idx, "CUSTOMER"] = customer_label

                        logging.info(f"[OpenOrdersReporting] Row {row} populated with \"{customer_label}\" matched: {field} ")

                        break


    @staticmethod
    def add_business_days(start: datetime, n: int) -> datetime:
        """
        Add n business days including `start` itself.
        Example: n=1 always returns start (if it's a weekday).
        If start is weekend, jump to nearest weekday first.
        """
        current = start

        # If weekend, move to closest weekday (forward if n>0, backward if n<0)
        if current.weekday() >= 5:
            step = 1 if n > 0 else -1
            while current.weekday() >= 5:
                current += timedelta(days=step)

        # If n=1, return today (inclusive rule)
        if abs(n) == 1:
            return current

        step = 1 if n > 0 else -1
        days_counted = 1  # already counted today

        while days_counted < abs(n):
            current += timedelta(days=step)
            if current.weekday() < 5:
                days_counted += 1

        return current


    @staticmethod
    def check_business_days(
        date: datetime,
        n: int,
        comparison: str,
        today: Optional[datetime] = None
    ) -> bool:
        """
        Comparison operations for business-day checks:

        Keys:
        - "n_days_ago"        -> True if the date is exactly N business days ago (inclusive of today).
        - "n_days_after"      -> True if the date is exactly N business days after today (inclusive of today).
        - "within_n_days_ago" -> True if the date falls between N business days ago and today, inclusive.
        - "less_than"         -> True if the date is on or after the N-days-ago cutoff (i.e. within N days ago).
        - "greater_than"      -> True if the date is before the N-days-ago cutoff (i.e. more than N days ago).
        - "passed"            -> True if the date is strictly before today.
        - "today_or_future"   -> True if the date is today or later.
        """

        if today is None:
            today = datetime.now(pytz.timezone('Australia/Sydney'))

        ago_date = OpenOrdersReporting.add_business_days(today, -n)
        after_date = OpenOrdersReporting.add_business_days(today, n)

        ops: dict[str, Callable[[datetime], bool]] = {
            "n_days_ago": lambda d: d.date() == ago_date.date(),
            "n_days_after": lambda d: d.date() == after_date.date(),
            "within_n_days_ago": lambda d: ago_date.date() <= d.date() <= today.date(),
            "less_than": lambda d: d.date() >= ago_date.date(),
            "greater_than": lambda d: d.date() < ago_date.date(),
            "passed": lambda d: d.date() < today.date(),
            "today_or_future": lambda d: d.date() >= today.date(),
        }

        if comparison not in ops:
            raise ValueError(f"Unsupported comparison: {comparison}")

        return ops[comparison](date)


    @staticmethod
    def extract_date(text: str) -> datetime | None:
        """
        Extract a date from messy strings (Australian format: D/M/YY).
        Accepts separators: ., -, / and even double dots.
        """
        # Find groups like 5.9.25, 6/8/25, 8-8-25, 6..8.25
        match = re.search(r'(\d{1,2})[.\-/]+(\d{1,2})[.\-/]+(\d{2,4})', text)
        if not match:
            return None  # no valid date
        
        day, month, year = match.groups()

        # Fix 2-digit year → assume 2000s
        if len(year) == 2:
            year = "20" + year

        try:
            # Parse with Australian format (day first)
            logging.info(f"[OpenOrdersReporting] Parsing extracted date: {day}/{month}/{year}")
            return datetime.strptime(f"{day}/{month}/{year}", "%d/%m/%Y")
        except Exception as e:
            logging.error(f"[OpenOrdersReporting] Failure to parse date: {day}/{month}/{year}. Error: {e}")
            return None
    
    def _populate_checking_notes(self, idx, main_df: pd.DataFrame, task_queue, qid, parsed_date_issued, parsed_qid_date, parsed_our_ref_date, our_ref_string):
        logging.info(f"[OpenOrdersReporting] Populating checking notes to row {idx}")

        customer = main_df.at[idx, "CUSTOMER"]

        label = ""

        if parsed_date_issued and self.check_business_days(date=parsed_date_issued, n=3, comparison="less_than", today=self.australia_now):
            label = "< 3 DAYS"

        if not pd.isna(task_queue) and str("62: CANCEL").lower() in str(task_queue).lower():
            label = "CANCEL Q"

        if not pd.isna(task_queue) and str("Data Entry CHK").lower() in str(task_queue).lower():
            label = "Data Entry CHK Q"

        if not pd.isna(task_queue) and str("24: CS Customer Service-Stock Issue").lower() in str(task_queue).lower():
            label = "CS STOCK ISSUE Q"

        if not pd.isna(task_queue) and str("3: CSMG").lower() in str(task_queue).lower():
            label = "CSMG Q"
        
        if not pd.isna(task_queue) and str("501: CS HOLD").lower() in str(task_queue).lower():
            label = "CS HOLD Q"

        if not pd.isna(qid) and str(int(qid)).strip() == "3" and parsed_our_ref_date:

            if self.check_business_days(date=parsed_our_ref_date, n=0, comparison="passed", today=self.australia_now):
                label = "PO OVERDUE"

            if self.check_business_days(date=parsed_our_ref_date, n=0, comparison="today_or_future", today=self.australia_now):
                label = "PO OK"

        if not pd.isna(qid) and str(int(qid)).strip() in ["4", "5"] and parsed_qid_date:

            if self.check_business_days(date=parsed_qid_date, n=3, comparison="less_than", today=self.australia_now):
                label = "SHOULD SHIP THIS WEEK"

            if self.check_business_days(date=parsed_qid_date, n=3, comparison="greater_than", today=self.australia_now):
                label = "PO RECEIVED PLEASE SHIP"

                if our_ref_string and "direct" in our_ref_string:
                    label = "DIRECT PO RECEIVED PLEASE SHIP"
                    
        if not pd.isna(qid) and str(int(qid)).strip() == "31" and parsed_qid_date:

            if self.check_business_days(date=parsed_qid_date, n=12, comparison="less_than", today=self.australia_now):
                label = "DECO OK"

            if self.check_business_days(date=parsed_qid_date, n=11, comparison="greater_than", today=self.australia_now):
                label = "DECO OVERDUE"

        if not pd.isna(qid) and str(int(qid)).strip() == "32" and parsed_qid_date:

            if self.check_business_days(date=parsed_qid_date, n=7, comparison="less_than", today=self.australia_now):
                label = "DECO OK"

            if self.check_business_days(date=parsed_qid_date, n=6, comparison="greater_than", today=self.australia_now):
                label = "DECO OVERDUE"

        if not pd.isna(customer) and "expire" in str(customer).lower():
            label = "EXPIRED"
            
        main_df.at[idx, "CHECKING NOTES"] = label
        
        logging.info(f"[OpenOrdersReporting] Row {idx} populated with {label} in checking notes")


    def _populate_robot_soh(self, idx, main_df: pd.DataFrame, row: pd.Series):
        logging.info(f"[OpenOrdersReporting] Populating robot SOH to row {idx}")

        if pd.isna(row["barcodeupc"]):
            return
        
        barcode = str(row["barcodeupc"])

        if isinstance(row["barcodeupc"], float):
            barcode = str(int(row["barcodeupc"]))

        robot_soh_value = self.robot_soh_lookup.get(barcode, "") or ""

        main_df.at[idx, "ROBOT SOH"] = str(robot_soh_value)

        logging.info(f"[OpenOrdersReporting] Row {idx} populated with robot SOH: {robot_soh_value}")


    def automate_oor_labels(self, main_df: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"[OpenOrdersReporting] Initializing OOR labeling automation")

        for idx, row in main_df.iterrows():
            date_issued = row["DateIssued"]
            task_queue = row["TaskQueue"]
            qid = row["QID"]
            qid_date = row["QIDDate"]
            our_ref = row["OurRef"]

            parsed_date_issued = None
            if not pd.isna(date_issued):
                parsed_date_issued = parse(str(date_issued), fuzzy=True)

            parsed_qid_date = None
            if not pd.isna(qid_date):
                parsed_qid_date = parse(str(qid_date), fuzzy=True)

            parsed_our_ref_date = None
            our_ref_string = None
            if not pd.isna(our_ref):
                parsed_our_ref_date = self.extract_date(our_ref)
                our_ref_string = str(our_ref).lower().strip()
                
            self._set_customer_label(idx=idx, main_df=main_df, row=row)

            self._populate_checking_notes(idx=idx, main_df=main_df, task_queue=task_queue, qid=qid, parsed_date_issued=parsed_date_issued, parsed_qid_date=parsed_qid_date, parsed_our_ref_date=parsed_our_ref_date, our_ref_string=our_ref_string)

            self._populate_robot_soh(idx=idx, main_df=main_df, row=row)

        logging.info(f"[OpenOrdersReporting] Finished OOR labeling automation")

        return main_df.reset_index(drop=True).copy(deep=True)
    

    def convert2xlsx_set_color(self, main_df: pd.DataFrame) -> bytes:
        logging.info(f"[OpenOrdersReporting] Converting final OOR file to excel and setting row colors")

        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            main_df.to_excel(writer, sheet_name="Sheet1", index=False)

            workbook  = writer.book
            worksheet = writer.sheets["Sheet1"]

            nrows, ncols = main_df.shape
            last_col = xl_col_to_name(ncols - 1)   # e.g., "B", "AA", etc.
            data_range = f"A2:{last_col}{nrows+1}"

            for label, color in self.color_mapping.items():
                fmt = workbook.add_format({"bg_color": color})
                worksheet.conditional_format(
                    data_range,
                    {
                        "type": "formula",
                        "criteria": f'=$B2="{label}"',   # column B is status
                        "format": fmt,
                    },
                )

        # Get the Excel file in memory
        return output.getvalue()
