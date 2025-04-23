import pandas as pd
import numpy as np
import io
import logging
from typing import List, Dict, Tuple
import os
import pytz
from openpyxl import load_workbook
from datetime import datetime
from dateutil.parser import parse

from AzureOperations import AzureOperations
from SharePointOperations import SharePointOperations

logging.basicConfig(level=logging.INFO)

class StockStatusReportOps:
    
    def __init__(self, exported_file_name: str, exported_file_bytes: bytes):
        self.azure_ops = AzureOperations()
        access_token = self.azure_ops.get_access_token()

        self.sharepoint_ops = SharePointOperations(access_token=access_token)

        self.product_list_prefix = os.environ.get('PRODUCT_LIST_PREFIX')
        self.ssr_summary_prefix = os.environ.get('SSR_SUMMARY_PREFIX')

        self.exported_file_name = exported_file_name
        self.exported_file_bytes = exported_file_bytes
        self.stock_status_report_sheet = None
        self.client_category = {
                                "BUS": {"BUSWAYS$"},
                                "COAL": {
                                    "COAL",
                                    "COAL$"
                                },
                                "IMB": {
                                    "IMB",
                                    "IMB$"
                                },
                                "MTS": {
                                    "MTS",
                                    "MTS$"
                                },
                                "NRMA": {
                                    "NRMA",
                                    "NRMA$"
                                },
                                "NRMA PARKS ": {
                                    "NRMAP",
                                    "NRMAP$"
                                },
                                "RFDS": {
                                    "RFDS",
                                    "RFDS$"
                                },
                                "STAR": {
                                    "STAR",
                                    "STAR$"
                                },
                                "WES": {
                                    "west",
                                    "WESTFLD",
                                },
                                "YOUNG": {"YOUNG$"},
                                "ZAM": {
                                    "ZAM",
                                    "ZAM$"
                                }
                            }
        self.columns_to_hide = [
                                "E",
                                "F",
                                "I",
                                "J",
                                "L",
                                "M",
                                "N",
                                "P",
                                "R",
                                "S",
                                "U",
                                "V",
                                "W",
                                "X",
                                "Y",
                                "Z",
                                "AA",
                                "AB",
                                "AD",
                                "AE",
                                "AF",
                                "AG",
                                "AH",
                                "AI",
                                "AJ",
                                "AK",
                                "AL",
                                "AM",
                                "AN",
                                "AO",
                                "AP",
                                "AQ",
                                "AR",
                                "AS",
                                "AT",
                                "AU",
                                "AV",
                                "AW",
                            ]
        self.new_columns = [
                                "UNIT PRICE",
                                "SOH Value xgst",
                                "PO Cost xgst",
                                "On Order Cost xgst"
                            ]
        self.columns_to_clip = ['qty_onhand', 'qty_SO', 'qty_PO']
        self.australia_now = datetime.now(pytz.timezone('Australia/Sydney'))


    def start_automate(self) -> Tuple[bytes, str, str, str]:
        logging.info("[StockStatusReportOps] Automating SSR")
        notification_message = ''
        try:
            exported_file_df = self.excel_bytes_as_df()

            # Filter rows to select data from clients only
            client_filter = {j for i in self.client_category.values() for j in i}
            client_rows_df = exported_file_df[exported_file_df['item_cat1'].astype(str).str.strip().astype(str).str.lower().isin({s.lower() for s in client_filter})]

            # Remove rows that are all samples
            client_rows_wo_samples_df = client_rows_df[~client_rows_df.apply(lambda row: row.astype(str).str.contains('sample', case=False, na=False)).any(axis=1)]

            # Add new columns
            client_rows_wo_samples_df.loc[:, self.new_columns] = np.nan
            client_rows_wo_samples_df.loc[:, ["active in web"]] = np.nan.__str__()
            client_rows_wo_samples_df.loc[:, ["CHECK FOR DUPLICATES"]] = np.nan

            # Set zeroes to negative values
            client_rows_wo_samples_df.loc[:, self.columns_to_clip] = client_rows_wo_samples_df.loc[:, self.columns_to_clip].clip(lower=0)

            # Check if any barcodes are missing
            empty_barcodes_df = client_rows_wo_samples_df[client_rows_wo_samples_df['barcode'].isnull()]
            if not empty_barcodes_df.empty:
                logging.info("[StockStatusReportOps] Records with no barcodes found! Building notification message")

                id_list = empty_barcodes_df['ID'].tolist()
                name_list = empty_barcodes_df['Name'].tolist()

                notification_message += "Missing Barcodes for following exported items: \n"

                for id_, name_ in zip(id_list, name_list):
                    notification_message += "\t - {id_}: {name_} \n".format(id_=id_, name_=name_)

                notification_message += "\n"
            
            # Get product list uploaded in sharepoint
            product_list = self.get_product_list()

            # Build lookup map
            unitPrice_mapping = product_list.groupby('barcode')['unitPrice'].first()
            activeInWeb_mapping = product_list.groupby('barcode')['ActiveInWeb'].first()

            # Lookup unit price
            client_rows_wo_samples_df.loc[:,'barcode'] = client_rows_wo_samples_df.loc[:,'barcode'].astype(str)
            client_rows_wo_samples_df.loc[:,'UNIT PRICE'] = client_rows_wo_samples_df.loc[:,'barcode'].astype(str).map(unitPrice_mapping).fillna(np.nan)

            # Lookup active in web
            client_rows_wo_samples_df.loc[:,'active in web'] = client_rows_wo_samples_df.loc[:,'active in web'].astype(str)
            client_rows_wo_samples_df.loc[:,'active in web'] = client_rows_wo_samples_df.loc[:,'barcode'].astype(str).map(activeInWeb_mapping).fillna('0')

            # check if no lookup value retrieved for rows with SOH, SO, and PO
            no_lookup_filtered_df = client_rows_wo_samples_df[
                (client_rows_wo_samples_df['qty_onhand'] > 0) &
                (client_rows_wo_samples_df['qty_SO'] > 0) &
                (client_rows_wo_samples_df['qty_PO'] > 0) &
                (client_rows_wo_samples_df['UNIT PRICE'] == np.nan)
            ]

            if not no_lookup_filtered_df.empty:
                logging.info("[StockStatusReportOps] Records with no lookup data where SOH, SO, and PO > 1 found! Building notification message")

                id_list = no_lookup_filtered_df['ID'].tolist()
                name_list = no_lookup_filtered_df['Name'].tolist()

                notification_message += "No Lookup Data for following export items where SOH, SO, and PO > 0: \n"

                for id_, name_ in zip(id_list, name_list):
                    notification_message += "\t - {id_}: {name_} \n".format(id_=id_, name_=name_)

                notification_message += "\n"

                # Skip rows without looked up price
                client_rows_wo_samples_df[:, :] = client_rows_wo_samples_df[~client_rows_wo_samples_df.index.isin(no_lookup_filtered_df.index)] 

            # Replace NA values with zero, these items has SOH, SO, and PO equals to zero
            client_rows_wo_samples_df.loc[:,'UNIT PRICE'] = client_rows_wo_samples_df.loc[:,'UNIT PRICE'].replace(np.nan, 0)

            # Calculate SOH, PO, and On Order cost
            client_rows_wo_samples_df.loc[:,'SOH Value xgst'] = client_rows_wo_samples_df.loc[:,'UNIT PRICE'] * client_rows_wo_samples_df.loc[:,'qty_onhand']
            client_rows_wo_samples_df.loc[:,'PO Cost xgst'] = client_rows_wo_samples_df.loc[:,'UNIT PRICE'] * client_rows_wo_samples_df.loc[:,'qty_PO']
            client_rows_wo_samples_df.loc[:,'On Order Cost xgst'] = client_rows_wo_samples_df.loc[:,'UNIT PRICE'] * client_rows_wo_samples_df.loc[:,'qty_SO']

            output_buffer = io.BytesIO()
            output_buffer, sum_per_client_sheet = self.build_excel_file_buffer(
                output_buffer=output_buffer,
                cleaned_ssr_df=client_rows_wo_samples_df
            )

            excel_file_bytes, ssr_filename = self.hide_excel_fields_return_excel_bytes(
                output_buffer=output_buffer
            )

            self.read_update_ssr_summary(sum_per_client_sheet=sum_per_client_sheet)

            notification_message = f"Stock Status Report Automation now done! \n" + notification_message

            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

            return excel_file_bytes, ssr_filename, mimetype, notification_message

        except Exception as e:
            logging.exception(f"[Stock Status Report Operation] Failure to automate stock status report. Error: {e}")
            return False

    def excel_bytes_as_df(self) -> pd.DataFrame:
        excel_file = io.BytesIO(self.exported_file_bytes)
        return pd.read_excel(excel_file)
    
    def df_to_excel_bytes(self, sheets: Dict[str, pd.DataFrame]) -> bytes:
        output_buffer = io.BytesIO()

        with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:

            for _sheet_name, _sheet_df in sheets.items():

                _sheet_name = _sheet_name[:31]
                cleaned_df = _sheet_df.dropna(how='all').reset_index(drop=True)
                cleaned_df.to_excel(writer, sheet_name=_sheet_name, index=False)

        return output_buffer.getvalue()

    def get_product_list(self) -> pd.DataFrame:
        logging.info("[StockStatusReportOps] Getting product list")

        site_id = self.sharepoint_ops.get_site_id()
        drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
        csv_bytes = self.sharepoint_ops.get_bytes_for_latest_file_with_prefix(prefix=self.product_list_prefix, drive_id=drive_id)

        product_list = pd.read_csv(io.BytesIO(csv_bytes))

        return product_list

    def get_ssr_summary(self) -> pd.DataFrame:
        logging.info("[StockStatusReportOps] Getting SSR Summary")

        site_id = self.sharepoint_ops.get_site_id()
        drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
        excel_bytes = self.sharepoint_ops.get_bytes_for_latest_file_with_prefix(prefix=self.ssr_summary_prefix, drive_id=drive_id)

        ssr_summary = pd.read_excel(io.BytesIO(excel_bytes))

        return ssr_summary

    def build_excel_file_buffer(self, output_buffer: io.BytesIO, cleaned_ssr_df: pd.DataFrame) -> Tuple[io.BytesIO, Dict]:
        sum_per_client_sheet = {}

        self.client_category = {"CURRENT CUSTOMERS": None, **self.client_category}

        with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:

            for sheet_name, category in self.client_category.items():
                if sheet_name.upper() != 'CURRENT CUSTOMERS':
                    client_sheet_df = cleaned_ssr_df[cleaned_ssr_df['item_cat1'].astype(str).str.strip().astype(str).str.lower().isin({s.lower() for s in category})]

                    sum_per_client_sheet[sheet_name] = {
                        "soh_value_sum": client_sheet_df['SOH Value xgst'].sum(),
                        "po_cost_sum": client_sheet_df['PO Cost xgst'].sum(),
                        "so_cost_sum": client_sheet_df['On Order Cost xgst'].sum()
                    }
                else:
                    client_sheet_df = cleaned_ssr_df
                    
                client_sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)

        output_buffer.seek(0)

        return output_buffer, sum_per_client_sheet
    
    def hide_excel_fields_return_excel_bytes(self, output_buffer: io.BytesIO) -> Tuple[bytes, str]:
        output_buffer.seek(0)
        wb = load_workbook(output_buffer)

        for sheet in wb.sheetnames:
            ws = wb[sheet]
            for col_letter in self.columns_to_hide:
                ws.column_dimensions[col_letter].hidden = True

        final_output = io.BytesIO()
        wb.save(final_output)
        final_output.seek(0)

        ssr_filename = f"STOCK STATUS REPORT {self.australia_now.strftime('%Y%m%d')}.xlsx"        

        return final_output.getvalue(), ssr_filename
    
    def read_update_ssr_summary(self, sum_per_client_sheet: Dict):
        logging.info(f"[StockStatusReportOps] Trying to read and update SSR Summary")

        ssr_summary_df = self.get_ssr_summary()

        new_ssr_summary_df = self.generate_new_ssr_summary(sum_per_client_sheet)

        short_date_now = self.australia_now.strftime('%d-%b')

        excel_buffer = io.BytesIO()

        fiscal_year_start, fiscal_year_end = self.get_start_end_fiscal_year()

        main_sheet_title = f"{fiscal_year_start} - {fiscal_year_end} FY"

        ssr_summary_filename = f"DINA Stock Status Report Overview FY{str(fiscal_year_start)[2:]}-{str(fiscal_year_end)[2:]}.xlsx"

        # Write both DataFrames to different sheets in the same in-memory Excel file
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            ssr_summary_df.to_excel(writer, sheet_name=main_sheet_title, index=False)
            new_ssr_summary_df.to_excel(writer, sheet_name=f'{short_date_now}', index=False)

        # Don't forget to rewind the buffer before using it
        excel_buffer.seek(0)

        site_id = self.sharepoint_ops.get_site_id()
        drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)        
        self.sharepoint_ops.upload_excel_file(drive_id=drive_id, excel_filename=ssr_summary_filename, file_bytes=excel_buffer.getvalue())

    def generate_new_ssr_summary(self, sum_per_client_sheet: Dict) -> pd.DataFrame:
        client_soh_suffix = "SOH LIABILITY"
        client_fields = [
            "SOH VALUE",
            "PO COST",
            "SOH + PO COST", # Sum of SOH VALUE & PO COST
            "SO COST",
            "LIABILITY" # Difference of SOH + PO COST & SO COST
        ]

        ssr_summary = pd.DataFrame([0, 1])

        fiscal_year_start, fiscal_year_end = self.get_start_end_fiscal_year()

        ssr_summary.loc[0,0] = f"{fiscal_year_start} - {fiscal_year_end} FY"

        row = 0
        short_date_now = self.australia_now.strftime('%d-%b')
        for client_name, values in sum_per_client_sheet.items():
            row += 1
            ssr_summary.loc[row,0] = f"{client_name} {client_soh_suffix}"
            ssr_summary.loc[row,1] = short_date_now

            soh_po_sum = 0
            for order, client_field in enumerate(client_fields, start=1):
                row += 1
                ssr_summary.loc[row,0] = client_field

                if order == 1:
                    ssr_summary.loc[row,1] = values.get("soh_value_sum")
                elif order == 2:
                    ssr_summary.loc[row,1] = values.get("po_cost_sum")
                elif order == 3:
                    soh_po_sum = values.get("soh_value_sum") + values.get("po_cost_sum")
                    ssr_summary.loc[row,1] = soh_po_sum
                elif order == 4:
                    ssr_summary.loc[row,1] = values.get("so_cost_sum")
                elif order == 5:
                    ssr_summary.loc[row,1] = soh_po_sum - values.get("so_cost_sum")

            row += 1
            ssr_summary.loc[row,0] = ''

        return ssr_summary
                            
    def get_target_ssr_summary_table_column(self, ssr_summary_df: pd.DataFrame, target_date) -> str:
        # Row 2 (index 1) contains the date-like values
        second_row = ssr_summary_df.iloc[1]

        # Search for the column where the parsed date matches your target
        matched_col_index = None
        for col in ssr_summary_df.columns:
            try:
                val_date = parse(str(second_row[col]), fuzzy=False).date()
                if val_date == target_date:
                    matched_col_index = col
                    break
            except (ValueError, TypeError):
                continue

        return matched_col_index
    
    def get_start_end_fiscal_year(self):
        if self.australia_now.month >= 7:
            fiscal_year_start = self.australia_now.year
            fiscal_year_end = self.australia_now.year + 1
        else:
            fiscal_year_start = self.australia_now.year - 1
            fiscal_year_end = self.australia_now.year  

        return fiscal_year_start, fiscal_year_end