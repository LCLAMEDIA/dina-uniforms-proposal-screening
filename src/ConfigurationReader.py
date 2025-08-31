import pandas as pd
import numpy as np
import io
import logging
from collections import defaultdict
from typing import Dict, List, Optional

class ConfigurationReader:
    """Reads configuration for Open Orders Reporting from SharePoint."""
    
    def __init__(self, sharepoint_ops=None):
        """Initialize the configuration reader."""
        self.sharepoint_ops = sharepoint_ops
        self.config_filename = "OOR_CONFIG.xlsx"
        self.config_path = "/KNOWLEDGE BASE/AUTOMATIONS/OPEN ORDER REPORTING (OOR)"
        
        # Load Inventory file as well
        self.inventory_path = "/KNOWLEDGE BASE/AUTOMATIONS/OPEN ORDER REPORTING (OOR)/Upload"
        self.invetory_prefix = "StockInventory"
        self.invetory_suffix = ".csv"
        
        # Initialize empty configurations
        self.customer_mapping_dict = {}
        self.customer_mapping_fields = []
        self.robot_soh_lookup = {}
    

    def load_configuration(self) -> bool:
        """Load configuration from SharePoint Excel file."""
        if not self.sharepoint_ops:
            logging.warning("[ConfigurationReader] No SharePoint operations instance provided")
            return False
            
        try:
            logging.info("[ConfigurationReader] Loading configuration from SharePoint")
            
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            
            # Get file content by path 
            file_path = f"{self.config_path}/{self.config_filename}"
            logging.info(f"[ConfigurationReader] Looking for config file at: {file_path}")
            
            config_bytes = None
            
            # Try to get file content
            try:
                items = self.sharepoint_ops.list_items_in_folder(
                    drive_id=drive_id, 
                    folder_path=self.config_path
                )
                
                logging.info(f"[ConfigurationReader] Found {len(items)} items in folder")
                for item in items:
                    logging.info(f"[ConfigurationReader] Found item: {item.get('name')} (ID: {item.get('id')})")
                    if item.get('name') == self.config_filename:
                        item_id = item.get('id')
                        logging.info(f"[ConfigurationReader] Found config file with ID: {item_id}")
                        config_bytes = self.sharepoint_ops.get_file_content(
                            drive_id=drive_id,
                            item_id=item_id
                        )
                        if config_bytes:
                            logging.info(f"[ConfigurationReader] Successfully retrieved file content: {len(config_bytes)} bytes")
                        else:
                            logging.warning("[ConfigurationReader] Failed to retrieve file content")
                        break
            except Exception as e:
                logging.warning(f"[ConfigurationReader] Error listing folder: {str(e)}")
            
            if not config_bytes:
                logging.warning(f"[ConfigurationReader] Config file not found: {file_path}")
                return False
                
            # Parse the Excel file
            return self._parse_config_file(config_bytes)
            
        except Exception as e:
            logging.error(f"[ConfigurationReader] Error loading configuration: {str(e)}")
            return False
    

    def load_stock_inventory_file(self) -> bool:
        """Load stock inventory file from SharePoint Excel file."""
        if not self.sharepoint_ops:
            logging.warning("[ConfigurationReader] No SharePoint operations instance provided")
            return False
            
        try:
            logging.info("[ConfigurationReader] Loading configuration from SharePoint")
            
            site_id = self.sharepoint_ops.get_site_id()
            drive_id = self.sharepoint_ops.get_drive_id(site_id=site_id)
            
            # Get file content by path 
            file_path = f"{self.inventory_path}/{self.invetory_prefix}*{self.invetory_suffix}"
            logging.info(f"[ConfigurationReader] Looking for stock inventory file: {file_path}")
            
            inventory_bytes = None
            
            # Try to get file content
            try:
                items = self.sharepoint_ops.list_items_in_folder(
                    drive_id=drive_id, 
                    folder_path=self.inventory_path
                )
                
                logging.info(f"[ConfigurationReader] Found {len(items)} items in folder")
                for item in items:
                    logging.info(f"[ConfigurationReader] Found item: {item.get('name')} (ID: {item.get('id')})")
                    if str(item.get('name', "") or "").lower().startswith(self.invetory_prefix.lower()) and str(item.get('name', "") or "").lower().endswith(self.invetory_suffix.lower()):
                        item_id = item.get('id')
                        logging.info(f"[ConfigurationReader] Found inventory file with ID: {item_id}")
                        inventory_bytes = self.sharepoint_ops.get_file_content(
                            drive_id=drive_id,
                            item_id=item_id
                        )
                        if inventory_bytes:
                            logging.info(f"[ConfigurationReader] Successfully retrieved file content: {len(inventory_bytes)} bytes")
                        else:
                            logging.warning("[ConfigurationReader] Failed to retrieve file content")
                        break
            except Exception as e:
                logging.warning(f"[ConfigurationReader] Error listing folder: {str(e)}")
            
            if not inventory_bytes:
                logging.warning(f"[ConfigurationReader] Inventory file not found: {file_path}")
                return False
                
            # Parse the Excel file
            return self._parse_stock_inventory_file(inventory_bytes=inventory_bytes)
            
        except Exception as e:
            logging.error(f"[ConfigurationReader] Error loading inventory file: {str(e)}")
            return False
    

    def _parse_stock_inventory_file(self, inventory_bytes: bytes) -> bool:
        try:
            csv_file = io.BytesIO(inventory_bytes)
            stock_inventory_df = pd.read_csv(csv_file)

            logging.info(f"[ConfigurationReader] Found inventory file: {stock_inventory_df.shape} / Performing clean up")

            clean_stock_inventory_df = stock_inventory_df.dropna(subset=["Barcode", "stockonhandST"])

            clean_stock_inventory_df = clean_stock_inventory_df[clean_stock_inventory_df["stockonhandST"].apply(lambda x: isinstance(x, int) or (isinstance(x, float) and x.is_integer()))]

            logging.info(f"[ConfigurationReader] Found inventory cleaned: {clean_stock_inventory_df.shape} / Performing clean up")

            robot_soh_lookup_barcode = clean_stock_inventory_df["Barcode"].astype(str).str.strip()

            robot_soh_lookup_qty = clean_stock_inventory_df["stockonhandST"].astype(int)
            
            logging.info(f"[ConfigurationReader] Converting to look up map object")

            self.robot_soh_lookup = dict(zip(robot_soh_lookup_barcode, robot_soh_lookup_qty))
            
            logging.info(f"[ConfigurationReader] File inventory look up map: {self.robot_soh_lookup}")
            
            return True
                
        except Exception as e:
            logging.error(f"[ConfigurationReader] Error parsing inventory file: {str(e)}")
            return False


    def _parse_config_file(self, config_bytes: bytes) -> bool:
        try:
            excel_file = io.BytesIO(config_bytes)
            xls = pd.ExcelFile(excel_file)
            sheets = xls.sheet_names
            
            logging.info(f"[ConfigurationReader] Found sheets: {sheets}")
            
            # Check for customer mapping sheet
            customer_mapping = next((s for s in sheets if 'customer' in s.lower() and 'mapping' in s.lower()), None)
            if customer_mapping:
                logging.info(f"[ConfigurationReader] Using '{customer_mapping}' for customer_mapping data")
                
                # Read customer mapping with header in row 2
                customer_mapping_df = pd.read_excel(
                    excel_file, 
                    sheet_name=customer_mapping,
                    skiprows=1,  # Skip the instruction row
                    header=0     # Use row 2 as header
                )
                
                # Extract customer mapping
                if 'CUSTOMER LABEL' in customer_mapping_df.columns:
                    self.customer_mapping_dict = self._convert_customer_mapping_config(config_df=customer_mapping_df)
                    logging.info(f"[ConfigurationReader] Loaded {len(self.customer_mapping_dict)} look up mapping: {self.customer_mapping_dict}")
                    
                    self.customer_mapping_fields = self._conver_customer_mapping_fields(config_df=customer_mapping_df)
                    logging.info(f"[ConfigurationReader] Loaded {len(self.customer_mapping_fields)} mapping fields: {self.customer_mapping_fields}")

                    logging.info(f"[ConfigurationReader] Loaded {len(self.customer_mapping_dict)} look up mapping: {self.customer_mapping_dict}")
                    logging.info(f"[ConfigurationReader] Loaded {len(self.customer_mapping_fields)} mapping fields: {self.customer_mapping_fields}")
                
            return True
                
        except Exception as e:
            logging.error(f"[ConfigurationReader] Error parsing config file: {str(e)}")
            return False
    

    def _convert_customer_mapping_config(self, config_df: pd.DataFrame):
        customer_mapping_rows = config_df.to_dict(orient="records")

        customer_mapping_dict = defaultdict(lambda: defaultdict(list))

        for row in customer_mapping_rows:
            cust_key = next(k for k in list(row.keys()) if str(k).lower() == "customer label")
            customer_label = row.get(cust_key)
            for k, v in row.items():
                if k == cust_key:
                    continue  # skip grouping key
                if isinstance(v, float) and v is np.nan:
                    continue
                if v is None:
                    continue
                if str(v).strip() == "":
                    continue

                customer_mapping_dict[customer_label][k].append(str(v).strip())
                    
        return {k: dict(v) for k, v in customer_mapping_dict.items()}


    def _conver_customer_mapping_fields(self, config_df: pd.DataFrame):

        mapping_fields = list(config_df.columns)

        mapping_fields_wo_unnamed = [field for field in mapping_fields if 'Unnamed' not in str(field)]
        
        return [field for field in mapping_fields_wo_unnamed if not str(field).lower() == "customer label"]


    def get_customer_mapping_dict(self) -> Dict:
        """Get customer mapping lookup values."""
        return self.customer_mapping_dict
    

    def get_customer_mapping_fields(self) -> List:
        """Get list of mapping fields."""
        return self.customer_mapping_fields


    def get_robot_soh_lookup(self) -> Dict:
        """Get list of mapping fields."""
        return self.robot_soh_lookup
    
