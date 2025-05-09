import pandas as pd
import io
import logging
from typing import Dict, List, Optional

class ConfigurationReader:
    """Reads configuration for Open Orders Reporting from SharePoint."""
    
    def __init__(self, sharepoint_ops=None):
        """Initialize the configuration reader."""
        self.sharepoint_ops = sharepoint_ops
        self.config_filename = "OOR_CONFIG.xlsx"
        self.config_path = "/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)"
        
        # Default configurations
        self.default_official_brands = [
            'COA', 'BUP', 'CSR', 'CNR', 'BUS', 'CAL', 'IMB', 'JET',
            'JETSTAR', 'JS', 'NRMA', 'MTS', 'SCENTRE', 'SYD', 'RFDS', 'RFL'
        ]
        
        self.default_product_num_mapping = {
            'SAK': 'SHARKS AT KARELLA', 'BW': 'BUSWAYS',
            'CLY': 'CALVARY', 'IMB': 'IMB', 'DC': 'Dolphins',
            'SG': 'ST George', 'CCC': 'CCC', 'DNA': 'DNATA', 'DOLP': 'DOLPHINS',
            'END': 'ESHS', 'GCL': 'GROWTH CIVIL LANDSCAPES', 'GYM': 'GYMEA TRADES',
            'RHH': 'REDHILL', 'RPA': 'REGAL REXNORR', 'SEL': 'SEASONS LIVING',
            'STAR': 'STAR AVIATION', 'YAE': 'YOUNG ACADEMICS', 'ZAM': 'ZAMBARERO',
            'STG': 'DRAGONS', 'KGT': 'KNIGHTS', 'SEL-SEASON': 'SEASON LIVING',
            'SGL': 'ST GEORGE LEAGUES', 'RRA': 'REGAL REXNORD', 'CRAIG SMITH': 'CRAIG SMITH',
            'TRADES GOLF CLUB': 'TRADES GOLF CLUB', 'MYTILENIAN': 'HOUSE',
            'BUS': 'BUSWAYS', 'COA': 'Coal Services'
        }
        
        self.default_taskqueue_mapping = {
            'Data Entry CHK': 'DATA ENTRY CHECK', 'CS HOLDING ORDERS': 'CS HOLD Q!',
            'CAL ROLLOUT DATES': 'CALL ROLLOUT DATE', 'CAL DISPATCH BY LOCATION': 'CAL DISPATCH BY LOCATION Q',
            'CANCEL ORDERS 2B DEL': 'CANCEL Q'
        }
        
        # Default separate file customers
        self.default_separate_file_customers = ['CLY', 'CAL']
        
        # Default customers needing deduplication
        self.default_dedup_customers = []
        
        # Initialize with defaults
        self.official_brands = self.default_official_brands.copy()
        self.product_num_mapping = self.default_product_num_mapping.copy()
        self.taskqueue_mapping = self.default_taskqueue_mapping.copy()
        self.separate_file_customers = self.default_separate_file_customers.copy()
        self.dedup_customers = self.default_dedup_customers.copy()
    
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
    
    def _parse_config_file(self, config_bytes: bytes) -> bool:
        """Parse configuration from Excel file bytes with correct header handling."""
        try:
            excel_file = io.BytesIO(config_bytes)
            xls = pd.ExcelFile(excel_file)
            sheets = xls.sheet_names
            
            logging.info(f"[ConfigurationReader] Found sheets: {sheets}")
            
            # Load official brands - headers in row 2 (index 1), data starts in row 3 (index 2)
            if 'OfficialBrands' in sheets:
                brands_df = pd.read_excel(
                    excel_file, 
                    sheet_name='OfficialBrands',
                    header=1,  # Row 2 (index 1) contains the headers
                    skiprows=[0]  # Skip row 1 (index 0) with instructions
                )
                
                if 'BrandCode' in brands_df.columns:
                    self.official_brands = brands_df['BrandCode'].dropna().tolist()
                    logging.info(f"[ConfigurationReader] Loaded {len(self.official_brands)} official brands: {self.official_brands}")
                else:
                    logging.warning(f"[ConfigurationReader] 'BrandCode' column not found. Available columns: {list(brands_df.columns)}")
            
            # Load customer code mapping - headers in row 2 (index 1), data starts in row 3 (index 2)
            if 'CustomerCodeMapping' in sheets:
                mapping_df = pd.read_excel(
                    excel_file, 
                    sheet_name='CustomerCodeMapping',
                    header=1,  # Row 2 (index 1) contains the headers
                    skiprows=[0]  # Skip row 1 (index 0) with instructions
                )
                
                # Reset lists for configuration
                self.separate_file_customers = []
                self.dedup_customers = []
                
                if 'Code' in mapping_df.columns and 'CustomerName' in mapping_df.columns:
                    # Create customer name mapping - filter out empty/NaN values
                    valid_rows = mapping_df[mapping_df['Code'].notna()]
                    self.product_num_mapping = dict(zip(
                        valid_rows['Code'].astype(str),
                        valid_rows['CustomerName'].astype(str)
                    ))
                    
                    # Create separate file list if column exists
                    if 'CreateSeparateFile' in mapping_df.columns:
                        separate_file_mask = (mapping_df['CreateSeparateFile'].astype(str).str.upper() == 'YES') & mapping_df['Code'].notna()
                        if separate_file_mask.any():
                            self.separate_file_customers = mapping_df.loc[separate_file_mask, 'Code'].tolist()
                    
                    # Create deduplication list if column exists
                    if 'RemoveDuplicates' in mapping_df.columns:
                        dedup_mask = (mapping_df['RemoveDuplicates'].astype(str).str.upper() == 'YES') & mapping_df['Code'].notna()
                        if dedup_mask.any():
                            self.dedup_customers = mapping_df.loc[dedup_mask, 'Code'].tolist()
                    
                    logging.info(f"[ConfigurationReader] Loaded {len(self.product_num_mapping)} product mappings")
                    logging.info(f"[ConfigurationReader] Loaded {len(self.separate_file_customers)} separate file customers: {self.separate_file_customers}")
                    logging.info(f"[ConfigurationReader] Loaded {len(self.dedup_customers)} customers for deduplication: {self.dedup_customers}")
                else:
                    logging.warning(f"[ConfigurationReader] Required columns not found. Available columns: {list(mapping_df.columns)}")
            
            return True
            
        except Exception as e:
            logging.error(f"[ConfigurationReader] Error parsing config file: {str(e)}")
            # Revert to defaults
            self.official_brands = self.default_official_brands.copy()
            self.product_num_mapping = self.default_product_num_mapping.copy()
            self.taskqueue_mapping = self.default_taskqueue_mapping.copy()
            self.separate_file_customers = self.default_separate_file_customers.copy()
            self.dedup_customers = self.default_dedup_customers.copy()
            return False
    
    def get_official_brands(self) -> List[str]:
        """Get list of official brands to filter out."""
        return self.official_brands
    
    def get_product_num_mapping(self) -> Dict[str, str]:
        """Get product number to customer name mapping."""
        return self.product_num_mapping
    
    def get_taskqueue_mapping(self) -> Dict[str, str]:
        """Get task queue to checking note mapping."""
        return self.taskqueue_mapping
    
    def get_separate_file_customers(self) -> List[str]:
        """Get list of customers that need separate files."""
        return self.separate_file_customers
    
    def get_dedup_customers(self) -> List[str]:
        """Get list of customers that need deduplication."""
        return self.dedup_customers