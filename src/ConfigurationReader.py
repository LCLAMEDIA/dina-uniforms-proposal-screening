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
        
        # Initialize empty configurations
        self.official_brands = []
        self.product_num_mapping = {}
        self.separate_file_customers = []
        self.dedup_customers = []
    
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
            
            # Initialize default configuration values
            self.official_brands = []
            self.product_num_mapping = {}
            self.separate_file_customers = []
            self.dedup_customers = []
            
            # Check for brand sheet
            brand_sheet = next((s for s in sheets if 'brand' in s.lower()), None)
            if brand_sheet:
                logging.info(f"[ConfigurationReader] Using '{brand_sheet}' for brands data")
                
                # Read the first few rows to determine structure
                brand_preview = pd.read_excel(excel_file, sheet_name=brand_sheet, nrows=5)
                logging.debug(f"[ConfigurationReader] Brand sheet preview:\n{brand_preview}")
                
                # Skip instruction rows and read with explicit column names
                brands_df = pd.read_excel(
                    excel_file, 
                    sheet_name=brand_sheet,
                    skiprows=2,  # Skip the instruction row and header row
                    names=['BrandCode', 'Description']  # Explicitly define columns
                )
                
                # Extract brand codes
                if not brands_df.empty:
                    self.official_brands = brands_df['BrandCode'].dropna().tolist()
                    logging.info(f"[ConfigurationReader] Loaded {len(self.official_brands)} official brands: {self.official_brands}")
            else:
                logging.warning("[ConfigurationReader] No brand sheet found")
            
            # Check for customer mapping sheet
            mapping_sheet = next((s for s in sheets if 'product' in s.lower() or 'mapping' in s.lower() or 'customer' in s.lower()), None)
            if mapping_sheet:
                logging.info(f"[ConfigurationReader] Using '{mapping_sheet}' for customer mapping data")
                
                # Read the first few rows to determine structure
                mapping_preview = pd.read_excel(excel_file, sheet_name=mapping_sheet, nrows=5)
                logging.debug(f"[ConfigurationReader] Mapping sheet preview:\n{mapping_preview}")
                
                # Skip instruction rows and read with explicit column names
                mapping_df = pd.read_excel(
                    excel_file, 
                    sheet_name=mapping_sheet,
                    skiprows=2,  # Skip the instruction row and header row
                    names=['Code', 'CustomerName', 'CreateSeparateFile', 'RemoveDuplicates', 'Description']  # Explicitly define columns
                )
                
                # Process customer mappings
                if not mapping_df.empty:
                    # Create customer name mapping - filter out empty/NaN values
                    valid_rows = mapping_df[mapping_df['Code'].notna()]
                    self.product_num_mapping = dict(zip(
                        valid_rows['Code'].astype(str),
                        valid_rows['CustomerName'].astype(str)
                    ))
                    
                    # Create separate file list
                    separate_file_mask = (mapping_df['CreateSeparateFile'].astype(str).str.upper().str.contains('YES')) & mapping_df['Code'].notna()
                    if separate_file_mask.any():
                        self.separate_file_customers = mapping_df.loc[separate_file_mask, 'Code'].tolist()
                    
                    # Create deduplication list
                    dedup_mask = (mapping_df['RemoveDuplicates'].astype(str).str.upper().str.contains('YES')) & mapping_df['Code'].notna()
                    if dedup_mask.any():
                        self.dedup_customers = mapping_df.loc[dedup_mask, 'Code'].tolist()
                    
                    logging.info(f"[ConfigurationReader] Loaded {len(self.product_num_mapping)} product mappings")
                    logging.info(f"[ConfigurationReader] Loaded {len(self.separate_file_customers)} separate file customers: {self.separate_file_customers}")
                    logging.info(f"[ConfigurationReader] Loaded {len(self.dedup_customers)} customers for deduplication: {self.dedup_customers}")
            else:
                logging.warning("[ConfigurationReader] No customer mapping sheet found")
                
            return True
                
        except Exception as e:
            logging.error(f"[ConfigurationReader] Error parsing config file: {str(e)}")
        return False
    
    def get_official_brands(self) -> List[str]:
        """Get list of official brands to filter out."""
        return self.official_brands
    
    def get_product_num_mapping(self) -> Dict[str, str]:
        """Get product number to customer name mapping."""
        return self.product_num_mapping
    
    def get_separate_file_customers(self) -> List[str]:
        """Get list of customers that need separate files."""
        return self.separate_file_customers
    
    def get_dedup_customers(self) -> List[str]:
        """Get list of customers that need deduplication."""
        return self.dedup_customers