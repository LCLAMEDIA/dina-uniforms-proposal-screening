import io
import logging
import pandas as pd

class SharePointConfigReader:
    """
    Retrieves and parses configuration data from SharePoint Excel files.
    """
    def __init__(self, sharepoint_ops, config_prefix="OOR_CONFIG"):
        """
        Initialize the configuration reader.
        
        Args:
            sharepoint_ops: Initialized SharePointOperations instance
            config_prefix: Prefix of the configuration file in SharePoint
        """
        self.sharepoint_ops = sharepoint_ops
        self.config_prefix = config_prefix
        self.config_data = {}
        self.logger = logging.getLogger(__name__)
    
    def load_config(self, drive_id):
        """
        Load configuration from SharePoint.
        
        Args:
            drive_id: SharePoint drive ID
            
        Returns:
            bool: True if configuration was loaded successfully
        """
        try:
            config_path = os.environ.get('OOR_CONFIG_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/')
            self.logger.info(f"[ConfigReader] Loading configuration with prefix: {self.config_prefix} from path: {config_path}")
            
            # Get all files in the path
            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{config_path}:/children"
            
            headers = {
                'Authorization': f'Bearer {self.sharepoint_ops.access_token}'
            }
            
            response = requests.request("GET", url, headers=headers)
            
            if response.status_code != 200:
                self.logger.error(f"[ConfigReader] Failed to retrieve files from path: {config_path}")
                return False
                
            response_dict = response.json()
            files = response_dict.get("value", [])
            
            # Filter files by prefix and sort by last modified date
            config_files = [f for f in files if f.get("name", "").startswith(self.config_prefix)]
            if not config_files:
                self.logger.error(f"[ConfigReader] No files with prefix {self.config_prefix} found in path {config_path}")
                return False
                
            # Sort by last modified date (newest first)
            config_files.sort(key=lambda x: x.get("lastModifiedDateTime", ""), reverse=True)
            
            # Get the most recent file
            latest_file = config_files[0]
            download_url = latest_file.get("@microsoft.graph.downloadUrl")
            
            if not download_url:
                self.logger.error(f"[ConfigReader] Failed to get download URL for file: {latest_file.get('name')}")
                return False
                
            # Download the file
            file_response = requests.get(download_url)
            if file_response.status_code != 200:
                self.logger.error(f"[ConfigReader] Failed to download file: {latest_file.get('name')}")
                return False
                
            config_bytes = file_response.content
            
            # Parse the configuration Excel file
            return self._parse_config_file(config_bytes)
            
        except Exception as e:
            self.logger.error(f"[ConfigReader] Error loading configuration: {str(e)}")
            return False
    
    def _parse_config_file(self, config_bytes):
        """
        Parse the configuration Excel file into usable dictionaries.
        
        Args:
            config_bytes: Excel file content as bytes
            
        Returns:
            bool: True if parsing was successful
        """
        try:
            excel_file = io.BytesIO(config_bytes)
            
            # Get sheet names to check what's available
            xls = pd.ExcelFile(excel_file)
            sheet_names = xls.sheet_names
            
            self.logger.info(f"[ConfigReader] Found sheets in config: {sheet_names}")
            
            # Load brand codes sheet if available
            if 'BrandCodes' in sheet_names:
                brand_codes_df = pd.read_excel(excel_file, sheet_name='BrandCodes')
                if not brand_codes_df.empty and 'BrandCode' in brand_codes_df.columns:
                    self.config_data['official_brands'] = brand_codes_df['BrandCode'].tolist()
                    self.logger.info(f"[ConfigReader] Loaded {len(self.config_data['official_brands'])} brand codes")
            
            # Load product mapping sheet if available
            if 'ProductMapping' in sheet_names:
                product_mapping_df = pd.read_excel(excel_file, sheet_name='ProductMapping')
                
                # Basic mapping from Code to CustomerName for customer display
                if not product_mapping_df.empty and 'Code' in product_mapping_df.columns and 'CustomerName' in product_mapping_df.columns:
                    self.config_data['product_num_mapping'] = dict(zip(
                        product_mapping_df['Code'], 
                        product_mapping_df['CustomerName']
                    ))
                    self.logger.info(f"[ConfigReader] Loaded {len(self.config_data['product_num_mapping'])} product mappings")
                
                # Enhanced processing rules for each product code
                if not product_mapping_df.empty and 'Code' in product_mapping_df.columns:
                    # Check for the additional processing columns
                    has_separate_file = 'CreateSeparateFile' in product_mapping_df.columns
                    has_remove_duplicates = 'RemoveDuplicates' in product_mapping_df.columns
                    
                    # Create processing rules dictionary
                    processing_rules = {}
                    
                    for _, row in product_mapping_df.iterrows():
                        code = row['Code']
                        rule = {
                            'customer_name': row['CustomerName'] if 'CustomerName' in product_mapping_df.columns else code,
                        }
                        
                        # Add separate file flag if available
                        if has_separate_file:
                            rule['create_separate_file'] = (
                                str(row['CreateSeparateFile']).strip().lower() == 'yes'
                            )
                        
                        # Add remove duplicates flag if available
                        if has_remove_duplicates:
                            rule['remove_duplicates'] = (
                                str(row['RemoveDuplicates']).strip().lower() == 'yes'
                            )
                        
                        processing_rules[code] = rule
                    
                    self.config_data['processing_rules'] = processing_rules
                    self.logger.info(f"[ConfigReader] Loaded {len(processing_rules)} product processing rules")
            
            # Load task queue mapping if available
            if 'TaskQueueMapping' in sheet_names:
                taskqueue_df = pd.read_excel(excel_file, sheet_name='TaskQueueMapping')
                if not taskqueue_df.empty and 'TaskValue' in taskqueue_df.columns and 'NoteValue' in taskqueue_df.columns:
                    self.config_data['taskqueue_mapping'] = dict(zip(
                        taskqueue_df['TaskValue'], 
                        taskqueue_df['NoteValue']
                    ))
                    self.logger.info(f"[ConfigReader] Loaded {len(self.config_data['taskqueue_mapping'])} task queue mappings")
            
            return True
            
        except Exception as e:
            self.logger.error(f"[ConfigReader] Error parsing config file: {str(e)}")
            return False
    
    def get_official_brands(self):
        """Get the list of official brands that should be filtered out."""
        return self.config_data.get('official_brands', [])
    
    def get_product_num_mapping(self):
        """Get the product number to full name mapping dictionary."""
        return self.config_data.get('product_num_mapping', {})
    
    def get_taskqueue_mapping(self):
        """Get the task queue to note value mapping dictionary."""
        return self.config_data.get('taskqueue_mapping', {})
    
    def get_processing_rules(self):
        """Get the processing rules for product codes."""
        return self.config_data.get('processing_rules', {})