#!/usr/bin/env python
"""
Local test script for OpenOrdersReporting class.
This script reads a local Excel file, processes it, and uploads the results to SharePoint.
"""

import os
import sys
import logging
import argparse
import pandas as pd
import io
from datetime import datetime

# Set up environment variables if not already set
os.environ.setdefault('OOR_INPUT_PREFIX', 'OOR')
os.environ.setdefault('OOR_INPUT_PATH', '/KNOWLEDGE BASE/AUTOMATIONS/OPEN ORDER REPORTING (OOR)/Upload')
os.environ.setdefault('OOR_OUTPUT_PATH', '/KNOWLEDGE BASE/AUTOMATIONS/OPEN ORDER REPORTING (OOR)/Processed')

# Import the OpenOrdersReporting class
from OpenOrdersReporting import OpenOrdersReporting
from ConfigurationReader import ConfigurationReader
from AzureOperations import AzureOperations
from SharePointOperations import SharePointOperations

# Define the OOR_CONFIG path
OOR_CONFIG_PATH = '/Shared Documents/KNOWLEDGE BASE/AUTOMATIONS/OPEN ORDER REPORTING (OOR)/OOR_CONFIG.xlsx'

def setup_logging():
    """Configure logging with a more detailed format for debugging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"oor_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('OORTest')

def debug_excel_content(file_bytes, filename="config.xlsx"):
    """
    Debug function to examine Excel file content.
    
    Args:
        file_bytes: The Excel file content as bytes
        filename: Name of the file for logging purposes
    """
    logger = logging.getLogger('OORTest')
    try:
        # Create a BytesIO object from the file bytes
        excel_file = io.BytesIO(file_bytes)
        
        # Read the Excel file
        df_dict = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')
        
        # Log the sheets found
        logger.info(f"Excel file '{filename}' contains sheets: {list(df_dict.keys())}")
        
        # For each sheet, log its structure
        for sheet_name, df in df_dict.items():
            logger.info(f"\nSheet '{sheet_name}' structure:")
            logger.info(f"Columns: {list(df.columns)}")
            logger.info(f"Number of rows: {len(df)}")
            logger.info(f"First few rows:\n{df.head()}")
            
            # Check for any empty or problematic columns
            for col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Column '{col}' has {null_count} null values")
                
                # Check for empty strings
                if df[col].dtype == 'object':
                    empty_count = (df[col].astype(str) == '').sum()
                    if empty_count > 0:
                        logger.warning(f"Column '{col}' has {empty_count} empty strings")
        
    except Exception as e:
        logger.error(f"Error examining Excel file: {str(e)}")
        raise

def fetch_sharepoint_file(file_path):
    """
    Fetch a file from SharePoint using the Azure API.
    
    Args:
        file_path: The path to the file in SharePoint
        
    Returns:
        bytes: The file content if successful, None otherwise
    """
    logger = logging.getLogger('OORTest')
    try:
        # Initialize Azure and SharePoint connections
        azure_ops = AzureOperations()
        access_token = azure_ops.get_access_token()
        
        if not access_token:
            logger.error("Failed to obtain Azure access token")
            return None
            
        sharepoint_ops = SharePointOperations(access_token=access_token)
        site_id = sharepoint_ops.get_site_id()
        
        if not site_id:
            logger.error("Failed to get SharePoint site ID")
            return None
            
        drive_id = sharepoint_ops.get_drive_id(site_id=site_id)
        
        if not drive_id:
            logger.error("Failed to get SharePoint drive ID")
            return None
        
        # List items in the folder to get the file ID
        folder_path = os.path.dirname(file_path)
        items = sharepoint_ops.list_items_in_folder(drive_id, folder_path)
        
        # Find the file in the items list
        file_name = os.path.basename(file_path)
        file_item = next((item for item in items if item.get('name') == file_name), None)
        
        if not file_item:
            logger.error(f"File not found in SharePoint: {file_path}")
            return None
        
        # Get the file content
        file_content = sharepoint_ops.get_file_content(drive_id, file_item['id'])
        
        if file_content:
            logger.info(f"Successfully fetched file from SharePoint: {file_path}")
            return file_content
        else:
            logger.error(f"Failed to get file content from SharePoint: {file_path}")
            return None
            
    except Exception as e:
        logger.exception(f"Error fetching file from SharePoint: {str(e)}")
        return None

def create_test_config(output_path="test_config.xlsx"):
    """
    Create a test configuration Excel file with the default values.
    
    Parameters:
    - output_path: Path where the test configuration file will be saved
    
    Returns:
    - Path to the created configuration file
    """
    logger = logging.getLogger('OORTest')
    logger.info(f"Creating test configuration file at {output_path}")
    
    # Define test configuration values for fuzzy matching demo
    test_official_brands = ['BIS', 'OLD']  # Brands to filter out
    test_product_mapping = {
        'SAK': 'Calvary (Little Company of Mary)',
        'NRM': 'NRMA Parks & Resorts',
        'RUM': 'Richmond United'
    }
    test_separate_file_customers = ['SAK', 'NRM', 'RUM']  # All customers get separate files for demo
    
    # Create OfficialBrands sheet
    official_brands_df = pd.DataFrame({
        'BrandCode': test_official_brands,
        'Description': [f"Brand {b}" for b in test_official_brands]
    })
    
    # Create CustomerCodeMapping sheet
    customer_codes = list(test_product_mapping.keys())
    customer_names = list(test_product_mapping.values())
    separate_file = ['Yes' if code in test_separate_file_customers else 'No' 
                     for code in customer_codes]
    dedup_customers = ['No' for _ in customer_codes]  # No deduplication for demo
    
    customer_mapping_df = pd.DataFrame({
        'Code': customer_codes,
        'CustomerName': customer_names,
        'CreateSeparateFile': separate_file,
        'RemoveDuplicates': dedup_customers,
        'Description/Notes': [f"Customer {name}" for name in customer_names]
    })
    
    # Create Excel file with sheets
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Add notes to OfficialBrands sheet
        worksheet = writer.book.create_sheet("OfficialBrands")
        worksheet['A1'] = "This table defines brand codes that will be filtered out during processing."
        worksheet['A2'] = "The system extracts codes from product numbers by taking everything before the first hyphen."
        worksheet['A3'] = "**How to use this table:**"
        worksheet['A4'] = "1. Add brand codes that should be completely filtered out from processing"
        worksheet['A5'] = "2. Typically these are former customers that no longer need reporting"
        worksheet['A6'] = "3. Any product code starting with these values will be removed from the main output"
        
        # Add data to OfficialBrands starting at row 7
        official_brands_df.to_excel(writer, sheet_name='OfficialBrands', startrow=6, index=False)
        
        # Add notes to CustomerCodeMapping sheet
        worksheet = writer.book.create_sheet("CustomerCodeMapping")
        worksheet['A1'] = "This mapping table converts product codes to customer names and controls processing rules."
        worksheet['A2'] = "**How to use this table:**"
        worksheet['A3'] = "1. 'Code' column: Enter the product code prefix that appears before the first hyphen"
        worksheet['A4'] = "2. 'CustomerName' column: Enter the full customer name to display in reports"
        worksheet['A5'] = "3. 'CreateSeparateFile' column: Enter 'Yes' to create a dedicated file for this customer"
        worksheet['A6'] = "4. 'RemoveDuplicates' column: Enter 'Yes' to remove duplicate orders"
        worksheet['A7'] = "5. When adding new customers, complete all columns to ensure proper processing"
        worksheet['A8'] = "6. For codes without hyphens (like 'GENERIC'), the entire value is used for matching"
        
        # Add data to CustomerCodeMapping starting at row 9
        customer_mapping_df.to_excel(writer, sheet_name='CustomerCodeMapping', startrow=8, index=False)
    
    logger.info(f"Created test configuration file with {len(official_brands_df)} brands and {len(customer_mapping_df)} customers")
    return output_path

def main():
    """Main function to process an Excel file with OpenOrdersReporting."""
    logger = setup_logging()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test OpenOrdersReporting with a local Excel file')
    parser.add_argument('excel_file', help='Path to the Excel file to process')
    parser.add_argument('--dry-run', action='store_true', help='Process the file but don\'t upload to SharePoint')
    parser.add_argument('--create-config', action='store_true', help='Create a test configuration file')
    parser.add_argument('--config-file', help='Path to a configuration file to use (creates one if not exists)')
    parser.add_argument('--use-sharepoint-config', action='store_true', 
                       help='Use the SharePoint OOR_CONFIG file instead of local config')
    parser.add_argument('--fetch-from-sharepoint', action='store_true',
                       help='Fetch the input file from SharePoint instead of using local file')
    parser.add_argument('--debug-config', action='store_true',
                       help='Debug the configuration file content')
    
    args = parser.parse_args()
    
    # Check if we need to create a test configuration file
    config_file = None
    if args.create_config:
        config_file = create_test_config()
    elif args.config_file:
        if not os.path.exists(args.config_file):
            logger.info(f"Configuration file {args.config_file} does not exist, creating it")
            config_file = create_test_config(args.config_file)
        else:
            config_file = args.config_file
    
    try:
        # Get the Excel file content
        if args.fetch_from_sharepoint:
            logger.info(f"Fetching file from SharePoint: {args.excel_file}")
            excel_file_bytes = fetch_sharepoint_file(args.excel_file)
            if not excel_file_bytes:
                logger.error("Failed to fetch file from SharePoint")
                sys.exit(1)
        else:
            # Check if the input file exists
            if not os.path.exists(args.excel_file):
                logger.error(f"Input file does not exist: {args.excel_file}")
                sys.exit(1)
            
            # Read the Excel file
            with open(args.excel_file, 'rb') as file:
                excel_file_bytes = file.read()
        
        # Debug configuration if requested
        if args.debug_config:
            if args.use_sharepoint_config:
                config_bytes = fetch_sharepoint_file(OOR_CONFIG_PATH)
                if config_bytes:
                    debug_excel_content(config_bytes, "SharePoint OOR_CONFIG.xlsx")
            elif config_file:
                with open(config_file, 'rb') as f:
                    debug_excel_content(f.read(), config_file)
        
        # Create an instance of OpenOrdersReporting
        if args.dry_run:
            # For dry run, mock the SharePoint uploads and config loading
            with mock_sharepoint_operations(config_file, use_sharepoint_config=args.use_sharepoint_config):
                oor = OpenOrdersReporting()
                result = oor.process_excel_file(excel_file_bytes, os.path.basename(args.excel_file))
        else:
            # Normal processing with SharePoint uploads
            oor = OpenOrdersReporting()
            result = oor.process_excel_file(excel_file_bytes, os.path.basename(args.excel_file))
        
        # Output the results
        logger.info("Processing completed successfully")
        logger.info(f"Total rows processed: {result['total_rows']}")
        logger.info(f"Former customer rows: {result['filtered_brand_rows']}")
        logger.info(f"Duplicate orders removed: {result.get('duplicate_orders_removed', 0)}")
        logger.info(f"Other rows: {result['remaining_rows']}")
        logger.info(f"Output files: {', '.join(result['output_files'].values())}")
        
        # Display fuzzy matching statistics if available
        if 'fuzzy_matching' in result:
            fuzzy_stats = result['fuzzy_matching']
            logger.info(f"Fuzzy matching results:")
            logger.info(f"  - OurRef matches: {fuzzy_stats.get('ourref_matches', 0)}")
            logger.info(f"  - ShipAddress matches: {fuzzy_stats.get('shipaddress_matches', 0)}")
            logger.info(f"  - Total matched: {fuzzy_stats.get('total_matched', 0)}")
        
        logger.info(f"Processing time: {result['duration']:.2f} seconds")
        
    except Exception as e:
        logger.exception(f"Error processing file: {e}")
        sys.exit(1)

class mock_sharepoint_operations:
    """
    Context manager to mock SharePoint operations for testing.
    This mocks both uploads and configuration file reading.
    """
    def __init__(self, config_file=None, use_sharepoint_config=False):
        self.config_file = config_file
        self.use_sharepoint_config = use_sharepoint_config
    
    def __enter__(self):
        # Setup patching for SharePointOperations
        import builtins
        self.original_import = builtins.__import__
        
        # Capture config variables for the closure
        config_file = self.config_file
        use_sharepoint_config = self.use_sharepoint_config
        
        def patched_import(name, *args, **kwargs):
            module = self.original_import(name, *args, **kwargs)
            
            if name == 'SharePointOperations' or getattr(module, '__name__', None) == 'SharePointOperations':
                # Patch the SharePointOperations class
                if hasattr(module, 'SharePointOperations'):
                    # Patch file upload
                    if hasattr(module.SharePointOperations, 'upload_file_to_path'):
                        original_upload = module.SharePointOperations.upload_file_to_path
                        
                        def mocked_upload(self, drive_id, file_path, file_name, file_bytes, content_type="text/csv"):
                            logger = logging.getLogger('OORTest')
                            logger.info(f"[DRY RUN] Would upload file {file_name} to {file_path}")
                            # Don't actually upload
                            return
                        
                        module.SharePointOperations.upload_file_to_path = mocked_upload
                    
                    # Add mock methods for configuration loading
                    def mocked_get_site_id(self):
                        return "mock-site-id"
                    
                    def mocked_get_drive_id(self, site_id):
                        return "mock-drive-id"
                    
                    def mocked_list_items_in_folder(self, drive_id, folder_path):
                        if use_sharepoint_config:
                            # Return the SharePoint OOR_CONFIG path
                            return [{"name": "OOR_CONFIG.xlsx", "id": "mock-config-id", "path": OOR_CONFIG_PATH}]
                        elif config_file:
                            return [{"name": "OOR_CONFIG.xlsx", "id": "mock-config-id"}]
                        return []
                    
                    def mocked_get_file_content(self, drive_id, item_id):
                        if use_sharepoint_config:
                            # Try to read from the SharePoint OOR_CONFIG path
                            try:
                                with open(OOR_CONFIG_PATH, 'rb') as f:
                                    return f.read()
                            except FileNotFoundError:
                                logger = logging.getLogger('OORTest')
                                logger.error(f"SharePoint OOR_CONFIG file not found at {OOR_CONFIG_PATH}")
                                return None
                        elif config_file and item_id == "mock-config-id":
                            with open(config_file, 'rb') as f:
                                return f.read()
                        return None
                    
                    # Add the mock methods
                    module.SharePointOperations.get_site_id = mocked_get_site_id
                    module.SharePointOperations.get_drive_id = mocked_get_drive_id
                    module.SharePointOperations.list_items_in_folder = mocked_list_items_in_folder
                    module.SharePointOperations.get_file_content = mocked_get_file_content
                
            return module
        
        # Store the config file for the mock methods to use
        self.config_file = self.config_file
        
        # Apply the patching
        builtins.__import__ = patched_import
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original import
        import builtins
        builtins.__import__ = self.original_import

if __name__ == '__main__':
    main()