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
os.environ.setdefault('OOR_INPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Upload')
os.environ.setdefault('OOR_OUTPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed')

# Import the OpenOrdersReporting class
from OpenOrdersReporting import OpenOrdersReporting
from ConfigurationReader import ConfigurationReader

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
    
    # Create a dummy config reader to get default values
    config_reader = ConfigurationReader()
    
    # Create OfficialBrands sheet
    official_brands_df = pd.DataFrame({
        'BrandCode': config_reader.default_official_brands,
        'Description': [f"Brand {b}" for b in config_reader.default_official_brands]
    })
    
    # Create CustomerCodeMapping sheet
    customer_codes = list(config_reader.default_product_num_mapping.keys())
    customer_names = list(config_reader.default_product_num_mapping.values())
    separate_file = ['Yes' if code in config_reader.default_separate_file_customers else 'No' 
                     for code in customer_codes]
    dedup_customers = ['Yes' if code in config_reader.default_dedup_customers else 'No' 
                      for code in customer_codes]
    
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
    
    # Check if the input file exists
    if not os.path.exists(args.excel_file):
        logger.error(f"Input file does not exist: {args.excel_file}")
        sys.exit(1)
    
    try:
        logger.info(f"Processing file: {args.excel_file}")
        
        # Read the Excel file
        with open(args.excel_file, 'rb') as file:
            excel_file_bytes = file.read()
        
        # Create an instance of OpenOrdersReporting
        if args.dry_run:
            # For dry run, mock the SharePoint uploads and config loading
            with mock_sharepoint_operations(config_file):
                oor = OpenOrdersReporting()
                result = oor.process_excel_file(excel_file_bytes, os.path.basename(args.excel_file))
        else:
            # Normal processing with SharePoint uploads
            oor = OpenOrdersReporting()
            result = oor.process_excel_file(excel_file_bytes, os.path.basename(args.excel_file))
        
        # Output the results
        logger.info("Processing completed successfully")
        logger.info(f"Total rows processed: {result['total_rows']}")
        logger.info(f"Generic rows: {result['generic_rows']}")
        logger.info(f"Calvary rows: {result['calvary_rows']}")
        logger.info(f"Former customer rows: {result['filtered_brand_rows']}")
        logger.info(f"Duplicate orders removed: {result.get('duplicate_orders_removed', 0)}")
        logger.info(f"Other rows: {result['remaining_rows']}")
        logger.info(f"Output files: {', '.join(result['output_files'].values())}")
        logger.info(f"Processing time: {result['duration']:.2f} seconds")
        
    except Exception as e:
        logger.exception(f"Error processing file: {e}")
        sys.exit(1)

class mock_sharepoint_operations:
    """
    Context manager to mock SharePoint operations for testing.
    This mocks both uploads and configuration file reading.
    """
    def __init__(self, config_file=None):
        self.config_file = config_file
    
    def __enter__(self):
        # Setup patching for SharePointOperations
        import builtins
        self.original_import = builtins.__import__
        
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
                        if self.config_file:
                            return [{"name": "OOR_CONFIG.xlsx", "id": "mock-config-id"}]
                        return []
                    
                    def mocked_get_file_content(self, drive_id, item_id):
                        if self.config_file and item_id == "mock-config-id":
                            with open(self.config_file, 'rb') as f:
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