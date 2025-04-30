#!/usr/bin/env python
"""
Local test script for OpenOrdersReporting class.
This script reads a local Excel file, processes it, and uploads the results to SharePoint.
"""

import os
import sys
import logging
import argparse
from datetime import datetime

# Set up environment variables if not already set
os.environ.setdefault('OOR_INPUT_PREFIX', 'OOR')
os.environ.setdefault('OOR_INPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Upload')
os.environ.setdefault('OOR_OUTPUT_PATH', '/Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed')

# Import the OpenOrdersReporting class
from OpenOrdersReporting import  OpenOrdersReporting

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

def main():
    """Main function to process an Excel file with OpenOrdersReporting."""
    logger = setup_logging()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test OpenOrdersReporting with a local Excel file')
    parser.add_argument('excel_file', help='Path to the Excel file to process')
    parser.add_argument('--dry-run', action='store_true', help='Process the file but don\'t upload to SharePoint')
    
    args = parser.parse_args()
    
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
            # For dry run, mock the SharePoint uploads
            with mock_sharepoint_uploads():
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
        logger.info(f"Other rows: {result['remaining_rows']}")
        logger.info(f"Output files: {', '.join(result['output_files'].values())}")
        logger.info(f"Processing time: {result['duration']:.2f} seconds")
        
    except Exception as e:
        logger.exception(f"Error processing file: {e}")
        sys.exit(1)

class mock_sharepoint_uploads:
    """Context manager to mock SharePoint uploads for dry run testing."""
    def __enter__(self):
        # Setup patching for SharePointOperations.upload_file_to_path
        import builtins
        self.original_import = builtins.__import__
        
        def patched_import(name, *args, **kwargs):
            module = self.original_import(name, *args, **kwargs)
            
            if name == 'SharePointOperations' or getattr(module, '__name__', None) == 'SharePointOperations':
                # Patch the SharePointOperations class
                if hasattr(module, 'SharePointOperations'):
                    original_upload = module.SharePointOperations.upload_file_to_path
                    
                    def mocked_upload(self, drive_id, file_path, file_name, file_bytes, content_type="text/csv"):
                        logger = logging.getLogger('OORTest')
                        logger.info(f"[DRY RUN] Would upload file {file_name} to {file_path}")
                        # Don't actually upload
                        return
                    
                    module.SharePointOperations.upload_file_to_path = mocked_upload
                
            return module
        
        builtins.__import__ = patched_import
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original import
        import builtins
        builtins.__import__ = self.original_import

if __name__ == '__main__':
    main()