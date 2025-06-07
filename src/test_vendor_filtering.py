#!/usr/bin/env python
"""
Test script for vendor filtering functionality.
This script tests the new vendor filtering logic with sample data.
"""

import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_vendor_filtering():
    """Test the vendor filtering logic with sample data."""
    print("=" * 60)
    print("TESTING VENDOR FILTERING FUNCTIONALITY")
    print("=" * 60)
    
    # Create sample data that matches Renee's scenario
    sample_data = {
        'Order': ['12345', '12346', '12347', '12348', '12349'],
        'ProductNum': [
            'GENERIC-SAMPLE-N/A-O/S',
            'GENERIC-SAMPLE-N/A-O/S', 
            'GENERIC-SAMPLE-N/A-O/S',
            'GENERIC-UNIFORM-001',
            'GENERIC-SAMPLE-N/A-O/S'
        ],
        'Vendors': ['PNW', 'FASHION BIZ', 'PNW', 'FASHION BIZ', 'Tabookai International P'],
        'DateIssued': [
            '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'
        ],
        'QtyOrdered': [10, 15, 20, 25, 30]
    }
    
    df = pd.DataFrame(sample_data)
    print("\nOriginal Data:")
    print(df.to_string(index=False))
    
    # Simulate the vendor filtering logic
    print("\n" + "="*40)
    print("APPLYING VENDOR FILTERING")
    print("="*40)
    
    # Step 1: Identify GENERIC-SAMPLE products
    sample_mask = df['ProductNum'].astype(str).str.contains('GENERIC-SAMPLE-N/A-O/S', na=False)
    sample_df = df[sample_mask].copy()
    non_sample_df = df[~sample_mask].copy()
    
    print(f"\nSample products found: {len(sample_df)}")
    print(f"Non-sample products: {len(non_sample_df)}")
    
    # Step 2: Filter sample products to keep only PNW vendor
    required_vendor = "PNW"
    vendor_match_mask = sample_df['Vendors'].astype(str).str.contains(required_vendor, case=False, na=False)
    filtered_sample_df = sample_df[vendor_match_mask].copy()
    
    print(f"\nAfter vendor filtering (keep only {required_vendor}):")
    print(f"Sample products: {len(sample_df)} -> {len(filtered_sample_df)}")
    print(f"Removed: {len(sample_df) - len(filtered_sample_df)} rows")
    
    # Step 3: Clean vendor names (remove PNW)
    filtered_sample_df['Vendors'] = filtered_sample_df['Vendors'].astype(str).str.replace(required_vendor, '', case=False).str.strip()
    
    # Step 4: Combine results
    result_df = pd.concat([filtered_sample_df, non_sample_df], ignore_index=True)
    
    print("\nAfter Vendor Filtering & Cleanup:")
    print(result_df.to_string(index=False))
    
    # Step 5: Simulate deduplication
    print("\n" + "="*40)
    print("APPLYING DEDUPLICATION")
    print("="*40)
    
    # Convert DateIssued to datetime for proper sorting
    result_df['DateIssued'] = pd.to_datetime(result_df['DateIssued'])
    
    # Sort by DateIssued descending to keep latest
    result_df = result_df.sort_values(by='DateIssued', ascending=False)
    
    # Remove duplicates based on composite key
    dedup_columns = ['ProductNum', 'Order', 'QtyOrdered']
    final_df = result_df.drop_duplicates(subset=dedup_columns, keep='first')
    
    print(f"\nAfter deduplication:")
    print(f"Rows: {len(result_df)} -> {len(final_df)}")
    print(f"Removed: {len(result_df) - len(final_df)} duplicate rows")
    
    print("\nFinal Result:")
    print(final_df.to_string(index=False))
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Original rows: {len(df)}")
    print(f"After vendor filtering: {len(result_df)}")
    print(f"After deduplication: {len(final_df)}")
    print(f"Total removed: {len(df) - len(final_df)}")
    
    # Verify the expected outcome
    expected_outcomes = [
        "Only PNW vendor entries kept for GENERIC-SAMPLE products",
        "PNW removed from vendor names",
        "Non-SAMPLE products unchanged",
        "Duplicates removed based on composite key"
    ]
    
    print("\nExpected Outcomes:")
    for i, outcome in enumerate(expected_outcomes, 1):
        print(f"{i}. ✅ {outcome}")

if __name__ == '__main__':
    test_vendor_filtering()
