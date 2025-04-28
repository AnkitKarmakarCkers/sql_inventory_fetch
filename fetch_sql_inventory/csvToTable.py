#!/usr/bin/env python3
"""
CSV to Table Converter - Converts CSV files to formatted text tables
"""
import csv
import sys
import os

def convert_csv_to_table(csv_filename):
    """Convert a CSV file to a formatted text table."""
    if not os.path.exists(csv_filename):
        print(f"Error: File {csv_filename} not found.")
        return False
    
    # Generate output filename
    base_name = os.path.splitext(csv_filename)[0]
    txt_filename = f"{base_name}_table.txt"
    
    try:
        # Read CSV data
        with open(csv_filename, 'r', newline='') as csv_file:
            reader = csv.reader(csv_file)
            headers = next(reader)  # Get header row
            rows = list(reader)     # Get all data rows
        
        if not rows:
            print(f"Warning: No data found in {csv_filename}")
            with open(txt_filename, 'w') as txt_file:
                txt_file.write("No data found in CSV file.\n")
            return True
        
        # Calculate column widths (maximum width of any value in that column)
        col_widths = [max(len(str(row[i])) if i < len(row) else 0 for row in [headers] + rows) + 2 
                     for i in range(len(headers))]
        
        # Generate the table
        with open(txt_filename, 'w') as txt_file:
            # Create header row
            header_row = '|' + '|'.join(f' {headers[i]:<{col_widths[i]-2}} ' for i in range(len(headers))) + '|'
            separator = '+' + '+'.join('-' * width for width in col_widths) + '+'
            
            # Write header
            txt_file.write(separator + '\n')
            txt_file.write(header_row + '\n')
            txt_file.write(separator + '\n')
            
            # Write data rows
            for row in rows:
                # Make sure row has enough values
                row_data = row + [''] * (len(headers) - len(row))
                data_row = '|' + '|'.join(f' {str(row_data[i]):<{col_widths[i]-2}} ' for i in range(len(headers))) + '|'
                txt_file.write(data_row + '\n')
            
            # Write bottom separator
            txt_file.write(separator + '\n')
        
        print(f"Table has been saved to {txt_filename}")
        return True
    
    except Exception as e:
        print(f"Error converting CSV to table: {str(e)}")
        return False

def main():
    """Main function to handle command line usage."""
    if len(sys.argv) != 2:
        print("Usage: python csvToTable.py <csv_filename>")  # Fixed to match the actual filename
        sys.exit(1)
    
    csv_filename = sys.argv[1]
    success = convert_csv_to_table(csv_filename)
    
    if not success:
        sys.exit(1)

if __name__ == '__main__':
    main()