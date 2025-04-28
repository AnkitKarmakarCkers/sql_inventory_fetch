#!/usr/bin/env python3
"""
Cloud SQL Inventory - Output Handling
"""
import csv
import os
import sys
import subprocess

def save_to_csv(data, filename='cloud_sql_inventory.csv'):
    """Save the Cloud SQL inventory data to a CSV file."""
    if not data:
        print("No data to save")
        return
    
    fieldnames = list(data[0].keys())
    
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Data has been saved to {filename}")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_to_table_script = os.path.join(script_dir, 'csvToTable.py')  # Make sure this matches your actual filename
    
    try:
        subprocess.run([sys.executable, csv_to_table_script, filename], check=True)
        print(f"Table view has been generated")
    except subprocess.CalledProcessError as e:
        print(f"Error generating table view: {e}")
    except FileNotFoundError as e:
        print(f"Error: {e}. Make sure csvToTable.py exists in the same directory.")