#!/usr/bin/env python3
"""
Cloud SQL Inventory - Main Entry Point
"""
import os
from credentials import get_credentials, list_accessible_projects
from asset_search import search_sql_instances
from sql_details import process_sql_instances
from output import save_to_csv
from sql_optimizer import optimize_sql_inventory  # Import optimizer function

def main():
    # Path to your service account key file
    service_account_file = "/home/ankit/Downloads/developing-gcp-5c21951f5ad6.json"
    
    # Get credentials from service account file
    credentials = get_credentials(service_account_file)
    
    print("Determining scope for asset search...")
    projects = list_accessible_projects(credentials)
    
    if not projects:
        print("No accessible projects found.")
        return
    
    print(f"Found {len(projects)} accessible projects.")
    all_sql_instances = []

    for project_id in projects:
        print(f"Scanning project: {project_id}")
        project_scope = f"projects/{project_id}"
        sql_instances = search_sql_instances(credentials, project_scope)
        
        if sql_instances:
            print(f"  Found {len(sql_instances)} Cloud SQL instances in project {project_id}.")
            all_sql_instances.extend(sql_instances)
        else:
            print(f"  No Cloud SQL instances found in project {project_id}.")

    if all_sql_instances:
        print(f"Processing details for {len(all_sql_instances)} SQL instances...")
        sql_details = process_sql_instances(all_sql_instances, credentials)
        csv_path = 'cloud_sql_inventory.csv'
        save_to_csv(sql_details, csv_path)
        print(f"Cloud SQL inventory has been saved to '{csv_path}'")

        # Now call the optimizer
        print("Running SQL optimizer...")
        optimize_sql_inventory(csv_path)
        print("SQL optimization report generated.")
    else:
        print("No Cloud SQL instances found in any accessible projects.")

if __name__ == '__main__':
    main()
