#!/usr/bin/env python3
"""
Cloud SQL Inventory - Main Entry Point
"""
import os
from credentials import get_credentials, list_accessible_projects
from asset_search import search_sql_instances
from sql_details import process_sql_instances
from output import save_to_csv

def main():
    # Path to your service account key file
    service_account_file = "/home/ankit/Downloads/developing-gcp-5c21951f5ad6.json"
    
    # Get credentials from service account file
    credentials = get_credentials(service_account_file)
    
    # Determine the scope - you can use project, folder, or organization level
    print("Determining scope for asset search...")
    projects = list_accessible_projects(credentials)
    
    if not projects:
        print("No accessible projects found.")
        return
    
    print(f"Found {len(projects)} accessible projects.")
    
    all_sql_instances = []
    
    # Uncomment one of these approaches based on your permissions:
    
    # Option 1: Organization-level scan (if you have org-level permissions)
    # organization_id = "organizations/123456789"  # Replace with your org ID
    # all_sql_instances = search_sql_instances(credentials, organization_id)
    
    # Option 2: Folder-level scan (if you have folder-level permissions)
    # folder_id = "folders/123456789"  # Replace with your folder ID
    # all_sql_instances = search_sql_instances(credentials, folder_id)
    
    # Option 3: Project-by-project scan (default)
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
        save_to_csv(sql_details, 'cloud_sql_inventory.csv')
        print(f"Total Cloud SQL instances found: {len(sql_details)}")
        print(f"Cloud SQL inventory has been saved to 'cloud_sql_inventory.csv'")
    else:
        print("No Cloud SQL instances found in any accessible projects.")

if __name__ == '__main__':
    main()