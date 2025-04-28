#!/usr/bin/env python3
"""
Cloud SQL Inventory - Asset Search
"""
from google.cloud import asset_v1
import json

def search_sql_instances(credentials, scope):
    """Search for SQL instances across projects using Cloud Asset API."""
    client = asset_v1.AssetServiceClient(credentials=credentials)
    
    # Use empty query string - filtering happens via asset_types parameter
    query = ""
    
    print(f"Searching for Cloud SQL instances across {scope}...")
    try:
        response = client.search_all_resources(
            request={
                "scope": scope,
                "query": query,
                "asset_types": ["sqladmin.googleapis.com/Instance"],
            }
        )
        
        sql_instances = []
        for result in response:
            # Extract project from resource name format: //cloudsql.googleapis.com/projects/{project}/instances/{instance}
            # or //sqladmin.googleapis.com/projects/{project}/instances/{instance}
            resource_name = result.name
            project_id = resource_name.split('/')[4] if '/projects/' in resource_name else None
            
            if project_id:
                instance_data = {
                    "name": result.display_name,
                    "project_id": project_id,
                    "resource_name": resource_name,
                    "location": result.location,
                    "raw_resource": json.loads(result.additional_attributes.value) if result.additional_attributes else {}
                }
                sql_instances.append(instance_data)
            
        return sql_instances
    except Exception as e:
        print(f"Error searching for SQL instances: {str(e)}")
        return []