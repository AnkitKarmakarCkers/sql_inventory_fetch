#!/usr/bin/env python3
"""
Cloud SQL Inventory - Credentials Management
"""
from google.oauth2 import service_account
from googleapiclient.discovery import build

def get_credentials(service_account_file):
    """Get credentials from service account key file."""
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, 
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    print(f"Using service account email: {credentials.service_account_email}")
    return credentials

def list_accessible_projects(credentials):
    """List all projects the service account has access to."""
    service = build('cloudresourcemanager', 'v1', credentials=credentials)
    
    try:
        request = service.projects().list()
        projects = []
        
        while request is not None:
            response = request.execute()
            projects.extend(response.get('projects', []))
            request = service.projects().list_next(previous_request=request, previous_response=response)
        
        return [p['projectId'] for p in projects if p.get('lifecycleState') == 'ACTIVE']
    except Exception as e:
        print(f"Error listing projects: {str(e)}")
        return []