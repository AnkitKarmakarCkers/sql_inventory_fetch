#!/usr/bin/env python3
"""
Cloud SQL Inventory - SQL Instance Details
"""
from googleapiclient.discovery import build
from metrics import get_instance_metrics

def get_cloud_sql_details(credentials, project_id, instance_name):
    """Get detailed information about a Cloud SQL instance using SQL Admin API."""
    service = build('sqladmin', 'v1', credentials=credentials)
    
    try:
        response = service.instances().get(project=project_id, instance=instance_name).execute()
        return response
    except Exception as e:
        print(f"Error getting details for SQL instance {instance_name} in project {project_id}: {str(e)}")
        return {}

def process_sql_instances(sql_instances, credentials):
    """Process SQL instances and extract relevant details."""
    sql_details = []
    
    for instance in sql_instances:
        project_id = instance.get('project_id')
        instance_name = instance.get('name')
        print(f"Processing instance: {instance_name} in project {project_id}")
        
        # Get detailed information about the instance
        detailed_info = get_cloud_sql_details(credentials, project_id, instance_name)
        
        # Get metrics
        try:
            metrics = get_instance_metrics(project_id, instance_name, credentials)
        except Exception as e:
            print(f"Error getting metrics for {instance_name}: {str(e)}")
            metrics = {}
        
        settings = detailed_info.get('settings', {})
        ip_config = settings.get('ipConfiguration', {})
        
        # Extract maintenance window information
        maintenance_window = settings.get('maintenanceWindow', {})
        maintenance_day = maintenance_window.get('day', 'Not specified')
        maintenance_hour = maintenance_window.get('hour', 'Not specified')
        maintenance_info = f"{maintenance_day} @ {maintenance_hour}:00" if maintenance_day != 'Not specified' else 'Not specified'
        
        # Extract authorized networks
        authorized_networks = ip_config.get('authorizedNetworks', [])
        auth_networks_str = ', '.join([network.get('value', '') for network in authorized_networks]) if authorized_networks else 'None'
        
        # Extract password policy information
        password_validation_policy = settings.get('passwordValidationPolicy', {})
        password_policy_enabled = 'Yes' if password_validation_policy.get('enablePasswordPolicy', False) else 'No'
        
        # Determine if password authentication is enabled
        auth_settings = settings.get('userLabels', {}).get('auth_type', '').lower()
        password_auth_enabled = 'No' if auth_settings == 'iam_only' else 'Yes'
        
        instance_info = {
            'name': instance_name,
            'project_id': project_id,
            'location': detailed_info.get('region', instance.get('location', '')),
            'database_version': detailed_info.get('databaseVersion', ''),
            'instance_type': detailed_info.get('instanceType', ''),
            'tier': settings.get('tier', ''),
            'availability_type': settings.get('availabilityType', ''),
            'activation_policy': settings.get('activationPolicy', ''),
            'backup_enabled': str(settings.get('backupConfiguration', {}).get('enabled', False)),
            'disk_size_gb': str(settings.get('dataDiskSizeGb', '')),
            'state': detailed_info.get('state', ''),
            'create_time': detailed_info.get('createTime', ''),
            'public_ip': 'Yes' if any(ip.get('type') == 'PRIMARY' for ip in detailed_info.get('ipAddresses', [])) else 'No',
            'private_ip': 'Yes' if any(ip.get('type') == 'PRIVATE' for ip in detailed_info.get('ipAddresses', [])) else 'No',
            'authorized_networks': auth_networks_str,
            'cert_expiry': detailed_info.get('serverCaCert', {}).get('expirationTime', '') if detailed_info.get('serverCaCert') else '',
            'maintenance_window': maintenance_info,
            'password_policy_enabled': password_policy_enabled,
            'password_auth_enabled': password_auth_enabled,
            'deletion_protection': 'Yes' if detailed_info.get('deletionProtection', False) else 'No',
            'cpu_util': f"{metrics.get('database/cpu/utilization', 0):.4f}",
            'memory_util': f"{metrics.get('database/memory/utilization', 0):.4f}",
            'disk_util': f"{metrics.get('database/disk/utilization', 0):.4f}",
            'connections': str(int(metrics.get('database/network/connections', 0))),
            'encrypted': 'Yes' if settings.get('diskEncryptionConfiguration', {}) else 'No'
        }
        
        sql_details.append(instance_info)
    
    return sql_details