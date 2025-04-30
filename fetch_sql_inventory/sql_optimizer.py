#!/usr/bin/env python3
"""
Cloud SQL Optimizer - Generate recommendations for cost optimization
with regional GCP pricing model and usage-based estimates
"""
import csv
import sys
import os
import json
import re
from datetime import datetime
import pandas as pd

# GCP Cloud SQL Pricing Model (USD)
# Source: https://cloud.google.com/sql/pricing (simplified for implementation)
GCP_PRICING = {
    # Format: region: {"cpu": price per vCPU per hour, "memory": price per GB per hour, "storage": price per GB per month}
    "us-central1": {"cpu": 0.0413, "memory": 0.007, "storage": 0.17},
    "us-east1": {"cpu": 0.0386, "memory": 0.0065, "storage": 0.17},
    "us-east4": {"cpu": 0.0458, "memory": 0.0077, "storage": 0.17},
    "us-west1": {"cpu": 0.0413, "memory": 0.007, "storage": 0.17},
    "us-west2": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17},
    "us-west3": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17},
    "us-west4": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17},
    "europe-west1": {"cpu": 0.0454, "memory": 0.0077, "storage": 0.17},
    "europe-west2": {"cpu": 0.0541, "memory": 0.0092, "storage": 0.17},
    "europe-west3": {"cpu": 0.0541, "memory": 0.0092, "storage": 0.17},
    "europe-west4": {"cpu": 0.0454, "memory": 0.0077, "storage": 0.17},
    "europe-west6": {"cpu": 0.0588, "memory": 0.01, "storage": 0.17},
    "europe-north1": {"cpu": 0.0454, "memory": 0.0077, "storage": 0.17},
    "asia-east1": {"cpu": 0.0503, "memory": 0.0085, "storage": 0.17},
    "asia-east2": {"cpu": 0.0588, "memory": 0.01, "storage": 0.17},
    "asia-northeast1": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17},
    "asia-northeast2": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17},
    "asia-northeast3": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17},
    "asia-southeast1": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17},
    "asia-south1": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17},
    "australia-southeast1": {"cpu": 0.0536, "memory": 0.0091, "storage": 0.17},
    # Default pricing for regions not explicitly listed
    "default": {"cpu": 0.0495, "memory": 0.0084, "storage": 0.17}
}

# Database version pricing modifiers (some versions have price premiums)
DB_VERSION_MODIFIER = {
    "MYSQL_8_0": 1.0,  # Standard pricing
    "POSTGRES_13": 1.0,  # Standard pricing
    "SQLSERVER_2019_STANDARD": 1.25,  # 25% premium
    "SQLSERVER_2019_ENTERPRISE": 2.0,  # Higher premium for enterprise
    "default": 1.0  # Default modifier
}

# High Availability pricing multiplier
HA_MODIFIER = 2.0  # HA doubles the instance cost

# Minimum instance parameters
MIN_VCPU = 1  # Minimum vCPU for standard instances
MIN_MEMORY_GB = 3.75  # Minimum memory in GB
MIN_DISK_SIZE_GB = 10  # Minimum disk size in GB

def get_db_version_modifier(db_version):
    """Get pricing modifier based on database version."""
    for key in DB_VERSION_MODIFIER:
        if key in db_version:
            return DB_VERSION_MODIFIER[key]
    return DB_VERSION_MODIFIER["default"]

def get_region_pricing(region):
    """Get pricing for a specific region."""
    if region in GCP_PRICING:
        return GCP_PRICING[region]
    return GCP_PRICING["default"]

def load_sql_inventory(csv_filename):
    """Load Cloud SQL inventory data from CSV file."""
    if not os.path.exists(csv_filename):
        print(f"Error: File {csv_filename} not found.")
        return []
    
    try:
        # Read CSV data
        with open(csv_filename, 'r', newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            return list(reader)
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        return []

def extract_machine_specs(tier):
    """Extract vCPUs and memory from machine tier."""
    try:
        # Check for shared-core instances first
        if 'small' in tier.lower() or 'micro' in tier.lower() or 'f1-micro' in tier.lower():
            # These are shared-core instances
            if 'small' in tier.lower():
                return 0.5, 1920  # 0.5 vCPU, 1.875 GB
            else:  # micro
                return 0.25, 614  # 0.25 vCPU, 0.6 GB
                
        if 'custom' in tier.lower():
            # Example format: db-custom-2-7680 (2 vCPUs, 7680 MB memory)
            parts = tier.split('-')
            vcpus = int(parts[-2])
            memory_mb = int(parts[-1])
            return vcpus, memory_mb
        elif 'standard' in tier.lower():
            # Handle standard tiers like db-standard-2
            parts = tier.split('-')
            vcpus = int(parts[-1])
            memory_mb = vcpus * 3840  # Standard tiers have fixed ratio
            return vcpus, memory_mb
        elif 'highmem' in tier.lower():
            # Handle highmem tiers
            parts = tier.split('-')
            vcpus = int(parts[-1])
            memory_mb = vcpus * 6656  # Highmem tiers have more memory
            return vcpus, memory_mb
        elif 'highcpu' in tier.lower():
            # Handle highcpu tiers
            parts = tier.split('-')
            vcpus = int(parts[-1])
            memory_mb = vcpus * 1024  # Highcpu tiers have less memory
            return vcpus, memory_mb
        else:
            # Default to minimal instance if can't parse
            return MIN_VCPU, MIN_MEMORY_GB * 1024
    except (IndexError, ValueError):
        # Default to minimal instance if can't parse
        return MIN_VCPU, MIN_MEMORY_GB * 1024

def is_at_minimum_spec(tier):
    """Check if the instance is already at minimum specifications."""
    vcpus, memory_mb = extract_machine_specs(tier)
    
    # Check if shared-core instance
    is_shared_core = 'small' in tier.lower() or 'micro' in tier.lower() or 'f1-micro' in tier.lower()
    
    # If it's a shared-core or at minimum specs
    if is_shared_core or (vcpus <= MIN_VCPU and memory_mb/1024 <= MIN_MEMORY_GB):
        return True
    
    return False

def get_instance_recommendations(instance):
    """Generate recommendations for a single SQL instance."""
    recommendations = []
    
    # Extract and convert metrics
    try:
        cpu_util = float(instance.get('cpu_util', '0'))
        memory_util = float(instance.get('memory_util', '0'))
        disk_util = float(instance.get('disk_util', '0'))
        disk_size_gb = int(instance.get('disk_size_gb', '0'))
        tier = instance.get('tier', '')
        instance_state = instance.get('state', '')
        activation_policy = instance.get('activation_policy', '')
        connections = int(instance.get('connections', '0'))
        vcpus, memory_mb = extract_machine_specs(tier)
        memory_gb = memory_mb / 1024
    except ValueError as e:
        return [f"Error processing metrics: {str(e)}"]
    
    # Skip instances that are not running
    if instance_state != 'RUNNABLE':
        recommendations.append(f"Instance is in {instance_state} state. No optimization possible until it's running.")
        return recommendations
    
    # Check if instance is already at minimum specs
    at_minimum_specs = is_at_minimum_spec(tier)
    
    # Check connections vs vCPUs (rule of thumb: ~100 connections per vCPU is reasonable)
    if vcpus > 1 and connections < (vcpus * 20) and not at_minimum_specs:
        recommendations.append(f"Low connection count ({connections}) relative to vCPUs ({vcpus}). Consider reducing vCPUs.")
    
    # Check CPU utilization with more detailed recommendations
    is_shared_core = 'small' in tier.lower() or 'micro' in tier.lower() or 'f1-micro' in tier.lower()
    
    if cpu_util < 0.05:
        if vcpus > 1 and not at_minimum_specs:
            recommendations.append(f"CPU utilization very low (<5%). Current: {vcpus} vCPUs. Recommend reducing to {max(1, vcpus // 2)} vCPUs.")
        elif not is_shared_core and not at_minimum_specs:
            recommendations.append("CPU utilization very low (<5%). Consider switching to a shared-core instance type.")
    elif cpu_util < 0.2:
        if vcpus > 2 and not at_minimum_specs:
            recommendations.append(f"CPU utilization low (<20%). Current: {vcpus} vCPUs. Recommend reducing to {max(1, int(vcpus * 0.6))} vCPUs.")
        elif not at_minimum_specs:
            recommendations.append("CPU utilization low (<20%). Monitor if this usage pattern continues.")
    elif cpu_util > 0.8:
        recommendations.append(f"CPU utilization high (>80%). Consider upgrading to {vcpus + 2} vCPUs for better performance.")
    
    # Check memory utilization with specific recommendations
    if memory_util < 0.3 and memory_gb > MIN_MEMORY_GB:
        new_memory_mb = max(MIN_MEMORY_GB * 1024, int(memory_mb * 0.7))  # Reduce by 30% but minimum 3.75GB
        new_memory_gb = new_memory_mb / 1024
        
        # Only suggest if there's an actual reduction
        if new_memory_gb < memory_gb:
            recommendations.append(f"Memory utilization low (<30%). Current: {memory_gb:.1f} GB. Recommend reducing to {new_memory_gb:.1f} GB.")
    elif memory_util > 0.85:
        new_memory_mb = int(memory_mb * 1.3)  # Increase by 30%
        new_memory_gb = new_memory_mb / 1024
        recommendations.append(f"Memory utilization high (>85%). Consider increasing memory from {memory_gb:.1f} GB to {new_memory_gb:.1f} GB.")
    
    # Check disk utilization with specific recommendations
    if disk_util < 0.2 and disk_size_gb > MIN_DISK_SIZE_GB:
        new_disk_size = max(MIN_DISK_SIZE_GB, int(disk_size_gb * 0.6))  # Reduce by 40% but minimum 10GB
        if new_disk_size < disk_size_gb:  # Only suggest if there's an actual reduction
            recommendations.append(f"Disk utilization very low ({disk_util:.1%}) with {disk_size_gb} GB. Consider reducing to {new_disk_size} GB.")
    elif disk_util < 0.5 and disk_size_gb > 100:
        new_disk_size = max(MIN_DISK_SIZE_GB, int(disk_size_gb * 0.7))  # Reduce by 30% but minimum 10GB
        if new_disk_size < disk_size_gb:  # Only suggest if there's an actual reduction
            recommendations.append(f"Disk utilization low ({disk_util:.1%}) with {disk_size_gb} GB. Consider reducing to {new_disk_size} GB.")
    elif disk_util > 0.85:
        new_disk_size = int(disk_size_gb * 1.3)  # Increase by 30%
        recommendations.append(f"Disk utilization high ({disk_util:.1%}). Consider increasing disk size from {disk_size_gb} GB to {new_disk_size} GB.")
    
    # Check activation policy
    if activation_policy == 'NEVER' and cpu_util == 0 and memory_util == 0:
        recommendations.append("Instance never activated but provisioned. Consider deleting if not needed.")
    
    # Check for unused instance
    if connections == 0 and cpu_util < 0.01 and instance_state == 'RUNNABLE':
        recommendations.append("Instance appears unused (no connections, negligible CPU usage). Consider stopping or deleting if not needed.")
    
    # If at minimum specs and still underutilized, give different advice
    if at_minimum_specs and cpu_util < 0.2 and memory_util < 0.3:
        # Remove any recommendations about reducing resources (since we can't)
        recommendations = [rec for rec in recommendations if not ('reducing' in rec.lower() or 'reduce' in rec.lower())]
        # Add alternative recommendation if not already there
        if not any('unused' in rec for rec in recommendations):
            recommendations.append("Instance is already at minimum specifications. Consider instance consolidation or stopping if not needed.")
    
    # If no specific recommendations, add a general one
    if not recommendations:
        recommendations.append("Instance appears to be appropriately sized based on current utilization.")
    
    return recommendations

def generate_cost_saving_estimate(instance, recommendations):
    """Generate estimated cost savings based on recommendations."""
    # Extract instance details
    tier = instance.get('tier', '')
    region = instance.get('location', 'us-central1') # Default to us-central1 if missing
    db_version = instance.get('database_version', '')
    availability_type = instance.get('availability_type', 'ZONAL')
    disk_size_gb = int(instance.get('disk_size_gb', '0'))
    
    # Get pricing for the instance's region
    pricing = get_region_pricing(region)
    
    # Get DB version pricing modifier
    db_modifier = get_db_version_modifier(db_version)
    
    # Extract machine type specs
    vcpus, memory_mb = extract_machine_specs(tier)
    memory_gb = memory_mb / 1024
    
    # Calculate if HA is enabled
    ha_modifier = HA_MODIFIER if availability_type == 'REGIONAL' else 1.0
    
    # Calculate monthly costs (730 hours in a month)
    cpu_cost_per_month = vcpus * pricing["cpu"] * 730 * db_modifier * ha_modifier
    memory_cost_per_month = memory_gb * pricing["memory"] * 730 * db_modifier * ha_modifier
    storage_cost_per_month = disk_size_gb * pricing["storage"] * ha_modifier
    
    # Total current monthly cost
    estimated_current_cost = cpu_cost_per_month + memory_cost_per_month + storage_cost_per_month
    
    # Check if instance is already at minimum specs
    at_minimum_specs = is_at_minimum_spec(tier)
    
    # Initialize default values
    new_vcpus = vcpus
    new_memory_gb = memory_gb
    new_disk_size = disk_size_gb
    
    # Extract optimization suggestions from recommendations
    no_optimization_possible = False
    if at_minimum_specs and any("already at minimum specifications" in rec for rec in recommendations):
        no_optimization_possible = True
    
    if not no_optimization_possible:
        for rec in recommendations:
            # CPU recommendations
            cpu_match = re.search(r"reducing to (\d+) vCPUs", rec)
            if cpu_match:
                new_vcpus = int(cpu_match.group(1))
            
            # Memory recommendations
            memory_match = re.search(r"reducing to (\d+\.\d+) GB", rec)
            if memory_match:
                new_memory_gb = float(memory_match.group(1))
            
            # Disk recommendations
            disk_match = re.search(r"reducing to (\d+) GB", rec)
            if disk_match:
                new_disk_size = int(disk_match.group(1))
    
    # Calculate optimized monthly costs
    optimized_cpu_cost = new_vcpus * pricing["cpu"] * 730 * db_modifier * ha_modifier
    optimized_memory_cost = new_memory_gb * pricing["memory"] * 730 * db_modifier * ha_modifier
    optimized_storage_cost = new_disk_size * pricing["storage"] * ha_modifier
    
    # Total optimized monthly cost
    estimated_optimized_cost = optimized_cpu_cost + optimized_memory_cost + optimized_storage_cost
    
    # Ensure we don't have negative savings (which indicates an error)
    if estimated_optimized_cost > estimated_current_cost:
        # If optimized cost is higher, revert to current values
        estimated_optimized_cost = estimated_current_cost
        optimized_cpu_cost = cpu_cost_per_month
        optimized_memory_cost = memory_cost_per_month
        optimized_storage_cost = storage_cost_per_month
    
    # Calculate savings
    savings = estimated_current_cost - estimated_optimized_cost
    savings_percentage = (savings / estimated_current_cost * 100) if estimated_current_cost > 0 else 0
    
    # Handle case where no optimization is possible
    if no_optimization_possible or savings <= 0:
        savings = 0
        savings_percentage = 0
        estimated_optimized_cost = estimated_current_cost
        optimized_cpu_cost = cpu_cost_per_month
        optimized_memory_cost = memory_cost_per_month
        optimized_storage_cost = storage_cost_per_month
    
    # Generate detailed cost breakdown
    cost_details = {
        "current": {
            "cpu": f"${cpu_cost_per_month:.2f}",
            "memory": f"${memory_cost_per_month:.2f}",
            "storage": f"${storage_cost_per_month:.2f}",
            "total": f"${estimated_current_cost:.2f}"
        },
        "optimized": {
            "cpu": f"${optimized_cpu_cost:.2f}",
            "memory": f"${optimized_memory_cost:.2f}",
            "storage": f"${optimized_storage_cost:.2f}",
            "total": f"${estimated_optimized_cost:.2f}"
        },
        "savings": {
            "monthly": f"${savings:.2f}",
            "percentage": f"{savings_percentage:.1f}%",
            "annual": f"${savings * 12:.2f}"
        },
        "no_optimization_possible": no_optimization_possible
    }
    
    return cost_details

def generate_optimization_report(instances):
    """Generate a full optimization report for all instances."""
    total_current_cost = 0
    total_optimized_cost = 0
    
    report = []
    report.append("=== Cloud SQL Instance Optimization Report ===")
    report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total instances analyzed: {len(instances)}")
    report.append("")
    
    for instance in instances:
        name = instance.get('name', 'Unknown')
        project_id = instance.get('project_id', 'Unknown')
        tier = instance.get('tier', 'Unknown')
        region = instance.get('location', 'Unknown')
        db_version = instance.get('database_version', 'Unknown')
        availability_type = instance.get('availability_type', 'ZONAL')
        
        try:
            cpu_util = float(instance.get('cpu_util', '0'))
            memory_util = float(instance.get('memory_util', '0'))
            disk_util = float(instance.get('disk_util', '0'))
            connections = int(instance.get('connections', '0'))
        except ValueError as e:
            report.append(f"Instance: {name} (Project: {project_id})")
            report.append(f"  Error processing metrics: {str(e)}")
            report.append("")
            continue
        
        # Extract machine specs
        vcpus, memory_mb = extract_machine_specs(tier)
        memory_gb = memory_mb / 1024
        
        report.append(f"Instance: {name} (Project: {project_id})")
        report.append(f"  Region: {region}")
        report.append(f"  Database Version: {db_version}")
        report.append(f"  High Availability: {'Yes' if availability_type == 'REGIONAL' else 'No'}")
        report.append(f"  Current configuration: {tier} ({vcpus} vCPUs, {memory_gb:.2f} GB memory), {instance.get('disk_size_gb', '0')} GB storage")
        report.append(f"  Usage Statistics:")
        report.append(f"    - CPU: {cpu_util:.1%} avg. utilization")
        report.append(f"    - Memory: {memory_util:.1%} avg. utilization")
        report.append(f"    - Storage: {disk_util:.1%} utilization")
        report.append(f"    - Connections: {connections} active connections")
        
        recommendations = get_instance_recommendations(instance)
        
        report.append("  Recommendations:")
        for rec in recommendations:
            report.append(f"    - {rec}")
        
        cost_details = generate_cost_saving_estimate(instance, recommendations)
        
        report.append("  Cost Analysis (Based on GCP pricing for region {0}):".format(region))
        report.append("    Current Monthly Costs:")
        report.append(f"      - Compute (CPU): {cost_details['current']['cpu']}")
        report.append(f"      - Memory: {cost_details['current']['memory']}")
        report.append(f"      - Storage: {cost_details['current']['storage']}")
        report.append(f"      - Total: {cost_details['current']['total']}")
        
        if cost_details.get('no_optimization_possible', False) or float(cost_details['savings']['monthly'].replace('$', '')) <= 0:
            report.append("    Optimized Monthly Costs: No cost optimization possible for this instance")
            report.append("    Potential Savings: $0.00 (0.0%)")
        else:
            report.append("    Optimized Monthly Costs:")
            report.append(f"      - Compute (CPU): {cost_details['optimized']['cpu']}")
            report.append(f"      - Memory: {cost_details['optimized']['memory']}")
            report.append(f"      - Storage: {cost_details['optimized']['storage']}")
            report.append(f"      - Total: {cost_details['optimized']['total']}")
            report.append("    Potential Savings:")
            report.append(f"      - Monthly: {cost_details['savings']['monthly']}")
            report.append(f"      - Annual: {cost_details['savings']['annual']}")
            report.append(f"      - Percentage Reduction: {cost_details['savings']['percentage']}")
        
        report.append("")
        
        try:
            current_cost = float(cost_details['current']['total'].replace('$', ''))
            optimized_cost = float(cost_details['optimized']['total'].replace('$', ''))
            total_current_cost += current_cost
            total_optimized_cost += optimized_cost
        except (ValueError, KeyError):
            pass
    
    # Add summary
    total_savings = total_current_cost - total_optimized_cost
    savings_percentage = (total_savings / total_current_cost * 100) if total_current_cost > 0 else 0
    annual_savings = total_savings * 12
    
    report.append("=== Summary ===")
    report.append(f"Total current estimated monthly cost: ${total_current_cost:.2f}")
    report.append(f"Total optimized estimated monthly cost: ${total_optimized_cost:.2f}")
    report.append(f"Total potential monthly savings: ${total_savings:.2f} ({savings_percentage:.1f}%)")
    report.append(f"Projected annual savings: ${annual_savings:.2f}")
    report.append("")
    report.append("Note: Cost estimates are based on GCP Cloud SQL pricing.")
    report.append("      Actual costs may vary based on commitment discounts, network usage, and other factors.")
    report.append("      Instances already at minimum specifications will show no potential savings.")
    
    return report

def optimize_sql_inventory(csv_file_path):
    """Main function to optimize SQL inventory from a CSV file."""
    try:
        # Load data (either using pandas or CSV file)
        try:
            # Try pandas first (as in your original function)
            df = pd.read_csv(csv_file_path)
            print(f"Loaded {len(df)} entries from inventory for optimization using pandas.")
            # Convert DataFrame to list of dictionaries for our processing functions
            instances = df.to_dict('records')
        except (ImportError, FileNotFoundError):
            # Fall back to regular CSV reading if pandas fails
            instances = load_sql_inventory(csv_file_path)
            print(f"Loaded {len(instances)} entries from inventory for optimization using CSV.")
        
        if not instances:
            print(f"No Cloud SQL instances found in {csv_file_path}")
            return False
        
        # Generate the optimization report
        report = generate_optimization_report(instances)
        
        # Generate output filenames with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Generate standard report filename
        base_name = os.path.splitext(csv_file_path)[0]
        report_filename = f"{base_name}_optimization_report.txt"
        
        # Generate timestamped recommendation filename
        recommendation_filename = f"recommendation-{timestamp}.txt"
        
        # Write report to standard file
        with open(report_filename, 'w') as report_file:
            for line in report:
                report_file.write(f"{line}\n")
        
        # Write report to timestamped recommendation file
        with open(recommendation_filename, 'w') as recommendation_file:
            for line in report:
                recommendation_file.write(f"{line}\n")
        
        print(f"Optimization report has been saved to:")
        print(f"  - {report_filename} (standard output)")
        print(f"  - {recommendation_filename} (timestamped recommendation)")
        
        return True
    
    except Exception as e:
        print(f"Error during SQL optimization: {str(e)}")
        return False

def main():
    """Main function to handle command line usage."""
    # Use default filename if none provided
    if len(sys.argv) == 1:
        csv_filename = "cloud_sql_inventory.csv"
        print(f"No filename provided, using default: {csv_filename}")
    elif len(sys.argv) == 2:
        csv_filename = sys.argv[1]
    else:
        print("Usage: python sql_optimizer.py [csv_filename]")
        print("If no filename is provided, 'cloud_sql_inventory.csv' will be used by default.")
        sys.exit(1)
    
    # Check if file exists
    if not os.path.exists(csv_filename):
        print(f"Error: File {csv_filename} not found in the current directory.")
        print(f"Current working directory: {os.getcwd()}")
        print("Available CSV files:")
        for file in os.listdir('.'):
            if file.endswith('.csv'):
                print(f"  - {file}")
        sys.exit(1)
    
    success = optimize_sql_inventory(csv_filename)
    
    if not success:
        print("SQL optimization failed.")
        sys.exit(1)

if __name__ == '__main__':
    main()