#!/usr/bin/env python3
"""
Cloud SQL Inventory - Metrics Collection
"""
from google.cloud import monitoring_v3
import datetime
import time

def get_instance_metrics(project_id, instance_name, credentials):
    """Get utilization metrics for a specific Cloud SQL instance."""
    client = monitoring_v3.MetricServiceClient(credentials=credentials)
    project_name = f"projects/{project_id}"
    
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10**9)
    end_time = datetime.datetime.fromtimestamp(now)
    start_time = end_time - datetime.timedelta(days=7)
    
    interval = monitoring_v3.TimeInterval(
        {
            "start_time": {"seconds": int(start_time.timestamp()), "nanos": 0},
            "end_time": {"seconds": seconds, "nanos": nanos},
        }
    )
    
    metrics = {}
    metric_types = [
        "cloudsql.googleapis.com/database/cpu/utilization",
        "cloudsql.googleapis.com/database/memory/utilization",
        "cloudsql.googleapis.com/database/disk/utilization",
        "cloudsql.googleapis.com/database/network/connections"
    ]
    
    print(f"Fetching metrics for instance {instance_name} in project {project_id}")
    
    resource_filters = [
        f'resource.labels.database_id="{instance_name}"',
        f'resource.labels.instance_id="{instance_name}"',
        f'resource.labels.database_id="{project_id}:{instance_name}"'
    ]
    
    for metric_type in metric_types:
        metric_name = metric_type.replace("cloudsql.googleapis.com/", "")
        metric_found = False
        
        for resource_filter in resource_filters:
            query = f'metric.type="{metric_type}" AND {resource_filter}'
            print(f"  Trying query: {query}")
            
            try:
                results = client.list_time_series(
                    request={
                        "name": project_name,
                        "filter": query,
                        "interval": interval,
                        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                        "aggregation": {
                            "alignment_period": {"seconds": 604800},
                            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
                        }
                    }
                )
                
                time_series_count = 0
                for time_series in results:
                    time_series_count += 1
                    if time_series.points:
                        metrics[metric_name] = time_series.points[0].value.double_value
                        metric_found = True
                        break
                
                print(f"  Found {time_series_count} time series for {metric_name}")
                
                if metric_found:
                    break
                    
            except Exception as e:
                print(f"  Error querying {metric_name} with filter {resource_filter}: {str(e)}")
        
        if metric_name not in metrics:
            print(f"  No data points found for {metric_name} after trying all filters")
            metrics[metric_name] = 0
    
    return metrics