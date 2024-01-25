from google.cloud import storage
from yaml import load, safe_load, YAMLError, FullLoader
from os import environ as env
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import monitoring_v3
from google.oauth2 import service_account
import datetime



def get_storage_metrics(project_id, key_path, metric_type, filter_str):
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(env["GCP_SERVICE_CREDS"], Loader=FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    client = monitoring_v3.MetricServiceClient(credentials=scoped_credentials)

    project_name = f"projects/{project_id}"
    interval = monitoring_v3.TimeInterval()
    interval.end_time = datetime.datetime.utcnow()
    interval.start_time = interval.end_time - datetime.timedelta(minutes=5)

    results = client.list_time_series(
        name=project_name,
        filter=filter_str,
        interval=interval,
        view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    )

    for result in results:
        print(f"Resource: {result.resource.type}, {result.resource.labels}")
        print(f"Metric: {result.metric.type}")
        for point in result.points:
            print(f"  Value: {point.value}")
            print(f"  Start Time: {point.interval.start_time}")
            print(f"  End Time: {point.interval.end_time}")
            print("\n")


if __name__ == "__main__":
    # Replace these values with your own
    project_id = "gitlab-analysis"
    metric_type = "compute.googleapis.com/instance/disk/write_bytes_count"  # Replace with your desired metric type
    filter_str = f'metric.type="{metric_type}"'

    get_storage_metrics(project_id, key_path, metric_type, filter_str)
