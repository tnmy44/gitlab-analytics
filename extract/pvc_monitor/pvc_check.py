from google.cloud import storage
from yaml import load, safe_load, YAMLError, FullLoader
from os import environ as env
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import monitoring_v3
from google.oauth2 import service_account
import datetime
import time

from google.cloud import monitoring_v3


def get_storage_metrics(project_id, metric_type, filter_str):
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(env["GCP_SERVICE_CREDS"], Loader=FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    project_name = f"projects/{project_id}"
    client = monitoring_v3.MetricServiceClient(credentials=scoped_credentials)

    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": (seconds - 1200), "nanos": nanos},
        }
    )

    results = client.list_time_series(
        request={
            "name": project_name,
            "filter": f'metric.type = f"{metric_type}"',
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )
    for result in results:
        print(result)


    # full_metric_type = f"{project_name}/metricDescriptors/{metric_type}"
#
    # descriptor = client.get_metric_descriptor(name=full_metric_type)
    # print(descriptor)

    # interval = monitoring_v3.TimeInterval()
    # interval.end_time = datetime.datetime.utcnow()
    # interval.start_time = interval.end_time - datetime.timedelta(minutes=5)
#
    # results = client.list_time_series(
    #     name=project_name,
    #     filter=filter_str,
    #     interval=interval,
    #     view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    # )
#
    # for result in results:
    #     print(f"Resource: {result.resource.type}, {result.resource.labels}")
    #     print(f"Metric: {result.metric.type}")
    #     for point in result.points:
    #         print(f"  Value: {point.value}")
    #         print(f"  Start Time: {point.interval.start_time}")
    #         print(f"  End Time: {point.interval.end_time}")
    #         print("\n")


if __name__ == "__main__":
    # Replace these values with your own
    project_id = "gitlab-analysis"
    metric_type = "file.googleapis.com/nfs/server/free_bytes_percent"  # Replace with your desired metric type
    filter_str = f'metric.type="{metric_type}"'
    get_storage_metrics(project_id, metric_type, filter_str)
