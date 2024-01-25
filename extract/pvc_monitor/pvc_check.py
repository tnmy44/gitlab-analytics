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


def get_metrics(scoped_credentials, project_id, metric_type, start_time_offset=1200):
    """

    :param scoped_credentials:
    :param project_id:
    :param metric_type:
    :param start_time_offset:
    :return:
    """
    project_name = f"projects/{project_id}"
    client = monitoring_v3.MetricServiceClient(credentials=scoped_credentials)

    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    start_time = {"seconds": (seconds - start_time_offset), "nanos": nanos}
    end_time = {"seconds": seconds, "nanos": nanos}
    interval = monitoring_v3.TimeInterval({"end_time": end_time, "start_time": start_time})

    try:
        results = client.list_time_series(
            name=project_name,
            filter=f'metric.type = "{metric_type}"',
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        )

        latest_metrics = []
        for result in results:
            if result.points:
                latest_value = result.points[-1].value
                latest_metric = {
                    "metric_type": result.metric.type,
                    "resource": result.resource.labels,
                    "value": latest_value,
                }
                latest_metrics.append(latest_metric)

        return latest_metrics

    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def check_pvc_metrics(scoped_credentials):
    """

    :param scoped_credentials:
    """
    project_id = "gitlab-analysis"
    metric_type = "file.googleapis.com/nfs/server/free_bytes_percent"  # Replace with your desired metric type
    filter_str = f'metric.type="{metric_type}"'

    pvc_free_space = get_metrics(scoped_credentials, project_id, metric_type, filter_str)
    for m in pvc_free_space:
        if m.get('value').int64_value <= 20:
            raise ValueError(f"{m.get('resource')} is running low on space")


if __name__ == "__main__":
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(env["GCP_SERVICE_CREDS"], Loader=FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    credentials_with_scope = credentials.with_scopes(scope)

    check_pvc_metrics(credentials_with_scope)
