import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from kubernetes_helpers import get_affinity, get_toleration
from airflow_utils import (
    DATA_IMAGE_3_10,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)

# Define the Kubernetes namespace and PVC name
namespace = 'airflow'
pvc_name = 'airflow-logs-pvc'

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = gitlab_pod_env_vars

# Default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "kubernetes_pvc_monitoring",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    concurrency=1,
    catchup=False,
    start_date= datetime(2023, 11, 15),
)

volume_mount = VolumeMount('airflow-logs-pvc',
                           mount_path='/mnt',
                           sub_path=None,
                           read_only=False)
volume_config = {
    'persistentVolumeClaim':
        {
            'claimName': 'airflow-logs-pvc'
        }
}
volume = Volume(name='data', configs=volume_config)

# Define the task to mount the PVC and execute a command
mount_pvc_task = KubernetesPodOperator(
    task_id='mount_pvc_task',
    name='mount-pvc',
    image='"registry.gitlab.com/gitlab-data/airflow-image:v0.0.2"',  # Replace with your Docker image
    namespace=namespace,
    cmds=['sh', '-c', 'df -h /mnt'],
    volumes=[volume],
    volume_mounts=[volume_mount],
    #volumes=[{'name': 'data', 'persistentVolumeClaim': {'claimName': pvc_name}}],
    #volume_mounts=[{'mountPath': '/mnt', 'name': 'data'}],
    dag=dag,
)
t1 = KubernetesPodOperator(
   task_id='example-task',
   name='example-pod',
   namespace='your-namespace',
   image='your-docker-image',
   cmds=['bash', '-c', 'echo "Volume Size: $(df -h /path/to/volume | awk \'NR==2{print $2}\')"'],
   get_logs=True,
   dag=dag,
)

# # Define the task to sense the PVC status
# sense_pvc_task = KubeAPIVolumeSensor(
#     task_id='sense_pvc_task',
#     namespace=namespace,
#     pvc_name=pvc_name,
#     dag=dag,
# )
#
# # Set task dependencies
# mount_pvc_task >> sense_pvc_task
