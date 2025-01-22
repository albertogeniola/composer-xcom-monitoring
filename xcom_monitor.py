#
# Created by Alberto Geniola, albertogeniola@gmail.com
# 
# COMPOSER 2 - XCOM Monitoring DAG.
#
# This DAG is responsible for fetching info from the Metadata DB within the GCP Tenant project
# to populate a custom metric in the Composer's project.
# This metric reports the bytes consumption within the "value" column of the XCOM table within Postgres DB.
# Please note that the XCOM table is not the only table consuming space within the Airflow DB.
# 
# Produced metrics:
# - total_xcom_table_size_bytes: total size of the xcom table contained into the Airflow database
# - xcom_dag_bytes: size of the values for each dag/task run within the xcom table
# - xcom_oldest_entry_age: age in seconds of the oldest entry for a given dag/task in the xcom table
# - xcom_newest_entry_age: age in seconds of the most recent entry for a given dag/task in the xcom table
#
# The DAG is configured to run every 5:00 minutes. You can change this value in accordance with your needs. 
#
# Before executing this DAG, make sure the service account running on the composer environment has appropriate
# IAM permissions to create metrics, write logs and publish time-series. The composer.worker role already
# comes with all the necessary permissions to create metric descriptors and to publish time-series data, and 
# the service account running on the nodes should already have that role granted.
#
import logging
import os
import json
import time
from typing import Optional
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator


l = logging.getLogger()

try:
    from google.cloud import monitoring_v3
    from google.api import metric_pb2 as ga_metric
    from google.api import label_pb2 as ga_label
except ModuleNotFoundError as e:
    l.error("Missing google-cloud-monitoring library. Please install that dependency on the environment before running this DAG.")
    raise

_SCHEDULE_INTERVAL = timedelta(minutes=5)  # Change this if you want the dag to execute on a different schedule.

_TOTAL_XCOM_TABLE_SIZE_METRIC_NAME = "total_xcom_table_size_bytes"
_TOTAL_XCOM_TABLE_SIZE_METRIC_DESC = "Total size of XCOM table in bytes"
_TOTAL_XCOM_TABLE_SIZE_METRIC_TYPE = ga_metric.MetricDescriptor.ValueType.DOUBLE

_DETAIL_XCOM_TASK_SIZE_METRIC_NAME = "xcom_dag_bytes"
_DETAILED_XCOM_TASK_SIZE_METRIC_DESC = "XCOM usage (in bytes) for dag/task"
_DETAILED_XCOM_TASK_SIZE_METRIC_TYPE = ga_metric.MetricDescriptor.ValueType.DOUBLE

_DETAIL_XCOM_TASK_OLDEST_ENTRY_NAME = "xcom_oldest_entry_age"
_DETAIL_XCOM_TASK_OLDEST_ENTRY_DESC = "Age - in seconds - of oldest xcom entry for task/dag"
_DETAIL_XCOM_TASK_OLDEST_ENTRY_TYPE = ga_metric.MetricDescriptor.ValueType.DOUBLE

_DETAIL_XCOM_TASK_NEWEST_ENTRY_NAME = "xcom_newest_entry_age"
_DETAIL_XCOM_TASK_NEWEST_ENTRY_DESC = "Age - in seconds - of the most recent xcom entry for task/dag"
_DETAIL_XCOM_TASK_NEWEST_ENTRY_TYPE = ga_metric.MetricDescriptor.ValueType.DOUBLE

PG_CONN_NAME="airflow_db"
GCP_PROJECT=os.environ.get("GCP_PROJECT")
COMPOSER_ENVIRONMENT=os.environ.get("COMPOSER_ENVIRONMENT")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0,    
}


def _publish_data_point(client: monitoring_v3.MetricServiceClient, metric_name:str, dag_id:Optional[str], task_id:Optional[str], value:float):
    project_name = f"projects/{GCP_PROJECT}"
    series = monitoring_v3.TimeSeries()
    series.metric.type = f"custom.googleapis.com/{metric_name}"
    series.resource.type = "global"
    series.metric.labels["environment"] = COMPOSER_ENVIRONMENT
    if dag_id is not None:
        series.metric.labels["dag_id"]=dag_id
    if task_id is not None:        
        series.metric.labels["task_id"]=task_id
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.TimeInterval(
        {"end_time": {"seconds": seconds, "nanos": nanos}}
    )
    point = monitoring_v3.Point({"interval": interval, "value": {"double_value": value}})
    series.points = [point]
    client.create_time_series(request={"name": project_name, "time_series": [series]})


def _create_gauge_metric(client: monitoring_v3.MetricServiceClient, metric_name:str, metric_type:ga_metric.MetricDescriptor.ValueType, metric_description:str=None, metric_unit:str=None, *args, **kwargs):
    """Creates a gauge metric descriptor if it doesn't exist.

    Args:
        client: The Cloud Monitoring client.
        metric_name (str): The name of the metric.
        metric_type (ga_metric.MetricDescriptor.ValueType): The value type of the metric.
        metric_description (str, optional): A description of the metric. Defaults to None.
        metric_unit (str, optional): The unit of measurement for the metric. Defaults to None.
        
    Returns: The created/retrieved metric descriptor. In case the creation fails, it raises an exception.

    """
    project_name = f"projects/{GCP_PROJECT}"
    descriptor_name = f"{project_name}/metricDescriptors/custom.googleapis.com/{metric_name}"

    # Check if descriptor already exists. If so, return it right away and assume it was already created.
    try:
        descriptor = client.get_metric_descriptor(name=descriptor_name)
        l.info(f"Metric descriptor {descriptor_name} already exists. Skipping creation.")
        return descriptor 
    except Exception as e:  
        l.info(f"Metric descriptor {descriptor_name} not found. Creating it.")

    # If not found, proceed with creation.
    descriptor = ga_metric.MetricDescriptor()
    descriptor.type = f"custom.googleapis.com/{metric_name}"
    descriptor.metric_kind = ga_metric.MetricDescriptor.MetricKind.GAUGE
    descriptor.value_type = metric_type
    descriptor.description = metric_description

    e_id_label = ga_label.LabelDescriptor()
    e_id_label.key = "environment"
    e_id_label.value_type = ga_label.LabelDescriptor.ValueType.STRING
    e_id_label.description = "COMPOSER ENVIRONMENT"
    descriptor.labels.append(e_id_label)

    d_id_label = ga_label.LabelDescriptor()
    d_id_label.key = "dag_id"
    d_id_label.value_type = ga_label.LabelDescriptor.ValueType.STRING
    d_id_label.description = "DAG ID"
    descriptor.labels.append(d_id_label)

    t_id_label = ga_label.LabelDescriptor()
    t_id_label.key = "task_id"
    t_id_label.value_type = ga_label.LabelDescriptor.ValueType.STRING
    t_id_label.description = "TASK ID"
    descriptor.labels.append(t_id_label)

    if metric_unit is not None:
        descriptor.unit = metric_unit

    try:  
        descriptor = client.create_metric_descriptor(
            name=project_name, metric_descriptor=descriptor
        )
        l.info(f"Successfully created metric descriptor: {descriptor.name}")
        return descriptor
    except Exception as e: 
        l.error(f"Failed to create metric descriptor {metric_name}: {e}")
        raise


def collect_total_xcom(ds=None, **kwargs):
    """Collect and log xcom table total size"""
    l.info("Retrieving total xcom table size.")
    postgres = PostgresHook(postgres_conn_id=PG_CONN_NAME)
    xcom_size_bytes = -1

    # Fetch total table size for XCOM table
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_total_relation_size('xcom') as xcom_size")            
            xcom_size_bytes = cursor.fetchone()[0]
            l.info("Total xcom table size: %d bytes", xcom_size_bytes)
    
    # Publish the metric
    client = monitoring_v3.MetricServiceClient()
    _publish_data_point(client, _TOTAL_XCOM_TABLE_SIZE_METRIC_NAME,None,None,xcom_size_bytes)


def create_metrics(ds=None, **kwargs):
    """Check the existence of the custom metrics and if necessary create them."""
    client = monitoring_v3.MetricServiceClient()
    l.info(f"Checking metrics existence. Creating if not existing.")
    if _create_gauge_metric(client, _TOTAL_XCOM_TABLE_SIZE_METRIC_NAME, _TOTAL_XCOM_TABLE_SIZE_METRIC_TYPE, _TOTAL_XCOM_TABLE_SIZE_METRIC_DESC, metric_unit="By"):
        l.debug("Metric %s handled OK.", _TOTAL_XCOM_TABLE_SIZE_METRIC_NAME)
    if _create_gauge_metric(client, _DETAIL_XCOM_TASK_SIZE_METRIC_NAME, _DETAILED_XCOM_TASK_SIZE_METRIC_TYPE, _DETAILED_XCOM_TASK_SIZE_METRIC_DESC, metric_unit="By"):
        l.debug("Metric %s handled OK.", _DETAIL_XCOM_TASK_SIZE_METRIC_NAME)
    if _create_gauge_metric(client, _DETAIL_XCOM_TASK_OLDEST_ENTRY_NAME, _DETAIL_XCOM_TASK_OLDEST_ENTRY_TYPE, _DETAIL_XCOM_TASK_OLDEST_ENTRY_DESC, metric_unit="s"):
        l.debug("Metric %s handled OK.", _DETAIL_XCOM_TASK_OLDEST_ENTRY_NAME)
    if _create_gauge_metric(client, _DETAIL_XCOM_TASK_NEWEST_ENTRY_NAME, _DETAIL_XCOM_TASK_NEWEST_ENTRY_TYPE, _DETAIL_XCOM_TASK_NEWEST_ENTRY_DESC, metric_unit="s"):
        l.debug("Metric %s handled OK.", _DETAIL_XCOM_TASK_NEWEST_ENTRY_NAME)
    l.info("All metrics have been checked/created.")


def collect_detailed_xcom(ds=None, **kwargs):
    """Collect and log detailed xcom metrics at dag/task level"""
    l.info("Retrieving detailed xcom sizeing at dag/task level.")
    postgres = PostgresHook(postgres_conn_id=PG_CONN_NAME)
    detailed_data = []

    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            # Fetch detailed info for each dag/task
            cursor.execute("""
                SELECT 
                    dag_id, 
                    task_id, 
                    sum(pg_column_size(value)) as total_size,
                    avg(pg_column_size(value)) as average_size,
                    min(timestamp) as oldest_timestamp,
                    max(timestamp) as newest_timestamp
                FROM xcom
                GROUP by dag_id, task_id;
                """)

            detailed_data = [{"dag_id": str(row[0]), "task_id": str(row[1]), "total_size": float(row[2]), "average_size": float(row[3]), "oldest_timestamp": float(row[4].timestamp()), "newest_timestamp": float(row[5].timestamp())} for row in cursor]
            l.info("Detailed xcom usage: str(%s)", json.dumps(detailed_data))
        
    client = monitoring_v3.MetricServiceClient()
    # Allocate metrics for detailed data
    for dag_task in detailed_data:
        dag_id = dag_task["dag_id"]
        task_id = dag_task["task_id"]
        # Create metrics
        size_metric_name = _DETAIL_XCOM_TASK_SIZE_METRIC_NAME
        oldest_entry_metric_name = _DETAIL_XCOM_TASK_OLDEST_ENTRY_NAME
        newest_entry_metric_name = _DETAIL_XCOM_TASK_NEWEST_ENTRY_NAME
        
        # Publish data points
        l.debug(f"Pushing data for {size_metric_name}")
        total_size = dag_task["total_size"]
        oldest_timestamp = dag_task["oldest_timestamp"]
        newest_timestamp = dag_task["newest_timestamp"]
        _publish_data_point(client, size_metric_name,dag_id,task_id,total_size)
        age_oldest = time.time() - oldest_timestamp
        l.info("Oldest timestamp for task \"%s\" of dag \"%s\": %s. Age: %d", dag_id, task_id, str(oldest_timestamp), age_oldest)
        _publish_data_point(client, oldest_entry_metric_name,dag_id,task_id,age_oldest)
        age_newest = time.time() - newest_timestamp
        l.info("Newest timestamp for task \"%s\" of dag \"%s\": %s. Age: %d", dag_id, task_id, str(newest_timestamp), age_newest)
        _publish_data_point(client, newest_entry_metric_name,dag_id,task_id,age_newest)


with DAG(
    'xcom_monitor',
    default_args=default_args,
    description='Performs XCOM checks and publishes metrics to GCP Cloud Monitoring',
    schedule_interval=_SCHEDULE_INTERVAL,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    create_metrics_task = PythonOperator(
        task_id="create_metrics", python_callable=create_metrics
    )
    publish_total_xcom = PythonOperator(
        task_id="publish_total_xcom", python_callable=collect_total_xcom
    )
    publish_detailed_xcom = PythonOperator(
        task_id="publish_detailed_xcom", python_callable=collect_detailed_xcom
    )
    end = EmptyOperator(task_id="end")
    create_metrics_task >> publish_total_xcom >> end
    create_metrics_task >> publish_detailed_xcom >> end
