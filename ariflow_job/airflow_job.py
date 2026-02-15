from datetime import datetime, timedelta
import uuid  # Import UUID for unique batch IDs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 15),
}

# Define the DAG
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
) as dag:

    # Fetch environment variables
    env_dev = Variable.get("env_dev", default_var="dev")
    env_prod = Variable.get("env_prod", default_var="prod")
    gcs_bucket_dev = Variable.get("gcs_bucket", default_var="us-central1-airflow-dev-535fa553-bucket")
    gcs_bucket_prod = Variable.get("gcs_bucket_prod", default_var="us-central1-airflow-prod-2746756d-bucket")
    bq_project = Variable.get("bq_project", default_var="project-b33ba036-13df-409f-b4f")
    bq_dataset_dev = Variable.get("bq_dataset", default_var=f"flight_data_{env_dev}")
    bq_dataset_prod = Variable.get("bq_dataset_prod", default_var=f"flight_data_{env_prod}")
    tables = Variable.get("tables", deserialize_json=True)

    # Extract table names from the 'tables' variable
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    # Generate a unique batch ID using UUID
    batch_id1 = f"flight-booking-batch-{env_dev}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity
    batch_id2 = f"flight-booking-batch-{env_prod}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

    # # Task 1: File Sensor for GCS
    file_sensor_dev = GCSObjectExistenceSensor(
        task_id="check_file_arrival_dev",
        bucket=gcs_bucket_dev,
        object=f"flight-booking-analysis/source-{env_dev}/flight_booking.csv",  # Full file path in GCS
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=300,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        mode="poke",  # Blocking mode
    )


    file_sensor_prod = GCSObjectExistenceSensor(
        task_id="check_file_arrival_prod",
        bucket=gcs_bucket_prod,
        object=f"flight-booking-analysis/source-{env_prod}/flight_booking.csv",  # Full file path in GCS
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=300,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        mode="poke",  # Blocking mode
    )

    # Task 2: Submit PySpark job to Dataproc Serverless
    batch_details_dev = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket_dev}/flight-booking-analysis/spark-job/spark_transformation_job.py",  # Main Python file
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": [],  # JAR files
            "args": [
                f"--env={env_dev}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset_dev}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed)
        },
        "environment_config": {
            "execution_config": {
                "service_account": "880074405527-compute@developer.gserviceaccount.com",
                "network_uri": "projects/project-b33ba036-13df-409f-b4f/global/networks/default",
                "subnetwork_uri": "projects/project-b33ba036-13df-409f-b4f/regions/us-central1/subnetworks/default",
            }
        },
    }

    batch_details_prod = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket_prod}/flight-booking-analysis/spark-job/spark_transformation_job.py",  # Main Python file
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": [],  # JAR files
            "args": [
                f"--env={env_prod}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset_prod}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed)
        },
        "environment_config": {
            "execution_config": {
                "service_account": "880074405527-compute@developer.gserviceaccount.com",
                "network_uri": "projects/project-b33ba036-13df-409f-b4f/global/networks/default",
                "subnetwork_uri": "projects/project-b33ba036-13df-409f-b4f/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_task_dev = DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless_dev",
        batch=batch_details_dev,
        batch_id=batch_id1,
        project_id="project-b33ba036-13df-409f-b4f",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    pyspark_task_prod = DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless_prod",
        batch=batch_details_prod,
        batch_id=batch_id2,
        project_id="project-b33ba036-13df-409f-b4f",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    # Task Dependencies
    file_sensor_dev >> pyspark_task_dev
    file_sensor_prod >> pyspark_task_prod