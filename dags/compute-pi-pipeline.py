import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': 'bipin-dev'
}

pipeline_cluster_name = 'cluster-2-compute-pi-{{ ds_nodash }}'

with models.DAG(
        'Compute-PI',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/code.html#default-variables
        cluster_name=pipeline_cluster_name,
        num_workers=2,
        region='us-central1',
        autoscaling_policy='projects/{}/regions/us-central1/autoscalingPolicies/ephimeral-scaling-policy'.format(os.environ['PROJECT_ID']),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

    run_py_spark = dataproc_operator.DataProcPySparkOperator(
        task_id='run_py_spark',
        region='us-central1',
        main='gs://{}/data/compute-pi-pipeline/calculate-pi.py'.format(os.environ['COMPOSER_BUCKET']),
        arguments=[ models.Variable.get("NUM_SAMPLES") ],
        cluster_name=pipeline_cluster_name
        )

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        region='us-central1',
        cluster_name=pipeline_cluster_name,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    create_dataproc_cluster >> run_py_spark >> delete_dataproc_cluster
    # [END composer_hadoop_steps]