from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import datetime
from datetime import timedelta
from airflow.operators.dummy import DummyOperator
import uuid
default_args = {
   'start_date': days_ago(1),
   'schedule_interval': '@daily',
   'dagrun_timeout': timedelta(minutes=10),
   'catchup': False
}
PREFIX = 'productorder'
common_args = [
   "PROJECT_ID=original-gasket-405913",
   "TEMP_BQ_BUCKET=gs://temp-bigquery1",
   "RAW_CSV_PATH=gs://product_order_use_case",
   "DELTA_FOLDER_PATH=gs://product_order_scala",
   "RAW_FOLDER_PATH=gs://product_order_use_case",
   "LAYER=gold"
]
with DAG(
   'dataproc_batch_operators',
   default_args=default_args,
   schedule_interval=datetime.timedelta(days=1),
) as dag:
   start = DummyOperator(task_id="start")

   def get_uuid(**kwargs):
       constant_uuid = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
       kwargs['ti'].xcom_push(key='constant_uuid', value=constant_uuid)  
       return constant_uuid

   def get_batch(layer):
       formatted_layer = layer.lower().replace('_', '-')
       constant_uuid = "{{task_instance.xcom_pull(key='constant_uuid', task_ids='get_uuid')}}" 
       args = common_args
       return DataprocCreateBatchOperator(
           project_id='original-gasket-405913',
           region='us-central1',
           task_id=layer,
           batch={
               'spark_batch': {
                   'jar_file_uris': [
                       'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.31.1.jar',
                       'gs://product_order_scala/jars/product-order.jar'
                   ],
                   'main_class': layer,
                   'args': args,
               },
               'environment_config': {
                   'execution_config': {
                       'service_account': '698522255489-compute@developer.gserviceaccount.com'
                   }
               },
               'runtime_config': {
                   'properties': {
                       'spark.jars.packages': 'io.delta:delta-core_2.13:2.3.0'
                   }
               }
           },
           batch_id=f"{PREFIX}-{constant_uuid}-{formatted_layer}",
       )
   end = DummyOperator(task_id="end", trigger_rule='all_done')

   uuid_task = PythonOperator(
       task_id='get_uuid',
       python_callable=get_uuid,
       provide_context=True
   )
   setup = get_batch('setup')
   bronze = get_batch('bronze')
   silver = get_batch('silver')
   gold = get_batch('gold')
   delta_to_bq = get_batch('deltaToBigQuery')
   start >> uuid_task >> setup >> bronze >> silver >> gold >> delta_to_bq >> end