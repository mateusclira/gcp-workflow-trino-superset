from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models.xcom import XCom
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance


from airflow.utils.dates import days_ago

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, substring

# set conf
conf = (
SparkConf()
    .set("google.cloud.auth.service.account.enable", "true")
    .set("google.cloud.auth.service.account.email", "marina.marinalira@gmail.com")
    .set("spark.hadoop.fs.gs.project.id", "igti-edc-mod4")
    .set("spark.hadoop.fs.gs.auth.service.account.private.key", "<-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDlol3mNO0MNEh7\nQc7jwvW2Qfv1QdxTYHf/t8l5OjJYox0KeSxyTT8oRwzrJ436tCUD1fTSRUB0yRkn\nAukb8uvJuJSypNwkh+HZ0snILi8zUbC90dNa2oGdh3rAjGVyVZbAtT1LmU5EYXy6\n6h6GuwjlLm3zXIYxjd9fhc3NU9ZbPdpF3IrI5H4YNIuV0GKPYefWqf17Pw5Sr/eJ\nFo+puYJfD8SoIynWSxpCUWcmIutoYRM0KKo78aqTcS4qgM8/2GlqcDtEm+Qlj3D1\n2LjVfQ02o9FXyK7Tr655HycOi55jlJ3uTd7E/zOeH8ueYn9jRtqxM5ImRPBhQ+2f\n3nk5Ce03AgMBAAECggEADcPYXhrRFNizeZP9y2Bd60F0UYTTqLnRJ6aEMLyg1Phe\nHskZtXZU8Vyk9RmnZJ5U07CJHuugy/9b/x8pCxBaOvrSCw1f1t7AVpbQmqvOD4T3\nk8FWqo0LlP5QiOdBk4N26HFUzcnQS8AOQoQTNV9Tzq0kUSC8OI85ExhJuGOsp0Zm\nU/m2+DpkxGaN7ysxLneLIul88rZgc9LdYWGwk0obsMbNDeritgJNWShXt1bC1/nQ\nMpjntNdLR5BIz/G31gIRQfJ93TH+hcnGtpmrFR2u2t9JQgQ8rwgxWThK/COBC80t\nQR9n3x5dWaZ+PMM1V8VJer+P3iwdfAEAh6bmNhC6PQKBgQD0hsECpN4mxTqZmdl+\n0K2DlxeWUJkFh7I2OsmlLZIVPxw72rv2S9tgOK9eCGu6RfemHW/N0eUG5siQllxM\nPsUNKhBs81mcm00iqOcP2RPwbYjfEW/V21rZf/HwHd+3vV1PhdtMNoHK3bA8D31A\nFoCwX2puDD/ufnWPqXJRKEgEwwKBgQDwaLqR86YXzUUO99F2UqWPUz5ZeAsPyPuo\nEi5r763lKnnZoAUOA0BLyAg45xONGWBTB1YmKjAUxvsnpeXnFScWhi6eaLo70xgD\niC/AEYG2zIy/jozNrdLNpeMC2GX3kn05eHe4i81/sFFU5KfrmNh119wQV2fq16nw\nDVUhMWBefQKBgQDooqNR7qRjYlSj+ZjQa5wIi2CnYzqR5k2IBln9SeOnTwl/cFRO\nuFCXtJYy8fP7ncFEStEpkT1BWaMK2+Inr7gtFA3YeZSRx7aU+PgLPc0Yej0zbBVn\n8m72aiE0/lbvKbtygrjmQx0sccjO+zWAGWiJY92PoRvZvHoAGYdm6tnJnQKBgHyo\n4a2S2NpYv2l+iWfAfUbvlFISVDIQNqY1H0sq0lIVj2+QcW6NswZYqCLRd6rfsLn9\nAOfzUlFDyLILbUZBCq68T2omxgMFd9tE//WpXkiqkoU15r80Vi36ydcM3PreQ/7L\n/9B69ckzZUkO9B/PdtbMYrk6iZEV2XG3iCAZGW6FAoGAOX1eFe7Aig0hLHMF4pp6\nKPLmWZw8rGv9XRI/xfkpzIATH7NkHe3ry1l8b8mQLssRXtOSGiTV7BgfYg8CM3o/\nMfWYaDg85ktI/gptMUO86S06EBnCyELJbDxsfpnfABXeYo+BM/SPhfld+IlZUmjw\n8yPYIZhggqU6s2o4+kiSvDc=\n-----END PRIVATE KEY-----\n>")
    .set("spark.hadoop.fs.gs.auth.service.account.private.key.id", "6a066667ff752dfad16e253eaa6dfa2f0cd3950a")
)

with DAG(
    'enem_batch_spark_k8s',
    default_args={
        'owner': 'Mateus',
        'depends_on_past': False,
        'email': ['mateusc.lira@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='submit spark-pi as sparkApplication on kubernetes',
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'enem'],
) as dag:
    converte_parquet = SparkKubernetesOperator(
        task_id='converte_parquet',
        namespace="airflow",
        application_file="enem_converte_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_parquet_monitor',
        namespace="airflow",
        application_name = "job-pyspark",
        kubernetes_conn_id="kubernetes_default",
    )

    anonimiza_inscricao = SparkKubernetesOperator(
        task_id='anonimiza_inscricao',
        namespace="airflow",
        application_file="enem_anonimiza_inscricao.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    anonimiza_inscricao_monitor = SparkKubernetesSensor(
        task_id='anonimiza_inscricao_monitor',
        namespace="airflow",
        application_name="spark-enem-anon",
        kubernetes_conn_id="kubernetes_default",
    )


    agrega_idade = SparkKubernetesOperator(
        task_id='agrega_idade',
        namespace="airflow",
        application_file="enem_agrega_idade.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agrega_idade_monitor = SparkKubernetesSensor(
        task_id='agrega_idade_monitor',
        namespace="airflow",
        application_name="spark-enem-idade",
        kubernetes_conn_id="kubernetes_default",
    )

    agrega_sexo = SparkKubernetesOperator(
        task_id='agrega_sexo',
        namespace="airflow",
        application_file="enem_agrega_sexo.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agrega_sexo_monitor = SparkKubernetesSensor(
        task_id='agrega_sexo_monitor',
        namespace="airflow",
        application_name="spark-enem-sexo",
        kubernetes_conn_id="kubernetes_default",
    )

    agrega_notas = SparkKubernetesOperator(
        task_id='agrega_notas',
        namespace="airflow",
        application_file="enem_agrega_notas.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agrega_notas_monitor = SparkKubernetesSensor(
        task_id='agrega_notas_monitor',
        namespace="airflow",
        application_name="spark-enem-notas",
        kubernetes_conn_id="kubernetes_default",
    )

    join_final = SparkKubernetesOperator(
        task_id='join_final',
        namespace="airflow",
        application_file="enem_join_final.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    join_final_monitor = SparkKubernetesSensor(
        task_id='join_final_monitor',
        namespace="airflow",
        application_name="spark-enem-final",
        kubernetes_conn_id="kubernetes_default",
    )


converte_parquet >> converte_parquet_monitor >> anonimiza_inscricao >> anonimiza_inscricao_monitor
converte_parquet_monitor >> agrega_idade >> agrega_idade_monitor
converte_parquet_monitor >> agrega_sexo >> agrega_sexo_monitor
converte_parquet_monitor >> agrega_notas >> agrega_notas_monitor
[agrega_idade_monitor, agrega_sexo_monitor, agrega_notas_monitor] >> join_final >> join_final_monitor
join_final_monitor
[agrega_idade_monitor, agrega_notas_monitor] >> agrega_sexo
[agrega_idade_monitor, agrega_notas_monitor] >> anonimiza_inscricao