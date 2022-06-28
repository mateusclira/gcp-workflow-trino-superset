from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models.xcom import XCom
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance


from airflow.utils.dates import days_ago




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