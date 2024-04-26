from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from cotizaciones_api import get_stocks, create_tables, insert_data, verify_threshold

default_args={
    'owner': 'Bautista Argañaraz Londero',
    'depends_on_past': True,
    'email': ['b.arganaraz.londero@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}

dag_principal = DAG(
    default_args = default_args,
    dag_id = 'cotizaciones_7_magnificos_con_alerta',
    description = 'Obtiene las cotizaciones de los 7 magníficos de NASDAQ y los carga en la tabla en AWS Redshift con una frecuencia diaria. Se envía una alerta según el valor con el que cerro una determinada cotización.',
    start_date = datetime(2024,4,24,2),
    schedule_interval = '@daily',
    tags = ['Proyecto_Final', 'Bautista_Arganaraz_Londero', 'CoderHouse'],
    catchup = False
)

def obtencion_cotizaciones(ti):
    df_json = get_stocks()
    ti.xcom_push(key = 'cotizaciones', value = df_json)

def insercion_datos(ti):
    df_json = ti.xcom_pull(key = 'cotizaciones', task_ids = 'obtencion_cotizaciones_api')
    insert_data(df_json)

def verificacion_limites(ti):
    df_json = ti.xcom_pull(key = 'cotizaciones', task_ids = 'obtencion_cotizaciones_api')
    verify_threshold(df_json)

tarea1 = PythonOperator(
    task_id = 'obtencion_cotizaciones_api',
    python_callable = obtencion_cotizaciones,
    provide_context = True,
    dag = dag_principal,
)

tarea2 = PythonOperator(
    task_id = 'creacion_tablas_redshift',
    python_callable = create_tables,
    dag = dag_principal,
)

tarea3 = PythonOperator(
    task_id = 'insercion_datos_incremental',
    python_callable = insercion_datos,
    provide_context = True,
    dag = dag_principal,
)

tarea4 = PythonOperator(
    task_id = 'verificacion_limites_alerta',
    python_callable = verificacion_limites,
    provide_context = True,
    dag = dag_principal,
)

tarea1 >> tarea2 >> tarea3
tarea1 >> tarea4