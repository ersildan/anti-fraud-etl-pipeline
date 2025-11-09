import time
import requests
import subprocess

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from kafka import KafkaConsumer

KAFKA_BROKER = '172.17.0.13:9092'
PATH_TO_KAFKA = '/opt/airflow/dags/27_svistunov/data_generators/producer_to_kafka_svistunov.py'
FROM_KAFKA_TO_HDFS = '/opt/airflow/dags/27_svistunov/data_consumers/consumer_from_kafka.py'
SQL_SCRIPTS_PATH = '/opt/airflow/dags/27_svistunov/data_greenplum/'


def send_telegram_alert(message, **context):
    """Функция для отправки уведомлений в тг"""

    ti = context['task_instance']
    hook = BaseHook.get_connection('svistunov_telegram')
    bot_token = hook.password
    chat_id = hook.extra_dejson.get('chat_id')

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        ti.log.info(f"Уведомление отправлен в Telegram")

    except Exception as e:
        ti.log.error(f"Ошибка отправки уведомления: {e}")
        raise


def pipeline_success_alert(**context):
    """Алерт об успешном завершении всего пайплайна"""

    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    message = (f"ETL ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН\n"
               f"DAG: {dag_id}\n"
               f"Время: {execution_date}\n"
               f"Статус: Все этапы выполнены\n"
               f"Генерация данных → Kafka\n"
               f"Обработка Spark → HDFS\n"
               f"Загрузка в Greenplum\n"
               f"Создание ODS/DDS таблиц\n"
               f"Создание DM")

    send_telegram_alert(message, **context)


def check_kafka(**context):
    """Проверка соединения с кафкой перед загрузкой"""

    ti = context['task_instance']
    for attempt in range(3):

        try:
            consumer = KafkaConsumer(bootstrap_servers=[KAFKA_BROKER])
            topics = consumer.topics()
            consumer.close()
            ti.log.info(f"Kafka available. Topics: {len(topics)}")
            return

        except Exception as e:
            ti.log.warning(f"Kafka attempt {attempt + 1}/3 failed: {e}")

            if attempt < 2:
                time.sleep(30)
    raise Exception("Kafka unavailable after 3 attempts")


def check_greenplum(**context):
    """Проверка подключения к Greenplum"""

    ti = context['task_instance']

    for attempt in range(3):
        try:
            hook = PostgresHook(postgres_conn_id="svistunov_gp")
            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.execute("SELECT 1 as test_connection;")
            result = cursor.fetchone()

            cursor.close()
            conn.close()

            ti.log.info(f"Greenplum успешное подключение: {result[0] == 1}")
            return

        except Exception as e:
            ti.log.warning(f"Greenplum ошибка соединения: {attempt + 1}/3 failed: {e}")

            if attempt < 2:
                time.sleep(30)

    raise Exception("Не удалось подключиться к Greenplum после 3 попыток")


def send_to_kafka(**context):
    """Запуск скрипта генерации и загрузки в кафку"""

    task_instance = context['task_instance']
    task_instance.log.info("Запускаем генерацию данных и отправку в Kafka")

    try:
        result = subprocess.run(
            ['python3', PATH_TO_KAFKA],
            capture_output=True,
            text=True,
            check=True
        )
        task_instance.log.info("Генерация завершена успешно")
        task_instance.log.info(f"STDOUT: {result.stdout}")

    except subprocess.CalledProcessError as e:
        task_instance.log.error(f"Ошибка генерации: {e}")
        task_instance.log.error(f"STDERR: {e.stderr}")
        raise


def _execute_sql_script(sql_file, **context):
    """Чтение и запуск SQL скриптов"""

    hook = PostgresHook(postgres_conn_id="svistunov_gp")
    task_instance = context['task_instance']

    task_instance.log.info(f"Executing SQL script: {sql_file}")

    with open(f'{SQL_SCRIPTS_PATH}{sql_file}', 'r') as f:
        sql_commands = f.read()

    hook.run(sql_commands, autocommit=True)
    task_instance.log.info(f"SQL script {sql_file} completed successfully")


default_args = {
    'owner': 'a.svistunov',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='27_svistunov_dag',
        tags=['27', 'svistunov'],
        default_args=default_args,
        description='Generate -> Kafka -> Spark -> HDFS -> Greenplum',
        schedule_interval='00 06 * * *',
        start_date=datetime(2025, 10, 22),
        catchup=False,
) as dag:

    check_kafka = PythonOperator(
        task_id='check_kafka_availability',
        python_callable=check_kafka,
    )

    check_ssh = SSHOperator(
        task_id='check_ssh_connection',
        ssh_conn_id='svistunov_ssh',
        command='echo "SSH connection successful"'
    )

    check_greenplum = PythonOperator(
        task_id='check_greenplum_connection',
        python_callable=check_greenplum,
    )
####################################################################################################
    send_data_to_kafka = PythonOperator(
        task_id='send_data_to_kafka',
        python_callable=send_to_kafka,
    )

    from_kafka_to_hdfs = SSHOperator(
        task_id='from_kafka_to_hdfs',
        ssh_conn_id='svistunov_ssh',
        cmd_timeout=600,
        get_pty=True,
        command=(
            f'spark-submit '
            f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 '
            f'{FROM_KAFKA_TO_HDFS}'
        )
    )
####################################################################################################
    create_external_tables = PythonOperator(
        task_id='create_external_tables',
        python_callable=_execute_sql_script,
        op_kwargs={'sql_file': 'create_external_tables.sql'}
    )

    create_ods_tables = PythonOperator(
        task_id='create_ods_tables',
        python_callable=_execute_sql_script,
        op_kwargs={'sql_file': 'create_tables_ods.sql'}
    )

    create_dds_tables = PythonOperator(
        task_id='create_dds_tables',
        python_callable=_execute_sql_script,
        op_kwargs={'sql_file': 'create_tables_dds.sql'}
    )
####################################################################################################
    load_ods_data = PythonOperator(
        task_id='load_ods_data',
        python_callable=_execute_sql_script,
        op_kwargs={'sql_file': 'data_load_to_ods.sql'}
    )

    load_dds_data = PythonOperator(
        task_id='load_dds_data',
        python_callable=_execute_sql_script,
        op_kwargs={'sql_file': 'data_load_to_dds.sql'}
    )

    create_and_load_to_dm = PythonOperator(
        task_id='create_dm',
        python_callable=_execute_sql_script,
        op_kwargs={'sql_file': 'create_and_load_to_dm.sql'}
    )
####################################################################################################
    # Финальное пуш-уведомление в тг
    pipeline_success = PythonOperator(
        task_id='pipeline_success_alert',
        python_callable=pipeline_success_alert,
    )
####################################################################################################
    [check_kafka, check_ssh, check_greenplum] >> send_data_to_kafka >> from_kafka_to_hdfs

    from_kafka_to_hdfs >> [create_external_tables, create_ods_tables, create_dds_tables]

    [create_external_tables, create_ods_tables, create_dds_tables] >> load_ods_data >> load_dds_data
    load_dds_data >> create_and_load_to_dm >> pipeline_success
