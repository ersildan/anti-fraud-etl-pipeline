import json
import logging
import pandas as pd
from kafka import KafkaProducer
from generator_activity import activity
from generator_clients import clients
from generator_logins import logins
from generator_payments import payments
from generator_transactions import transactions


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    acks=1,
    retries=3,
    bootstrap_servers=['172.17.0.13:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    key_serializer=lambda x: str(x).encode('utf-8'),
    compression_type='gzip'
)

def send_to_kafka(dataframe, topic):
    logger.info(f"Начало отправки в топик: {topic}")

    record_count = 0
    for record in dataframe.to_dict('records'):
        processed = {k: None if pd.isna(v) else str(v) for k, v in record.items()}

        try:
            producer.send(
                topic,
                key=processed['client_id'],
                value=processed,
            )
            record_count += 1
        except Exception as e:
            logger.error(f'Ошибка в {topic}: {e}')

    producer.flush()
    logger.info(f"Отправлено {record_count} записей в топик {topic}")

if __name__ == '__main__':
    logger.info('Начинаем загрузку данных в кафку')

    send_to_kafka(activity(), "raw_activity_svistunov_wave27")
    send_to_kafka(clients(), "raw_clients_svistunov_wave27")
    send_to_kafka(logins(), "raw_logins_svistunov_wave27")
    send_to_kafka(payments(), "raw_payments_svistunov_wave27")
    send_to_kafka(transactions(), "raw_transactions_svistunov_wave27")

    logger.info("Все данные успешно отправлены в Kafka")
