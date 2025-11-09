import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta


def transactions():
    """Функция генерирует данные о банковских операциях"""

    fake = Faker()
    num_clients = 1000
    max_transactions_per_client = 150

    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Генерация данных для банковских транзакций
    transaction_data = []
    for client_id in range(1, num_clients + 1):
        num_transactions = random.randint(1, max_transactions_per_client)
        for _ in range(num_transactions):
            transaction_date = random_date(start_date, end_date)
            currency = np.random.choice(['USD', 'RUB'])
            amount = round(np.random.uniform(100, 10000), 2) if currency == 'USD' else round(
                np.random.uniform(9000, 900000), 2)

            # Добавление аномально больших сумм
            if random.random() < 0.01:
                amount *= 10

            transaction_data.append({
                'client_id': client_id,
                'transaction_id': np.random.randint(1000, 10000),
                'transaction_date': transaction_date,
                'transaction_type': np.random.choice(['deposit', 'withdrawal', 'transfer']),
                'account_number': fake.iban(),
                'currency': currency,
                'amount': amount
            })

    # Добавление аномально большого кол-ва транзакций в один день для 4 клиентов
    anomalous_clients = random.sample(range(1, num_clients + 1), 4)
    for client_id in anomalous_clients:
        anomalous_dates = [random_date(start_date, end_date) for _ in range(2)]
        for date in anomalous_dates:
            for _ in range(10):
                transaction_data.append({
                    'client_id': client_id,
                    'transaction_id': np.random.randint(1000, 10000),
                    'transaction_date': date,
                    'transaction_type': np.random.choice(['deposit', 'withdrawal', 'transfer']),
                    'account_number': fake.iban(),
                    'currency': np.random.choice(['USD', 'RUB']),
                    'amount': round(np.random.uniform(100, 10000), 2),
                    'record_saved_at': datetime.now()
                })

    transaction_df = pd.DataFrame(transaction_data)

    # Добавление пропусков
    for column in ['client_id', 'transaction_id', 'transaction_date', 'transaction_type', 'account_number', 'currency',
                   'amount']:
        mask = np.random.rand(len(transaction_df)) < 0.04
        transaction_df.loc[mask, column] = np.nan

    # Добавление аномальных дат
    anomalous_years = [1700, 1800, 2100, 2200]
    if len(transaction_df) > 0:
        mask = np.random.rand(len(transaction_df)) < 0.05
        transaction_df.loc[mask, 'transaction_date'] = [
            pd.to_datetime(
                datetime(year=np.random.choice(anomalous_years), month=random.randint(1, 12),
                         day=random.randint(1, 28)))
            for _ in range(mask.sum())
        ]

    # Добавляем дубликаты вычисляем количество строк для дублирования (5% от общего количества строк)
    num_rows_to_duplicate = max(1, int(len(transaction_df) * 0.05))

    # Случайным образом выбираем строки для дублирования
    duplicated_rows = transaction_df.sample(n=num_rows_to_duplicate, replace=True)

    # Добавляем дублируемые строки обратно в DataFrame
    transaction_df = pd.concat([transaction_df, duplicated_rows], ignore_index=True)

    # Перемешиваем строки, чтобы дубликаты не шли подряд
    transaction_df = transaction_df.sample(frac=1).reset_index(drop=True)

    return transaction_df
