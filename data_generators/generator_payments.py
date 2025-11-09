import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def payments():
    """Функция генерирует данные о платежах клиентов"""

    num_clients = 1000
    max_payments_per_client = 50

    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Генерация данных для платежей
    payment_data = []
    for client_id in range(1, num_clients + 1):
        num_payments = random.randint(1, max_payments_per_client)
        for _ in range(num_payments):
            payment_date = random_date(start_date, end_date)
            currency = np.random.choice(['USD', 'RUB'])
            amount = round(np.random.uniform(100, 10000), 2) if currency == 'USD' else round(np.random.uniform(9000, 900000), 2)
            payment_data.append({
                'client_id': client_id,
                'payment_id': np.random.randint(1000, 10000),
                'payment_date': payment_date,
                'currency': currency,
                'amount': amount,
                'payment_method': np.random.choice(['credit_card', 'debit_card', 'bank_transfer', 'e_wallet'])
            })

    payment_df = pd.DataFrame(payment_data)

    # Добавление пропусков
    for column in ['client_id', 'payment_id', 'payment_date', 'currency', 'amount', 'payment_method']:
        mask = np.random.rand(len(payment_df)) < 0.05
        payment_df.loc[mask, column] = np.nan

    # Добавление аномальных дат
    anomalous_years = [1700, 1800, 2100, 2200]
    if len(payment_df) > 0:
        mask = np.random.rand(len(payment_df)) < 0.05
        payment_df.loc[mask, 'payment_date'] = [
            pd.to_datetime(
                datetime(year=np.random.choice(anomalous_years), month=random.randint(1, 12), day=random.randint(1, 28)))
            for _ in range(mask.sum())
        ]

    # Добавляем дубликаты вычисляем количество строк для дублирования (5% от общего количества строк)
    num_rows_to_duplicate = max(1, int(len(payment_df) * 0.05))

    # Случайным образом выбираем строки для дублирования
    duplicated_rows = payment_df.sample(n=num_rows_to_duplicate, replace=True)

    # Добавляем дублируемые строки обратно в DataFrame
    payment_df = pd.concat([payment_df, duplicated_rows], ignore_index=True)

    # Перемешиваем строки, чтобы дубликаты не шли подряд
    payment_df = payment_df.sample(frac=1).reset_index(drop=True)

    return payment_df
