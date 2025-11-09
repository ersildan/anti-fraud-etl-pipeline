import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta


def activity():
    """Функция генерирует данные об активности клиентов"""

    fake = Faker()
    num_clients = 1000
    max_activities_per_client = 100

    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    activity_data = []

    for client_id in range(1, num_clients + 1):
        num_activities = random.randint(1, max_activities_per_client)

        for _ in range(num_activities):
            activity_date = random_date(start_date, end_date)
            activity_data.append({
                'client_id': client_id,
                'activity_date': activity_date,
                'activity_type': np.random.choice(['view_account', 'transfer_funds', 'pay_bill', 'login', 'logout']),
                'activity_location': fake.uri_path(),
                'ip_address': fake.ipv4(),
                'device': fake.user_agent()
            })

    activity_df = pd.DataFrame(activity_data)

    # Добавление пропусков
    for column in ['client_id', 'activity_date', 'activity_type', 'activity_location', 'ip_address', 'device']:
        mask = np.random.rand(len(activity_df)) < 0.05
        activity_df.loc[mask, column] = np.nan

    # Добавление аномальных дат
    anomalous_years = [1700, 1800, 2100, 2200]
    if len(activity_df) > 0:
        mask = np.random.rand(len(activity_df)) < 0.05
        activity_df.loc[mask, 'activity_date'] = [
            pd.to_datetime(
                datetime(year=np.random.choice(anomalous_years), month=random.randint(1, 12), day=random.randint(1, 28)))
            for _ in range(mask.sum())
        ]

    # Добавляем дубликаты вычисляем количество строк для дублирования (5% от общего количества строк)
    num_rows_to_duplicate = max(1, int(len(activity_df) * 0.05))

    # Случайным образом выбираем строки для дублирования
    duplicated_rows = activity_df.sample(n=num_rows_to_duplicate, replace=True)

    # Добавляем дублируемые строки обратно в DataFrame
    activity_df = pd.concat([activity_df, duplicated_rows], ignore_index=True)

    # Перемешиваем строки, чтобы дубликаты не шли подряд
    activity_df = activity_df.sample(frac=1).reset_index(drop=True)

    return activity_df
