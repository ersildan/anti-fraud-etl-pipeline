import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta


def logins():
    """Функция генерирует данные логирования клиентов"""

    fake = Faker()
    num_clients = 1000
    max_logins_per_client = 100

    def random_date(start, end):
        return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()

    # Генерация данных для клиентских логинов
    login_data = []
    for client_id in range(1, num_clients + 1):
        num_logins = random.randint(1, max_logins_per_client)
        for _ in range(num_logins):
            login_date = random_date(start_date, end_date)
            login_data.append({
                'client_id': client_id,
                'login_date': login_date,
                'ip_address': fake.ipv4(),
                'location': f"{random.uniform(-90, 90)}, {random.uniform(-180, 180)}",
                'device': fake.user_agent()
            })

    login_df = pd.DataFrame(login_data)

    # Добавление пропусков
    for column in ['client_id', 'login_date', 'ip_address', 'location', 'device']:
        mask = np.random.rand(len(login_df)) < 0.05
        login_df.loc[mask, column] = np.nan

    # Добавление аномальных дат
    anomalous_years = [1700, 1800, 2100, 2200]
    if len(login_df) > 0:
        mask = np.random.rand(len(login_df)) < 0.05
        login_df.loc[mask, 'login_date'] = [
            pd.to_datetime(
                datetime(year=np.random.choice(anomalous_years), month=random.randint(1, 12), day=random.randint(1, 28)))
            for _ in range(mask.sum())
        ]

    # Добавляем дубликаты вычисляем количество строк для дублирования (5% от общего количества строк)
    num_rows_to_duplicate = max(1, int(len(login_df) * 0.05))

    # Случайным образом выбираем строки для дублирования
    duplicated_rows = login_df.sample(n=num_rows_to_duplicate, replace=True)

    # Добавляем дублируемые строки обратно в DataFrame
    login_df = pd.concat([login_df, duplicated_rows], ignore_index=True)

    # Перемешиваем строки, чтобы дубликаты не шли подряд
    login_df = login_df.sample(frac=1).reset_index(drop=True)

    return login_df
