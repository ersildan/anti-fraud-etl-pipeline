import numpy as np
import pandas as pd
from faker import Faker


def clients():
    """Функция генерирует данные о клиентах"""

    fake = Faker()
    num_clients = 1000
    client_data = []

    for client_id in range(1, num_clients + 1):
        client_data.append({
            'client_id': client_id,
            'client_first_name': fake.first_name(),
            'client_last_name': fake.last_name(),
            'client_email': fake.email(),
            'client_phone': fake.phone_number(),
            'client_address': fake.address(),
            'client_birthday': fake.date_of_birth()
        })

    client_df = pd.DataFrame(client_data)

    # Добавление пропусков
    for column in ['client_id', 'client_first_name', 'client_last_name', 'client_email', 'client_phone',
                   'client_address', 'client_birthday']:
        mask = np.random.rand(len(client_df)) < 0.07
        client_df.loc[mask, column] = np.nan

    # Добавление дубликатов вычисляем количество строк для дублирования (4% от общего количества строк)
    num_rows_to_duplicate = max(1, int(len(client_df) * 0.04))

    # Случайным образом выбираем строки для дублирования
    duplicated_rows = client_df.sample(n=num_rows_to_duplicate, replace=True)

    # Добавляем дублируемые строки обратно в DataFrame
    client_df = pd.concat([client_df, duplicated_rows], ignore_index=True)

    # Перемешиваем строки, чтобы дубликаты не шли подряд
    client_df = client_df.sample(frac=1).reset_index(drop=True)

    return client_df
