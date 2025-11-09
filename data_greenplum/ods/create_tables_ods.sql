CREATE TABLE IF NOT EXISTS svistunov_ods.activity (
    client_id INTEGER,
    activity_date TIMESTAMP,
    activity_type TEXT,
    activity_location TEXT,
    ip_address TEXT,
    device TEXT
)

DISTRIBUTED BY (client_id);

CREATE TABLE IF NOT EXISTS svistunov_ods.clients (
    client_id INTEGER,
    client_first_name TEXT,
    client_last_name TEXT,
    client_email TEXT,
    client_phone TEXT,
    client_address TEXT,
    client_birthday DATE
)

DISTRIBUTED BY (client_id);

CREATE TABLE IF NOT EXISTS svistunov_ods.logins (
    client_id INTEGER,
    login_date TIMESTAMP,
    ip_address TEXT,
    location TEXT,
    device TEXT
)

DISTRIBUTED BY (client_id);


CREATE TABLE IF NOT EXISTS svistunov_ods.payments (
    client_id INTEGER,
    payment_id INTEGER,
    payment_date TIMESTAMP,
    currency TEXT,
    amount DECIMAL(15,2),
    payment_method TEXT
)

DISTRIBUTED BY (client_id);

CREATE TABLE IF NOT EXISTS svistunov_ods.transactions (
    client_id INTEGER,
    transaction_id INTEGER,
    transaction_date TIMESTAMP,
    transaction_type TEXT,
    account_number TEXT,
    currency TEXT,
    amount DECIMAL(15,2)
)

DISTRIBUTED BY (client_id);