TRUNCATE TABLE svistunov_dds.activity;
INSERT INTO svistunov_dds.activity 
SELECT 
    client_id,
    activity_date,
    activity_type,
    activity_location,
    ip_address,
    device
FROM svistunov_ods.activity 
WHERE 
    activity_date IS NOT NULL
    AND activity_date >= '2023-01-01'::TIMESTAMP
    AND activity_date < date_trunc('day', now())
    AND activity_date < '2026-01-01'::TIMESTAMP;
ANALYZE svistunov_dds.activity;

TRUNCATE TABLE svistunov_dds.clients;
INSERT INTO svistunov_dds.clients 
SELECT 
    client_id,
    client_first_name,
    client_last_name,
    client_email,
    client_phone,
    client_address,
    client_birthday
FROM svistunov_ods.clients 
WHERE 
    client_birthday IS NOT NULL
    AND client_birthday >= '1920-01-01'::DATE
    AND client_birthday < date_trunc('day', now())::DATE
    AND client_birthday <= '2010-01-01'::DATE;
ANALYZE svistunov_dds.clients;

TRUNCATE TABLE svistunov_dds.logins;
INSERT INTO svistunov_dds.logins 
SELECT 
    client_id,
    login_date,
    ip_address,
    location,
    device
FROM svistunov_ods.logins 
WHERE 
    login_date IS NOT NULL
    AND login_date >= '2023-01-01'::TIMESTAMP
    AND login_date < date_trunc('day', now())
    AND login_date < '2026-01-01'::TIMESTAMP
    AND client_id IS NOT NULL;
ANALYZE svistunov_dds.logins;

TRUNCATE TABLE svistunov_dds.payments;
INSERT INTO svistunov_dds.payments 
SELECT 
    client_id,
    payment_id,
    payment_date,
    currency,
    amount,
    payment_method
FROM svistunov_ods.payments
WHERE 
    payment_date IS NOT NULL
    AND payment_date >= '2023-01-01'::TIMESTAMP
    AND payment_date < date_trunc('day', now())
    AND payment_date < '2026-01-01'::TIMESTAMP;
ANALYZE svistunov_dds.payments;

TRUNCATE TABLE svistunov_dds.transactions;
INSERT INTO svistunov_dds.transactions 
SELECT 
    client_id,
    transaction_id,
    transaction_date,
    transaction_type,
    account_number,
    currency,
    amount
FROM svistunov_ods.transactions
WHERE 
    transaction_date IS NOT NULL
    AND transaction_date >= '2023-01-01'::TIMESTAMP
    AND transaction_date < date_trunc('day', now())
    AND transaction_date < '2026-01-01'::TIMESTAMP;
ANALYZE svistunov_dds.transactions;
