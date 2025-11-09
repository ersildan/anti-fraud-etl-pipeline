TRUNCATE table svistunov_ods.activity;
INSERT INTO svistunov_ods.activity
SELECT
    CAST(REPLACE(client_id, '.0', '') AS INTEGER) as client_id,
    to_timestamp(activity_date, 'YYYY-MM-DD HH24:MI:SS') as activity_date,
    activity_type,
    activity_location,
    ip_address,
    device
FROM svistunov_ods.raw_activity;
ANALYZE svistunov_ods.activity;


TRUNCATE table svistunov_ods.clients;
INSERT INTO svistunov_ods.clients
SELECT DISTINCT ON (client_id)
    CAST(REPLACE(client_id, '.0', '') AS INTEGER) as client_id,
    client_first_name,
    client_last_name,
    client_email,
    client_phone,
    client_address,
    to_date(client_birthday, 'YYYY-MM-DD') as client_birthday
FROM svistunov_ods.raw_clients;
ANALYZE svistunov_ods.clients;


TRUNCATE table svistunov_ods.logins;
INSERT INTO svistunov_ods.logins
SELECT
    CAST(REPLACE(client_id, '.0', '') AS INTEGER) as client_id,
    to_timestamp(login_date, 'YYYY-MM-DD HH24:MI:SS') as login_date,
    ip_address,
    location,
    device
FROM svistunov_ods.raw_logins;
ANALYZE svistunov_ods.logins;


TRUNCATE table svistunov_ods.payments;
INSERT INTO svistunov_ods.payments
SELECT
    CAST(REPLACE(client_id, '.0', '') AS INTEGER) as client_id,
    CAST(REPLACE(payment_id, '.0', '') AS INTEGER) as payment_id,
    to_timestamp(payment_date, 'YYYY-MM-DD HH24:MI:SS') as payment_date,
    currency,
    CAST(amount AS DECIMAL(15,2)) as amount,
    payment_method
FROM svistunov_ods.raw_payments;
ANALYZE svistunov_ods.payments;


TRUNCATE table svistunov_ods.transactions;
INSERT INTO svistunov_ods.transactions
SELECT
    CAST(REPLACE(client_id, '.0', '') AS INTEGER) as client_id,
    CAST(REPLACE(transaction_id, '.0', '') AS INTEGER) as transaction_id,
    to_timestamp(transaction_date, 'YYYY-MM-DD HH24:MI:SS') as transaction_date,
    transaction_type,
    account_number,
    currency,
    CAST(amount AS DECIMAL(15,2)) as amount
FROM svistunov_ods.raw_transactions;
ANALYZE svistunov_ods.transactions;