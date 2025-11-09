DROP EXTERNAL TABLE IF EXISTS svistunov_ods.raw_activity;
CREATE EXTERNAL TABLE svistunov_ods.raw_activity (
    client_id TEXT,
    activity_date TEXT,
    activity_type TEXT,
    activity_location TEXT,
    ip_address TEXT,
    device TEXT
)
LOCATION ('pxf:///user/a.svistunov/a.svistunov_wave27/raw_activity_svistunov_wave27?PROFILE=parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');


DROP EXTERNAL TABLE IF EXISTS svistunov_ods.raw_clients;
CREATE EXTERNAL TABLE svistunov_ods.raw_clients (
    client_id TEXT,
    client_first_name TEXT,
    client_last_name TEXT,
    client_email TEXT,
    client_phone TEXT,
    client_address TEXT,
    client_birthday TEXT
)
LOCATION ('pxf:///user/a.svistunov/a.svistunov_wave27/raw_clients_svistunov_wave27?PROFILE=parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');


DROP EXTERNAL TABLE IF EXISTS svistunov_ods.raw_logins;
CREATE EXTERNAL TABLE svistunov_ods.raw_logins (
    client_id TEXT,
    login_date TEXT,
    ip_address TEXT,
    location TEXT,
    device TEXT
)
LOCATION ('pxf:///user/a.svistunov/a.svistunov_wave27/raw_logins_svistunov_wave27?PROFILE=parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');


DROP EXTERNAL TABLE IF EXISTS svistunov_ods.raw_payments;
CREATE EXTERNAL TABLE svistunov_ods.raw_payments (
    client_id TEXT,
    payment_id TEXT,
    payment_date TEXT,
    currency TEXT,
    amount TEXT,
    payment_method TEXT
)
LOCATION ('pxf:///user/a.svistunov/a.svistunov_wave27/raw_payments_svistunov_wave27?PROFILE=parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');


DROP EXTERNAL TABLE IF EXISTS svistunov_ods.raw_transactions;
CREATE EXTERNAL TABLE svistunov_ods.raw_transactions (
    client_id TEXT,
    transaction_id TEXT,
    transaction_date TEXT,
    transaction_type TEXT,
    account_number TEXT,
    currency TEXT,
    amount TEXT
)
LOCATION ('pxf:///user/a.svistunov/a.svistunov_wave27/raw_transactions_svistunov_wave27?PROFILE=parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
