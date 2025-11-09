DROP TABLE IF EXISTS svistunov_dm.fraud_dashboard;
CREATE TABLE svistunov_dm.fraud_dashboard AS
SELECT
    c.client_id,
    c.client_first_name AS first_name,
    c.client_last_name AS last_name,
    t.total_transactions,
    ROUND(t.avg_transaction_amount, 2) AS avg_transaction_amount,
    t.max_transaction_amount,
    t.suspicious_transaction_count,
    a.login_count,
    (t.max_transaction_amount > t.avg_transaction_amount * 3) as financial_risk,
    (a.login_count > 10) as activity_risk,
    CASE
        WHEN (t.max_transaction_amount > t.avg_transaction_amount * 3) THEN 3
        WHEN (a.login_count > 10) THEN 2
        ELSE 0
    END as risk_score
FROM svistunov_dds.clients c
LEFT JOIN (
    SELECT
        client_id,
        COUNT(*) as total_transactions,
        AVG(amount) as avg_transaction_amount,
        MAX(amount) as max_transaction_amount,
        COUNT(
            CASE
                WHEN amount > (
                    SELECT AVG(amount)
                    FROM svistunov_dds.transactions t2
                    WHERE t2.client_id = t1.client_id
                ) * 3
                THEN 1
            END
        ) as suspicious_transaction_count
    FROM svistunov_dds.transactions t1
    GROUP BY client_id
) t ON c.client_id = t.client_id
LEFT JOIN (
    SELECT
        client_id,
        COUNT(*) as login_count
    FROM svistunov_dds.logins
    GROUP BY client_id
) a ON c.client_id = a.client_id;

ANALYZE svistunov_dm.fraud_dashboard;
