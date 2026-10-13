CREATE OR REPLACE VIEW `cloud-finops-lakehouse.finops_analytics.v_daily_cost_anomalies` AS
WITH daily_service_spend AS (
    SELECT date_day, service_id, ROUND(SUM(unblended_cost_usd), 2) AS total_daily_spend
    FROM `cloud-finops-lakehouse.finops_analytics.fact_cloud_spend_daily`
    GROUP BY date_day, service_id
),
rolling_baseline AS (
    SELECT
        date_day, service_id, total_daily_spend,
        AVG(total_daily_spend) OVER (PARTITION BY service_id ORDER BY date_day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS rolling_7d_avg,
        STDDEV(total_daily_spend) OVER (PARTITION BY service_id ORDER BY date_day ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS rolling_7d_stddev
    FROM daily_service_spend
)
SELECT
    date_day, service_id, total_daily_spend,
    ROUND(rolling_7d_avg, 2) AS rolling_7d_avg,
    ROUND(total_daily_spend - rolling_7d_avg, 2) AS excess_spend_usd,
    CASE 
        WHEN rolling_7d_stddev IS NOT NULL AND rolling_7d_stddev > 0
        THEN ROUND((total_daily_spend - rolling_7d_avg) / rolling_7d_stddev, 2)
        ELSE 0 
    END AS z_score,
    CASE
        WHEN rolling_7d_stddev IS NOT NULL 
             AND (total_daily_spend - rolling_7d_avg) > (2.5 * rolling_7d_stddev)
             AND (total_daily_spend - rolling_7d_avg) > 200
        THEN TRUE
        ELSE FALSE
    END AS is_anomaly_alert
FROM rolling_baseline
ORDER BY date_day DESC, excess_spend_usd DESC;
