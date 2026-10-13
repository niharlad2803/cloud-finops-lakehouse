-- 1. dim_service
CREATE OR REPLACE TABLE `cloud-finops-lakehouse.finops_analytics.dim_service` AS
SELECT DISTINCT
    service_id, service_name, category, usage_unit,
    CASE WHEN service_id = 'SRV_SNOWFLAKE' THEN 'Snowflake' ELSE 'AWS' END AS cloud_provider
FROM `cloud-finops-lakehouse.finops_raw.stg_cloud_billing`;

-- 2. dim_tag
CREATE OR REPLACE TABLE `cloud-finops-lakehouse.finops_analytics.dim_tag` AS
SELECT 
    GENERATE_UUID() AS tag_key, raw_tags, environment, team, managed_by, cost_center,
    (environment != 'untagged' AND team != 'unknown') AS is_compliant
FROM (
    SELECT DISTINCT
        resource_tags AS raw_tags,
        JSON_VALUE(resource_tags, '$.environment') AS environment,
        JSON_VALUE(resource_tags, '$.team') AS team,
        COALESCE(JSON_VALUE(resource_tags, '$.managed_by'), 'manual') AS managed_by,
        COALESCE(JSON_VALUE(resource_tags, '$.cost_center'), 'UNALLOCATED') AS cost_center
    FROM `cloud-finops-lakehouse.finops_raw.stg_cloud_billing`
);

-- 3. dim_date
CREATE OR REPLACE TABLE `cloud-finops-lakehouse.finops_analytics.dim_date` AS
SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    FORMAT_DATE('%B', date_day) AS month_name,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    FORMAT_DATE('%A', date_day) AS day_of_week,
    EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) AS is_weekend
FROM (
    SELECT DISTINCT DATE(CAST(timestamp AS DATETIME)) AS date_day
    FROM `cloud-finops-lakehouse.finops_raw.stg_cloud_billing`
);

-- 4. fact_cloud_spend_daily
CREATE OR REPLACE TABLE `cloud-finops-lakehouse.finops_analytics.fact_cloud_spend_daily` AS
SELECT
    r.record_id,
    DATE(CAST(r.timestamp AS DATETIME)) AS date_day,
    CAST(r.timestamp AS DATETIME) AS usage_timestamp,
    r.service_id,
    t.tag_key,
    r.region,
    r.usage_amount,
    r.unblended_cost_usd,
    r.is_synthetic_anomaly
FROM `cloud-finops-lakehouse.finops_raw.stg_cloud_billing` r
LEFT JOIN `cloud-finops-lakehouse.finops_analytics.dim_tag` t
    ON r.resource_tags = t.raw_tags;
