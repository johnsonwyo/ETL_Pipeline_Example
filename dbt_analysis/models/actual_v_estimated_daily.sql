WITH master_ref AS (
    SELECT settlement_timestamp_utc, smart_meter_consumption, estimated_half_hour_consumption
    FROM {{ ref( "demand_price_elasticity" )}}
),
actual_v_estimated_daily AS (
    SELECT CAST(settlement_timestamp_utc AS DATE) AS a_date, AVG(smart_meter_consumption) AS smart_meter_consumption, AVG(estimated_half_hour_consumption) AS estimated_half_hour_consumption
    FROM master_ref
    GROUP BY CAST(settlement_timestamp_utc AS DATE)
)
SELECT *
FROM actual_v_estimated_daily