WITH master_ref AS (
    SELECT settlement_timestamp_utc, smart_meter_consumption, estimated_half_hour_consumption
    FROM {{ ref( "demand_price_elasticity" )}}
),
actual_v_estimated_weekly AS (
    SELECT WEEK(settlement_timestamp_utc) AS a_week, AVG(smart_meter_consumption) AS smart_meter_consumption, AVG(estimated_half_hour_consumption) AS estimated_half_hour_consumption
    FROM master_ref
    GROUP BY WEEK(settlement_timestamp_utc)
),
create_date AS (
    SELECT DATE '2023-01-01' + INTERVAL (a_week * 7) DAY AS a_date, smart_meter_consumption, estimated_half_hour_consumption
    FROM actual_v_estimated_weekly
)
SELECT *
FROM create_date