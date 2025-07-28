WITH master_ref AS (
    SELECT settlement_timestamp_utc, demand_price_elasticity
    FROM {{ ref( "demand_price_elasticity" )}}
),
elasticity_weekly AS (
    SELECT WEEK(settlement_timestamp_utc) AS a_week, AVG(demand_price_elasticity) AS avg_demand_price_elasticity
    FROM master_ref
    GROUP BY WEEK(settlement_timestamp_utc)
),
create_date AS (
    SELECT DATE '2023-01-01' + INTERVAL (a_week * 7) DAY AS a_date, avg_demand_price_elasticity
    FROM elasticity_weekly
)
SELECT *
FROM create_date