WITH master_ref AS (
    SELECT settlement_timestamp_utc, demand_price_elasticity
    FROM {{ ref( "demand_price_elasticity" )}}
),
elasticity_weekly AS (
    SELECT WEEK(settlement_timestamp_utc) AS a_week, AVG(demand_price_elasticity)
    FROM master_ref
    GROUP BY WEEK(settlement_timestamp_utc)
)
SELECT *
FROM elasticity_weekly