WITH master_ref AS (
    SELECT settlement_timestamp_utc, demand_price_elasticity
    FROM {{ ref( "demand_price_elasticity" )}}
),
elasticity_daily AS (
    SELECT CAST(settlement_timestamp_utc AS DATE) AS a_date, AVG(demand_price_elasticity) AS avg_demand_price_elasticity
    FROM master_ref
    GROUP BY CAST(settlement_timestamp_utc AS DATE)
)
SELECT *
FROM elasticity_daily