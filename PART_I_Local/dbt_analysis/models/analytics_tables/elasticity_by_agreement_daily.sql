WITH master_ref AS (
    SELECT settlement_timestamp_utc, demand_price_elasticity, is_export, is_charged_half_hourly, is_variable
    FROM {{ ref( "demand_price_elasticity" )}}
),
elasticity_export_weekly AS (
    SELECT CAST(settlement_timestamp_utc AS DATE) AS a_date, AVG(demand_price_elasticity) AS avg_demand_price_elasticity_export
    FROM master_ref
    WHERE is_export = True
    GROUP BY CAST(settlement_timestamp_utc AS DATE)
),
elasticity_charged_half_hourly_weekly AS (
    SELECT CAST(settlement_timestamp_utc AS DATE) AS a_date, AVG(demand_price_elasticity) AS avg_demand_price_elasticity_charged_half_hourly
    FROM master_ref
    WHERE is_charged_half_hourly = True
    GROUP BY CAST(settlement_timestamp_utc AS DATE)
),
elasticity_variable_weekly AS (
    SELECT CAST(settlement_timestamp_utc AS DATE) AS a_date, AVG(demand_price_elasticity) AS avg_demand_price_elasticity_variable
    FROM master_ref
    WHERE is_variable = True
    GROUP BY CAST(settlement_timestamp_utc AS DATE)
),
join_tables AS (
    SELECT a_date, e.avg_demand_price_elasticity_export AS adpe_export, hh.avg_demand_price_elasticity_charged_half_hourly AS adpe_half_hour,
        v.avg_demand_price_elasticity_variable AS adpe_variable
    FROM elasticity_export_weekly e
    JOIN elasticity_charged_half_hourly_weekly hh USING(a_date)
    JOIN elasticity_variable_weekly v USING(a_date)
)
SELECT *
FROM join_tables