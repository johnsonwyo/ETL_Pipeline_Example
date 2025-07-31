WITH master_ref AS (
    SELECT settlement_timestamp_utc, demand_price_elasticity, is_export, is_charged_half_hourly, is_variable
    FROM {{ ref( "demand_price_elasticity" )}}
),
elasticity_export_weekly AS (
    SELECT WEEK(settlement_timestamp_utc) AS a_week, AVG(demand_price_elasticity) AS avg_demand_price_elasticity_export
    FROM master_ref
    WHERE is_export = True
    GROUP BY WEEK(settlement_timestamp_utc)
),
elasticity_charged_half_hourly_weekly AS (
    SELECT WEEK(settlement_timestamp_utc) AS a_week, AVG(demand_price_elasticity) AS avg_demand_price_elasticity_charged_half_hourly
    FROM master_ref
    WHERE is_charged_half_hourly = True
    GROUP BY WEEK(settlement_timestamp_utc)
),
elasticity_variable_weekly AS (
    SELECT WEEK(settlement_timestamp_utc) AS a_week, AVG(demand_price_elasticity) AS avg_demand_price_elasticity_variable
    FROM master_ref
    WHERE is_variable = True
    GROUP BY WEEK(settlement_timestamp_utc)
),
join_tables AS (
    SELECT a_week, e.avg_demand_price_elasticity_export AS adpe_export, hh.avg_demand_price_elasticity_charged_half_hourly AS adpe_half_hour,
        v.avg_demand_price_elasticity_variable AS adpe_variable
    FROM elasticity_export_weekly e
    JOIN elasticity_charged_half_hourly_weekly hh USING(a_week)
    JOIN elasticity_variable_weekly v USING(a_week)
),
create_date AS (
    SELECT DATE '2023-01-01' + INTERVAL (a_week * 7) DAY AS a_date, adpe_export, adpe_half_hour, adpe_variable
    FROM join_tables
)
SELECT *
FROM create_date