-- Filter meter_register_agreement_configuration_interval table to get rid of values ourside of 2023
WITH filter_mraci AS (
    SELECT *
    FROM meter_register_agreement_configuration_interval
    WHERE interval_start <= '2023-12-31 23:30:00' AND interval_end >= '2023-01-01 00:00:00'
),
-- Filter out 0 percent annual consumption
ppc_filtered AS (
    SELECT *
    FROM period_profile_coefficients
    WHERE percentage_of_annual_consumption > 0
),
-- Join period_profile_coefficients
add_ppc AS (
    SELECT ppc.settlement_timestamp_utc, ppc.percentage_of_annual_consumption, fm.meter_point_id, fm.register_id, fm.product_category, fm.is_export, fm.is_charged_half_hourly, fm.is_variable, fm.gsp_group_id,
        fm.profile_class_id, fm.standard_settlement_configuration_id, fm.time_pattern_regime_id
    FROM filter_mraci fm
    JOIN ppc_filtered ppc
    ON fm.gsp_group_id = ppc.gsp_group_id AND fm.profile_class_id = ppc.profile_class_id AND fm.standard_settlement_configuration_id = ppc.standard_settlement_configuration_id AND 
    fm.time_pattern_regime_id = ppc.time_pattern_regime_id AND ppc.settlement_timestamp_utc >= fm.interval_start AND ppc.settlement_timestamp_utc < fm.interval_end
),
-- Join estimated_annual_consumption_interval
add_estimated AS (
    SELECT ppc.*, est.estimated_annual_consumption
    FROM add_ppc AS ppc
    JOIN estimated_annual_consumption_interval est
    ON ppc.meter_point_id = est.meter_point_id AND ppc.time_pattern_regime_id = est.time_pattern_regime_id AND ppc.settlement_timestamp_utc >= est.effective_from AND ppc.settlement_timestamp_utc < est.effective_to
),
-- Group at meter_point level to make joining estimated consumption to actual consumption 1-1.
meter_point_level AS (
    SELECT settlement_timestamp_utc, meter_point_id, AVG(percentage_of_annual_consumption) AS percentage_of_annual_consumption, AVG(estimated_annual_consumption) AS estimated_annual_consumption,
     product_category, is_export, is_charged_half_hourly, is_variable, gsp_group_id, profile_class_id, standard_settlement_configuration_id
    FROM add_estimated
    GROUP BY settlement_timestamp_utc, meter_point_id, product_category, is_export, is_charged_half_hourly, is_variable, gsp_group_id, profile_class_id, standard_settlement_configuration_id
),
-- Need to filter overlapping ranges for q2 file
q2 AS (
    SELECT *
    FROM smart_meter_consumption_filtered_q2
    WHERE settlement_timestamp_utc > '2023-03-31 22:30:00'
),
-- Create time series backbone from quarterly tables
smart_meter_consumption_filtered AS(
    SELECT *
    FROM smart_meter_consumption_filtered_q1
    UNION
    SELECT *
    FROM q2
    UNION
    SELECT *
    FROM smart_meter_consumption_filtered_q3
    UNION
    SELECT *
    FROM smart_meter_consumption_filtered_q4
),
-- Join estimated and actual ctes
estimated_and_actual AS (
    SELECT estimated.*, actual.smart_meter_consumption
    FROM meter_point_level estimated
    NATURAL JOIN smart_meter_consumption_filtered actual
),
-- Calculate estimated_half_hour_consumption
calc_estimated_half_hour_consumption AS (
    SELECT *, estimated_annual_consumption * percentage_of_annual_consumption AS estimated_half_hour_consumption
    FROM estimated_and_actual
),
-- Calculate demand price elasticity
calc_demand_price_elasticity AS (
    SELECT *, (smart_meter_consumption - estimated_half_hour_consumption) / estimated_half_hour_consumption AS demand_price_elasticity
    FROM calc_estimated_half_hour_consumption
    WHERE estimated_half_hour_consumption > 0
)

SELECT *
FROM calc_demand_price_elasticity