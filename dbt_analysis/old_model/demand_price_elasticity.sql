-- Need to filter overlapping ranges for q2 file
WITH q2 AS (
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
-- Filter meter_register_agreement_configuration_interval table to get rid of values ourside of 2023
filter_mraci AS (
    SELECT *
    FROM meter_register_agreement_configuration_interval
    WHERE interval_start <= '2023-12-31 23:30:00' AND interval_end >= '2023-01-01 00:00:00'
),
-- Join meter_register_agreement_configuration_interval table
add_mraci AS (
    SELECT smcf.settlement_timestamp_utc, smcf.smart_meter_consumption, mraci.meter_point_id, mraci.register_id, mraci.product_category, mraci.is_export, mraci.is_charged_half_hourly, mraci.is_variable, mraci.gsp_group_id,
        mraci.profile_class_id, mraci.standard_settlement_configuration_id, mraci.time_pattern_regime_id
    FROM smart_meter_consumption_filtered smcf
    JOIN filter_mraci mraci
    ON mraci.meter_point_id = smcf.meter_point_id AND smcf.settlement_timestamp_utc >= mraci.interval_start AND smcf.settlement_timestamp_utc < mraci.interval_end
),
-- Join period_profile_coefficients
add_ppc AS (
    SELECT am.*, ppc.percentage_of_annual_consumption
    FROM add_mraci am
    NATURAL JOIN period_profile_coefficients ppc
),
-- Join estimated_annual_consumption_interval
add_estimated AS (
    SELECT ppc.*, est.estimated_annual_consumption
    FROM add_ppc AS ppc
    JOIN estimated_annual_consumption_interval est
    ON ppc.meter_point_id = est.meter_point_id AND ppc.time_pattern_regime_id = est.time_pattern_regime_id AND ppc.settlement_timestamp_utc >= est.effective_from AND ppc.settlement_timestamp_utc < est.effective_to
),
-- Calculate estimated_half_hour_consumption
calc_estimated_half_hour_consumption AS (
    SELECT *, estimated_annual_consumption * percentage_of_annual_consumption AS estimated_half_hour_consumption
    FROM add_estimated
)

SELECT *
FROM calc_estimated_half_hour_consumption