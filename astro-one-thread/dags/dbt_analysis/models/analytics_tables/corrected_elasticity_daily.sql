WITH corrected_elasticity_daily AS (
    SELECT a_date, {{ correct_elasticity("avg_demand_price_elasticity", 0.05) }} AS corrected_avg_demand_price_elasticity
    FROM {{ ref("elasticity_daily") }}
)
SELECT *
FROM corrected_elasticity_daily