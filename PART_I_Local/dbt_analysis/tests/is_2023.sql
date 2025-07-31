SELECT settlement_timestamp_utc
FROM {{ ref("demand_price_elasticity") }}
WHERE settlement_timestamp_utc < '2023-01-01 00:00:00' OR settlement_timestamp_utc >= '2024-01-01 00:00:00'