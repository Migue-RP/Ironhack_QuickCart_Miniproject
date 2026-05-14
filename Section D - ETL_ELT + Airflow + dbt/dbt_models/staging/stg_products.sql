SELECT
    product_id,
    TRIM(name) AS product_name,
    UPPER(TRIM(category)) AS category,
    CAST(price_usd AS NUMERIC(10,2)) AS price_usd,
    CAST(cost_usd AS NUMERIC(10,2)) AS cost_usd
FROM {{ source('quickcart_raw', 'products') }}
