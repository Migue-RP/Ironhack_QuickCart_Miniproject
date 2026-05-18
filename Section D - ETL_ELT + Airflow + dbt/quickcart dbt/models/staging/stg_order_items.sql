SELECT
    order_id,
    product_id,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price_usd AS NUMERIC(10,2)) AS unit_price_usd,
    CAST(quantity * unit_price_usd AS NUMERIC(10,2)) AS line_total_usd
FROM {{ source('quickcart_raw', 'order_items') }}
