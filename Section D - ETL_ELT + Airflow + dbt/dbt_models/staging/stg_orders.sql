SELECT
    order_id,
    customer_id,
    CAST(order_time AS TIMESTAMP) AS order_time,
    payment_method,
    discount_pct,
    subtotal_usd,
    total_usd AS revenue_usd,
    UPPER(TRIM(country)) AS country,
    device,
    source
FROM {{ source('quickcart_raw', 'orders') }}
