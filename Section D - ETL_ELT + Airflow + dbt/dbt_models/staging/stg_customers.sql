SELECT
    customer_id,
    name AS customer_name,
    email AS customer_email,
    UPPER(TRIM(country)) AS country,
    age,
    CAST(signup_date AS DATE) AS signup_date,
    marketing_opt_in
FROM {{ source('quickcart_raw', 'customers') }}