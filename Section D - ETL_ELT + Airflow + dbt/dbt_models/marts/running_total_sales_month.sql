{{ config(
    materialized='table'
) }}


WITH monthly_sales AS (
    SELECT *
    FROM {{ ref('int_monthly_sales') }}
)

SELECT
    sales_year,
    sales_month,
    total_sales,
    SUM(total_sales) OVER (ORDER BY sales_year, sales_month) AS running_total_sales_month

FROM monthly_sales
ORDER BY sales_year, sales_month

