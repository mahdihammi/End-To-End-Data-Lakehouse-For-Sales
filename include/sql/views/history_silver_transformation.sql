
SELECT
        row_id,
        order_id,
        order_date,
        ship_date,
        DATE_DIFF('day', order_date, ship_date) AS shipping_days,
        YEAR(order_date) AS order_year,
        MONTH(order_date) AS order_month,
        QUARTER(order_date) AS order_quarter,
        DAYNAME(order_date) AS order_day_of_week,
        ship_mode,
        customer_id,
        customer_name,
        segment,
        country,
        city,
        governorate,
        postal_code,
        region,
        product_id,
        category,
        sub_category,
        product_name,
        CAST(sales AS DECIMAL(10,2)) AS sales,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(discount AS DECIMAL(5,2)) AS discount,
        CAST(profit AS DECIMAL(10,2)) AS profit,
        CAST(sales AS DECIMAL(10,2)) / NULLIF(CAST(quantity AS INTEGER),0) AS unit_price,
        CAST(profit AS DECIMAL(10,2)) / NULLIF(CAST(sales AS DECIMAL(10,2)),0) AS profit_margin,
        CAST(sales AS DECIMAL(10,2)) * CAST(discount AS DECIMAL(5,2)) AS discount_amount,
        CASE
            WHEN CAST(profit AS DECIMAL(10,2)) > 0 THEN 'Profitable'
            WHEN CAST(profit AS DECIMAL(10,2)) = 0 THEN 'Break-even'
            ELSE 'Loss'
        END AS profit_status,
        CASE
            WHEN CAST(sales AS DECIMAL(10,2)) >= 200 THEN 'High Value'
            WHEN CAST(sales AS DECIMAL(10,2)) >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS order_value_tier,
        CASE
            WHEN ship_mode = 'Same Day' THEN 0
            WHEN ship_mode = 'First Class' THEN 1
            WHEN ship_mode = 'Second Class' THEN 2
            WHEN ship_mode = 'Standard Class' THEN 3
            ELSE 4
        END AS ship_mode_priority,
        CASE
            WHEN sales IS NULL OR quantity IS NULL OR profit IS NULL THEN TRUE
            ELSE FALSE
        END AS has_missing_financial_data,
        CASE
            WHEN DATE_DIFF('day', order_date, ship_date) < 0 THEN TRUE
            ELSE FALSE
        END AS is_invalid_ship_date,
        updated_at,
        load_date

    FROM mahdi_ducklake.bronze.orders_bronze b
    WHERE b.order_id IS NOT NULL