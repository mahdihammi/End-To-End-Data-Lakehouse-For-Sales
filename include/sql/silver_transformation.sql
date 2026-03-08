MERGE INTO silver.orders_silver AS tgt
USING (

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
        state,
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

    FROM bronze.orders_bronze b
    WHERE b.order_id IS NOT NULL
    AND b.updated_at >= (
        SELECT COALESCE(MAX(updated_at), '1900-01-01')
        FROM silver.orders_silver
    )
    AND b.load_date > s.load_date

) AS src

ON tgt.row_id = src.row_id

WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
    UPDATE SET
        order_id = src.order_id,
        order_date = src.order_date,
        ship_date = src.ship_date,
        shipping_days = src.shipping_days,
        order_year = src.order_year,
        order_month = src.order_month,
        order_quarter = src.order_quarter,
        order_day_of_week = src.order_day_of_week,
        ship_mode = src.ship_mode,
        customer_id = src.customer_id,
        customer_name = src.customer_name,
        segment = src.segment,
        country = src.country,
        city = src.city,
        state = src.state,
        postal_code = src.postal_code,
        region = src.region,
        product_id = src.product_id,
        category = src.category,
        sub_category = src.sub_category,
        product_name = src.product_name,
        sales = src.sales,
        quantity = src.quantity,
        discount = src.discount,
        profit = src.profit,
        unit_price = src.unit_price,
        profit_margin = src.profit_margin,
        discount_amount = src.discount_amount,
        profit_status = src.profit_status,
        order_value_tier = src.order_value_tier,
        ship_mode_priority = src.ship_mode_priority,
        has_missing_financial_data = src.has_missing_financial_data,
        is_invalid_ship_date = src.is_invalid_ship_date,
        updated_at = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (
        row_id,
        order_id,
        order_date,
        ship_date,
        shipping_days,
        order_year,
        order_month,
        order_quarter,
        order_day_of_week,
        ship_mode,
        customer_id,
        customer_name,
        segment,
        country,
        city,
        state,
        postal_code,
        region,
        product_id,
        category,
        sub_category,
        product_name,
        sales,
        quantity,
        discount,
        profit,
        unit_price,
        profit_margin,
        discount_amount,
        profit_status,
        order_value_tier,
        ship_mode_priority,
        has_missing_financial_data,
        is_invalid_ship_date,
        updated_at
    )
    VALUES (
        src.row_id,
        src.order_id,
        src.order_date,
        src.ship_date,
        src.shipping_days,
        src.order_year,
        src.order_month,
        src.order_quarter,
        src.order_day_of_week,
        src.ship_mode,
        src.customer_id,
        src.customer_name,
        src.segment,
        src.country,
        src.city,
        src.state,
        src.postal_code,
        src.region,
        src.product_id,
        src.category,
        src.sub_category,
        src.product_name,
        src.sales,
        src.quantity,
        src.discount,
        src.profit,
        src.unit_price,
        src.profit_margin,
        src.discount_amount,
        src.profit_status,
        src.order_value_tier,
        src.ship_mode_priority,
        src.has_missing_financial_data,
        src.is_invalid_ship_date,
        src.updated_at
    );
