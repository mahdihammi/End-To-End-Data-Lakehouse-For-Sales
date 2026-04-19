SELECT
        order_year,
        order_month,
        order_quarter,
        region,
        governorate,
        segment,
        category,
        sub_category,

        COUNT(DISTINCT order_id)                                                AS total_orders,
        SUM(quantity)                                                           AS total_units_sold,
        CAST(SUM(sales) AS DECIMAL(10,2))                                       AS total_revenue,
        CAST(AVG(sales) AS DECIMAL(10,2))                                       AS avg_order_value,
        CAST(SUM(profit) AS DECIMAL(10,2))                                      AS total_profit,
        CAST(AVG(profit_margin) AS DECIMAL(10,4))                               AS avg_profit_margin,
        SUM(CASE WHEN profit_status = 'Profitable' THEN 1 ELSE 0 END)          AS profitable_orders,
        SUM(CASE WHEN profit_status = 'Loss'       THEN 1 ELSE 0 END)          AS loss_orders,
        CAST(SUM(discount_amount) AS DECIMAL(10,2))                             AS total_discount_given,
        CAST(AVG(discount) AS DECIMAL(5,2))                                     AS avg_discount_rate,
        CAST(AVG(shipping_days) AS DECIMAL(5,2))                                AS avg_shipping_days,
        SUM(CASE WHEN is_invalid_ship_date THEN 1 ELSE 0 END)                  AS invalid_ship_dates,
        MAX(load_date)                                                          AS load_date,
        MAX(updated_at)                                                         AS updated_at

    FROM mahdi_ducklake.silver.orders_silver
    GROUP BY 1,2,3,4,5,6,7,8