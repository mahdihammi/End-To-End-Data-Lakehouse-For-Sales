SELECT
        order_year,
        order_month,
        order_quarter,

        CAST(SUM(sales) AS DECIMAL(10,2))                                       AS monthly_revenue,
        CAST(SUM(profit) AS DECIMAL(10,2))                                      AS monthly_profit,
        COUNT(DISTINCT order_id)                                                AS monthly_orders,
        COUNT(DISTINCT customer_id)                                             AS active_customers,
        CAST(AVG(profit_margin) AS DECIMAL(10,4))                               AS avg_profit_margin,
        CAST(SUM(discount_amount) AS DECIMAL(10,2))                             AS total_discounts,

        CAST(SUM(sales) - LAG(SUM(sales)) OVER (
            ORDER BY order_year, order_month
        ) AS DECIMAL(10,2))                                                     AS revenue_mom_change,

        CAST(ROUND(
            (SUM(sales) - LAG(SUM(sales)) OVER (
                ORDER BY order_year, order_month)
            ) / NULLIF(LAG(SUM(sales)) OVER (
                ORDER BY order_year, order_month), 0) * 100
        , 2) AS DECIMAL(10,2))                                                  AS revenue_mom_pct,

        SUM(CASE WHEN ship_mode = 'First Class'    THEN 1 ELSE 0 END)          AS first_class_orders,
        SUM(CASE WHEN ship_mode = 'Second Class'   THEN 1 ELSE 0 END)          AS second_class_orders,
        SUM(CASE WHEN ship_mode = 'Standard Class' THEN 1 ELSE 0 END)          AS standard_class_orders,

        MAX(load_date)                                                          AS load_date,
        MAX(updated_at)                                                         AS updated_at

    FROM mahdi_ducklake.silver.orders_silver
    GROUP BY 1,2,3