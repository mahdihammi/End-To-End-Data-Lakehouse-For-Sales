MERGE INTO mahdi_ducklake.gold.monthly_trend AS tgt
USING (
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

) AS src

ON  tgt.order_year  = src.order_year
AND tgt.order_month = src.order_month

WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
    UPDATE SET
        order_quarter           = src.order_quarter,
        monthly_revenue         = src.monthly_revenue,
        monthly_profit          = src.monthly_profit,
        monthly_orders          = src.monthly_orders,
        active_customers        = src.active_customers,
        avg_profit_margin       = src.avg_profit_margin,
        total_discounts         = src.total_discounts,
        revenue_mom_change      = src.revenue_mom_change,
        revenue_mom_pct         = src.revenue_mom_pct,
        first_class_orders      = src.first_class_orders,
        second_class_orders     = src.second_class_orders,
        standard_class_orders   = src.standard_class_orders,
        load_date               = src.load_date,
        updated_at              = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (
        order_year, order_month, order_quarter,
        monthly_revenue, monthly_profit, monthly_orders, active_customers,
        avg_profit_margin, total_discounts, revenue_mom_change, revenue_mom_pct,
        first_class_orders, second_class_orders, standard_class_orders,
        load_date, updated_at
    )
    VALUES (
        src.order_year, src.order_month, src.order_quarter,
        src.monthly_revenue, src.monthly_profit, src.monthly_orders, src.active_customers,
        src.avg_profit_margin, src.total_discounts, src.revenue_mom_change, src.revenue_mom_pct,
        src.first_class_orders, src.second_class_orders, src.standard_class_orders,
        src.load_date, src.updated_at
    );