SELECT
        product_id,
        product_name,
        category,
        sub_category,

        COUNT(DISTINCT order_id)                                                AS times_ordered,
        SUM(quantity)                                                           AS total_units_sold,
        CAST(SUM(sales) AS DECIMAL(10,2))                                       AS total_revenue,
        CAST(AVG(unit_price) AS DECIMAL(10,2))                                  AS avg_unit_price,
        CAST(SUM(profit) AS DECIMAL(10,2))                                      AS total_profit,
        CAST(AVG(profit_margin) AS DECIMAL(10,4))                               AS avg_profit_margin,
        CAST(SUM(discount_amount) AS DECIMAL(10,2))                             AS total_discount_given,
        SUM(CASE WHEN profit_status = 'Loss' THEN 1 ELSE 0 END)                AS loss_order_count,

        RANK() OVER (
            PARTITION BY category
            ORDER BY SUM(sales) DESC
        )                                                                       AS revenue_rank_in_category,

        RANK() OVER (
            PARTITION BY category
            ORDER BY AVG(profit_margin) DESC
        )                                                                       AS margin_rank_in_category,

        MAX(load_date)                                                          AS load_date,
        MAX(updated_at)                                                         AS updated_at

    FROM mahdi_ducklake.silver.orders_silver
    GROUP BY 1,2,3,4