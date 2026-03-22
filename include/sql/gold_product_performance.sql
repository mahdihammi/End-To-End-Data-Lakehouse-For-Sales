MERGE INTO mahdi_ducklake.gold.product_performance AS tgt
USING (
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

) AS src

ON tgt.product_id = src.product_id

WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
    UPDATE SET
        product_name                = src.product_name,
        category                    = src.category,
        sub_category                = src.sub_category,
        times_ordered               = src.times_ordered,
        total_units_sold            = src.total_units_sold,
        total_revenue               = src.total_revenue,
        avg_unit_price              = src.avg_unit_price,
        total_profit                = src.total_profit,
        avg_profit_margin           = src.avg_profit_margin,
        total_discount_given        = src.total_discount_given,
        loss_order_count            = src.loss_order_count,
        revenue_rank_in_category    = src.revenue_rank_in_category,
        margin_rank_in_category     = src.margin_rank_in_category,
        load_date                   = src.load_date,
        updated_at                  = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (
        product_id, product_name, category, sub_category,
        times_ordered, total_units_sold, total_revenue, avg_unit_price,
        total_profit, avg_profit_margin, total_discount_given, loss_order_count,
        revenue_rank_in_category, margin_rank_in_category, load_date, updated_at
    )
    VALUES (
        src.product_id, src.product_name, src.category, src.sub_category,
        src.times_ordered, src.total_units_sold, src.total_revenue, src.avg_unit_price,
        src.total_profit, src.avg_profit_margin, src.total_discount_given, src.loss_order_count,
        src.revenue_rank_in_category, src.margin_rank_in_category, src.load_date, src.updated_at
    );