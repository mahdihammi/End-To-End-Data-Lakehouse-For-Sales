MERGE INTO mahdi_ducklake.gold.sales_performance AS tgt
USING (
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

) AS src

ON  tgt.order_year    = src.order_year
AND tgt.order_month   = src.order_month
AND tgt.region        = src.region
AND tgt.governorate   = src.governorate
AND tgt.segment       = src.segment
AND tgt.category      = src.category
AND tgt.sub_category  = src.sub_category

WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
    UPDATE SET
        total_orders          = src.total_orders,
        total_units_sold      = src.total_units_sold,
        total_revenue         = src.total_revenue,
        avg_order_value       = src.avg_order_value,
        total_profit          = src.total_profit,
        avg_profit_margin     = src.avg_profit_margin,
        profitable_orders     = src.profitable_orders,
        loss_orders           = src.loss_orders,
        total_discount_given  = src.total_discount_given,
        avg_discount_rate     = src.avg_discount_rate,
        avg_shipping_days     = src.avg_shipping_days,
        invalid_ship_dates    = src.invalid_ship_dates,
        load_date             = src.load_date,
        updated_at            = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (
        order_year, order_month, order_quarter,
        region, governorate, segment, category, sub_category,
        total_orders, total_units_sold, total_revenue, avg_order_value,
        total_profit, avg_profit_margin, profitable_orders, loss_orders,
        total_discount_given, avg_discount_rate, avg_shipping_days,
        invalid_ship_dates, load_date, updated_at
    )
    VALUES (
        src.order_year, src.order_month, src.order_quarter,
        src.region, src.governorate, src.segment, src.category, src.sub_category,
        src.total_orders, src.total_units_sold, src.total_revenue, src.avg_order_value,
        src.total_profit, src.avg_profit_margin, src.profitable_orders, src.loss_orders,
        src.total_discount_given, src.avg_discount_rate, src.avg_shipping_days,
        src.invalid_ship_dates, src.load_date, src.updated_at
    );