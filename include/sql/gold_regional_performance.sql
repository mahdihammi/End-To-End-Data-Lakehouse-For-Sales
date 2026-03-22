MERGE INTO mahdi_ducklake.gold.regional_performance AS tgt
USING (
    SELECT
        region,
        governorate,
        order_year,
        order_month,

        COUNT(DISTINCT order_id)                                                AS total_orders,
        COUNT(DISTINCT customer_id)                                             AS unique_customers,
        CAST(SUM(sales) AS DECIMAL(10,2))                                       AS total_revenue,
        CAST(SUM(profit) AS DECIMAL(10,2))                                      AS total_profit,
        CAST(AVG(profit_margin) AS DECIMAL(10,4))                               AS avg_profit_margin,
        CAST(AVG(shipping_days) AS DECIMAL(5,2))                                AS avg_shipping_days,

        SUM(CASE WHEN order_value_tier = 'High Value'   THEN 1 ELSE 0 END)     AS high_value_orders,
        SUM(CASE WHEN order_value_tier = 'Medium Value' THEN 1 ELSE 0 END)     AS medium_value_orders,
        SUM(CASE WHEN order_value_tier = 'Low Value'    THEN 1 ELSE 0 END)     AS low_value_orders,

        SUM(CASE WHEN segment = 'Consumer'    THEN 1 ELSE 0 END)               AS consumer_orders,
        SUM(CASE WHEN segment = 'Corporate'   THEN 1 ELSE 0 END)               AS corporate_orders,
        SUM(CASE WHEN segment = 'Home Office' THEN 1 ELSE 0 END)               AS home_office_orders,

        MAX(load_date)                                                          AS load_date,
        MAX(updated_at)                                                         AS updated_at

    FROM mahdi_ducklake.silver.orders_silver
    GROUP BY 1,2,3,4

) AS src

ON  tgt.region      = src.region
AND tgt.governorate = src.governorate
AND tgt.order_year  = src.order_year
AND tgt.order_month = src.order_month

WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
    UPDATE SET
        total_orders        = src.total_orders,
        unique_customers    = src.unique_customers,
        total_revenue       = src.total_revenue,
        total_profit        = src.total_profit,
        avg_profit_margin   = src.avg_profit_margin,
        avg_shipping_days   = src.avg_shipping_days,
        high_value_orders   = src.high_value_orders,
        medium_value_orders = src.medium_value_orders,
        low_value_orders    = src.low_value_orders,
        consumer_orders     = src.consumer_orders,
        corporate_orders    = src.corporate_orders,
        home_office_orders  = src.home_office_orders,
        load_date           = src.load_date,
        updated_at          = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (
        region, governorate, order_year, order_month,
        total_orders, unique_customers, total_revenue, total_profit,
        avg_profit_margin, avg_shipping_days,
        high_value_orders, medium_value_orders, low_value_orders,
        consumer_orders, corporate_orders, home_office_orders,
        load_date, updated_at
    )
    VALUES (
        src.region, src.governorate, src.order_year, src.order_month,
        src.total_orders, src.unique_customers, src.total_revenue, src.total_profit,
        src.avg_profit_margin, src.avg_shipping_days,
        src.high_value_orders, src.medium_value_orders, src.low_value_orders,
        src.consumer_orders, src.corporate_orders, src.home_office_orders,
        src.load_date, src.updated_at
    );