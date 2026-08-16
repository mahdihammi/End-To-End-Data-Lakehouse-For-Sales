import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from include.helpers.ducklake_init import attach_ducklake_and_set_secrets


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Tunisia Online Sales Lakehouse",
    page_icon="📊",
    layout="wide",
)


# ============================================================
# CUSTOM CSS
# ============================================================

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
        }

        h1 {
            padding-bottom: 0.2rem;
        }

        div[data-testid="stMetric"] {
            background-color: rgba(128, 128, 128, 0.08);
            border: 1px solid rgba(128, 128, 128, 0.18);
            padding: 16px;
            border-radius: 12px;
        }

        div[data-testid="stMetricLabel"] {
            font-weight: 600;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# DUCKLAKE CONNECTION
# ============================================================

@st.cache_resource
def get_connection():
    conn = duckdb.connect(database=":memory:")

    attach_ducklake_and_set_secrets(
        conn=conn,
        dbname=os.getenv("DBNAME", "postgres"),
        supabase_host=os.getenv("SUPABASE_HOST"),
        supabase_port=os.getenv("SUPABASE_PORT"),
        supabase_user=os.getenv("SUPABASE_USER"),
        supabase_pwd=os.getenv("SUPABASE_PWD"),
        minio_endpoint=os.getenv(
            "MINIO_ENDPOINT",
            "minio:9000",
        ),
        minio_access_key=os.getenv(
            "MINIO_ACCESS_KEY",
            "minioadmin",
        ),
        minio_secret_key=os.getenv(
            "MINIO_SECRET_KEY",
            "minioadmin",
        ),
        ducklake_name=os.getenv(
            "DUCKLAKE_NAME",
            "mahdi_ducklake",
        ),
        data_path=os.getenv(
            "DATA_PATH",
            "s3://lakehouse-project/",
        ),
        postgres_secret_name=os.getenv(
            "DUCKDB_SECRET",
            "__default_postgres",
        ),
    )

    return conn


# ============================================================
# LOAD DATA
# ============================================================

def load_monthly_sales(
    conn,
    catalog_name: str,
) -> pd.DataFrame:

    df = conn.execute(
        f"""
        SELECT
            order_year,
            order_month,
            order_quarter,
            monthly_revenue,
            monthly_profit,
            monthly_orders,
            active_customers,
            avg_profit_margin,
            total_discounts,
            revenue_mom_change,
            revenue_mom_pct,
            first_class_orders,
            second_class_orders,
            standard_class_orders
        FROM "{catalog_name}".gold.sales_monthly_kpis
        ORDER BY
            order_year,
            order_month
        """
    ).df()

    if not df.empty:
        df["period"] = pd.to_datetime(
            {
                "year": df["order_year"],
                "month": df["order_month"],
                "day": 1,
            }
        )

        # Calculate our own MoM growth.
        df["calculated_mom_pct"] = (
            df["monthly_revenue"]
            .pct_change()
            .mul(100)
        )

    return df


def load_regional_sales(
    conn,
    catalog_name: str,
) -> pd.DataFrame:

    return conn.execute(
        f"""
        SELECT
            order_year,
            region,
            governorate,

            SUM(total_revenue) AS total_revenue,
            SUM(total_profit) AS total_profit,
            SUM(total_orders) AS total_orders,

            SUM(high_value_orders) AS high_value_orders,
            SUM(medium_value_orders) AS medium_value_orders,
            SUM(low_value_orders) AS low_value_orders,

            SUM(consumer_orders) AS consumer_orders,
            SUM(corporate_orders) AS corporate_orders,
            SUM(home_office_orders) AS home_office_orders

        FROM "{catalog_name}".gold.region_kpis

        GROUP BY
            order_year,
            region,
            governorate

        ORDER BY
            order_year,
            total_revenue DESC
        """
    ).df()


def load_products(
    conn,
    catalog_name: str,
) -> pd.DataFrame:

    return conn.execute(
        f"""
        SELECT
            product_name,
            category,
            sub_category,
            total_units_sold,
            total_revenue,
            total_profit,
            avg_profit_margin

        FROM "{catalog_name}".gold.product_kpis

        ORDER BY
            total_revenue DESC
        """
    ).df()


def load_customers(
    conn,
    catalog_name: str,
) -> pd.DataFrame:

    return conn.execute(
        f"""
        SELECT
            customer_id,
            customer_name,
            segment,
            region,
            governorate,

            total_orders,
            first_order_date,
            last_order_date,
            customer_lifespan_days,

            total_spent,
            avg_order_value,
            total_profit_generated,
            avg_discount_used,

            total_units_bought,
            product_variety,

            customer_value_tier,
            customer_loyalty_tier

        FROM "{catalog_name}".gold.customer_kpis

        ORDER BY
            total_profit_generated DESC
        """
    ).df()


# ============================================================
# HELPERS
# ============================================================

def money(value):
    if pd.isna(value):
        return "0 TND"

    return f"{value:,.2f} TND"


def percentage(value):
    if pd.isna(value):
        return "N/A"

    return f"{value:,.1f}%"


def style_chart(fig):
    fig.update_layout(
        template="plotly_white",
        margin=dict(
            l=20,
            r=20,
            t=60,
            b=20,
        ),
        legend_title_text="",
    )

    return fig


# ============================================================
# DASHBOARD
# ============================================================

st.title("🇹🇳 Tunisia Online Sales Lakehouse")

st.caption(
    "Executive sales, customer, product and regional performance dashboard"
)

try:

    # --------------------------------------------------------
    # CONNECTION
    # --------------------------------------------------------

    conn = get_connection()

    catalog_name = os.getenv(
        "DUCKLAKE_NAME",
        "mahdi_ducklake",
    )

    monthly_df = load_monthly_sales(
        conn,
        catalog_name,
    )

    regional_df = load_regional_sales(
        conn,
        catalog_name,
    )

    product_df = load_products(
        conn,
        catalog_name,
    )

    customer_df = load_customers(
        conn,
        catalog_name,
    )

    if monthly_df.empty:
        st.warning(
            "The monthly Gold KPI table is empty."
        )
        st.stop()

    # ========================================================
    # SIDEBAR
    # ========================================================

    st.sidebar.header("Dashboard Filters")

    available_years = sorted(
        monthly_df["order_year"]
        .dropna()
        .unique()
        .tolist()
    )

    selected_year = st.sidebar.selectbox(
        "Sales Year",
        ["All"] + available_years,
    )

    if selected_year == "All":
        monthly_filtered = monthly_df.copy()
        regional_filtered = regional_df.copy()
    else:
        monthly_filtered = monthly_df[
            monthly_df["order_year"]
            == selected_year
        ].copy()

        regional_filtered = regional_df[
            regional_df["order_year"]
            == selected_year
        ].copy()

    st.sidebar.divider()

    st.sidebar.caption(
        "Customer and product Gold tables contain "
        "lifetime-level aggregates, so the year filter "
        "currently applies to monthly and regional analysis."
    )

    # ========================================================
    # KPI CALCULATIONS
    # ========================================================

    monthly_filtered = monthly_filtered.sort_values(
        "period"
    )

    total_revenue = (
        monthly_filtered["monthly_revenue"].sum()
    )

    total_profit = (
        monthly_filtered["monthly_profit"].sum()
    )

    total_orders = (
        monthly_filtered["monthly_orders"].sum()
    )

    profit_margin = (
        total_profit / total_revenue * 100
        if total_revenue
        else 0
    )

    latest_row = monthly_filtered.iloc[-1]

    latest_active_customers = (
        latest_row["active_customers"]
    )

    # --------------------------------------------------------
    # Revenue growth
    # --------------------------------------------------------

    if len(monthly_filtered) >= 2:

        current_revenue = monthly_filtered.iloc[-1][
            "monthly_revenue"
        ]

        previous_revenue = monthly_filtered.iloc[-2][
            "monthly_revenue"
        ]

        if previous_revenue:

            revenue_growth = (
                (
                    current_revenue
                    - previous_revenue
                )
                / previous_revenue
                * 100
            )

        else:
            revenue_growth = 0

    else:
        revenue_growth = None

    # --------------------------------------------------------
    # Best month
    # --------------------------------------------------------

    best_month_row = monthly_filtered.loc[
        monthly_filtered[
            "monthly_revenue"
        ].idxmax()
    ]

    best_month_name = pd.to_datetime(
        best_month_row["period"]
    ).strftime("%B %Y")

    best_month_revenue = (
        best_month_row["monthly_revenue"]
    )

    # ========================================================
    # KPI CARDS
    # ========================================================

    st.subheader("Executive KPIs")

    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

    kpi1.metric(
        "Revenue",
        money(total_revenue),
        delta=(
            percentage(revenue_growth)
            if revenue_growth is not None
            else None
        ),
    )

    kpi2.metric(
        "Profit",
        money(total_profit),
    )

    kpi3.metric(
        "Orders",
        f"{total_orders:,.0f}",
    )

    kpi4.metric(
        "Profit Margin",
        percentage(profit_margin),
    )

    kpi5.metric(
        "Active Customers",
        f"{latest_active_customers:,.0f}",
    )

    st.caption(
        f"🏆 Best sales month: "
        f"**{best_month_name}** — "
        f"**{money(best_month_revenue)}** revenue"
    )

    st.divider()

    # ========================================================
    # TABS
    # ========================================================

    (
        overview_tab,
        trends_tab,
        geography_tab,
        product_tab,
        customer_tab,
    ) = st.tabs(
        [
            "📊 Overview",
            "📈 Sales Trends",
            "🗺️ Geography",
            "📦 Products",
            "👥 Customers",
        ]
    )

    # ========================================================
    # OVERVIEW
    # ========================================================

    with overview_tab:

        st.subheader("Business Overview")

        left, right = st.columns(
            [2, 1]
        )

        # ----------------------------------------------------
        # Revenue trend
        # ----------------------------------------------------

        with left:

            overview_revenue_fig = px.area(
                monthly_filtered,
                x="period",
                y="monthly_revenue",
                title="Revenue Trend",
                markers=True,
                labels={
                    "period": "Month",
                    "monthly_revenue":
                        "Revenue (TND)",
                },
            )

            style_chart(
                overview_revenue_fig
            )

            st.plotly_chart(
                overview_revenue_fig,
                use_container_width=True,
            )

        # ----------------------------------------------------
        # Customer segment distribution
        # ----------------------------------------------------

        with right:

            segment_overview = (
                customer_df
                .groupby(
                    "segment",
                    as_index=False,
                )
                .agg(
                    customers=(
                        "customer_id",
                        "nunique",
                    )
                )
            )

            segment_pie = px.pie(
                segment_overview,
                names="segment",
                values="customers",
                hole=0.55,
                title="Customer Segments",
            )

            style_chart(segment_pie)

            st.plotly_chart(
                segment_pie,
                use_container_width=True,
            )

        # ----------------------------------------------------
        # Revenue and profit
        # ----------------------------------------------------

        revenue_profit_fig = px.line(
            monthly_filtered,
            x="period",
            y=[
                "monthly_revenue",
                "monthly_profit",
            ],
            markers=True,
            title="Revenue vs Profit",
            labels={
                "period": "Month",
                "value": "Amount (TND)",
                "variable": "Metric",
            },
        )

        style_chart(
            revenue_profit_fig
        )

        st.plotly_chart(
            revenue_profit_fig,
            use_container_width=True,
        )

        # ----------------------------------------------------
        # Quick business highlights
        # ----------------------------------------------------

        best_region = (
            regional_filtered
            .groupby(
                "region",
                as_index=False,
            )
            .agg(
                revenue=(
                    "total_revenue",
                    "sum",
                )
            )
            .sort_values(
                "revenue",
                ascending=False,
            )
        )

        best_customer = customer_df.iloc[0]

        best_product = product_df.iloc[0]

        highlight1, highlight2, highlight3 = (
            st.columns(3)
        )

        with highlight1:
            st.info(
                "📍 **Top Region**\n\n"
                f"{best_region.iloc[0]['region']}\n\n"
                f"{money(best_region.iloc[0]['revenue'])}"
            )

        with highlight2:
            st.info(
                "👤 **Most Profitable Customer**\n\n"
                f"{best_customer['customer_name']}\n\n"
                f"{money(best_customer['total_profit_generated'])}"
            )

        with highlight3:
            st.info(
                "📦 **Top Revenue Product**\n\n"
                f"{best_product['product_name']}\n\n"
                f"{money(best_product['total_revenue'])}"
            )

    # ========================================================
    # SALES TRENDS
    # ========================================================

    with trends_tab:

        st.subheader("Monthly Sales Performance")

        # ----------------------------------------------------
        # Revenue + profit
        # ----------------------------------------------------

        sales_trend_fig = px.line(
            monthly_filtered,
            x="period",
            y=[
                "monthly_revenue",
                "monthly_profit",
            ],
            markers=True,
            title="Monthly Revenue and Profit",
            labels={
                "period": "Month",
                "value": "TND",
                "variable": "Metric",
            },
        )

        style_chart(
            sales_trend_fig
        )

        st.plotly_chart(
            sales_trend_fig,
            use_container_width=True,
        )

        chart1, chart2 = st.columns(2)

        # ----------------------------------------------------
        # Orders
        # ----------------------------------------------------

        with chart1:

            monthly_orders_fig = px.bar(
                monthly_filtered,
                x="period",
                y="monthly_orders",
                title="Monthly Order Volume",
                labels={
                    "period": "Month",
                    "monthly_orders":
                        "Orders",
                },
            )

            style_chart(
                monthly_orders_fig
            )

            st.plotly_chart(
                monthly_orders_fig,
                use_container_width=True,
            )

        # ----------------------------------------------------
        # MoM growth
        # ----------------------------------------------------

        with chart2:

            growth_fig = px.bar(
                monthly_filtered,
                x="period",
                y="calculated_mom_pct",
                title="Revenue Month-over-Month Growth",
                labels={
                    "period": "Month",
                    "calculated_mom_pct":
                        "Growth (%)",
                },
            )

            growth_fig.add_hline(
                y=0,
                line_dash="dash",
            )

            style_chart(growth_fig)

            st.plotly_chart(
                growth_fig,
                use_container_width=True,
            )

        # ----------------------------------------------------
        # Shipping methods
        # ----------------------------------------------------

        shipping_df = monthly_filtered[
            [
                "period",
                "first_class_orders",
                "second_class_orders",
                "standard_class_orders",
            ]
        ].melt(
            id_vars="period",
            var_name="shipping_class",
            value_name="orders",
        )

        shipping_df[
            "shipping_class"
        ] = shipping_df[
            "shipping_class"
        ].replace(
            {
                "first_class_orders":
                    "First Class",
                "second_class_orders":
                    "Second Class",
                "standard_class_orders":
                    "Standard Class",
            }
        )

        shipping_fig = px.area(
            shipping_df,
            x="period",
            y="orders",
            color="shipping_class",
            title="Shipping Class Mix",
            labels={
                "period": "Month",
                "orders": "Orders",
                "shipping_class":
                    "Shipping Class",
            },
        )

        style_chart(shipping_fig)

        st.plotly_chart(
            shipping_fig,
            use_container_width=True,
        )

        # ----------------------------------------------------
        # Discounts vs margin
        # ----------------------------------------------------

        discount_margin_fig = px.scatter(
            monthly_filtered,
            x="total_discounts",
            y="avg_profit_margin",
            size="monthly_orders",
            hover_name="period",
            title="Discounts vs Profit Margin",
            labels={
                "total_discounts":
                    "Total Discounts",
                "avg_profit_margin":
                    "Average Profit Margin",
                "monthly_orders":
                    "Orders",
            },
        )

        style_chart(
            discount_margin_fig
        )

        st.plotly_chart(
            discount_margin_fig,
            use_container_width=True,
        )

        with st.expander(
            "View Monthly KPI Data"
        ):

            st.dataframe(
                monthly_filtered,
                use_container_width=True,
                hide_index=True,
            )

    # ========================================================
    # GEOGRAPHY
    # ========================================================

    with geography_tab:

        st.subheader(
            "Regional & Governorate Performance"
        )

        # ----------------------------------------------------
        # Region filter
        # ----------------------------------------------------

        available_regions = sorted(
            regional_filtered["region"]
            .dropna()
            .unique()
            .tolist()
        )

        selected_region = st.selectbox(
            "Region",
            ["All"] + available_regions,
            key="region_filter",
        )

        regional_view = (
            regional_filtered.copy()
        )

        if selected_region != "All":
            regional_view = regional_view[
                regional_view["region"]
                == selected_region
            ]

        governorate_summary = (
            regional_view
            .groupby(
                [
                    "region",
                    "governorate",
                ],
                as_index=False,
            )
            .agg(
                total_revenue=(
                    "total_revenue",
                    "sum",
                ),
                total_profit=(
                    "total_profit",
                    "sum",
                ),
                total_orders=(
                    "total_orders",
                    "sum",
                ),
                consumer_orders=(
                    "consumer_orders",
                    "sum",
                ),
                corporate_orders=(
                    "corporate_orders",
                    "sum",
                ),
                home_office_orders=(
                    "home_office_orders",
                    "sum",
                ),
            )
        )

        geo1, geo2 = st.columns(2)

        # ----------------------------------------------------
        # Governorate ranking
        # ----------------------------------------------------

        with geo1:

            top_governorates = (
                governorate_summary
                .nlargest(
                    12,
                    "total_revenue",
                )
                .sort_values(
                    "total_revenue"
                )
            )

            governorate_fig = px.bar(
                top_governorates,
                x="total_revenue",
                y="governorate",
                color="region",
                orientation="h",
                title="Top Governorates by Revenue",
                labels={
                    "governorate":
                        "Governorate",
                    "total_revenue":
                        "Revenue (TND)",
                    "region":
                        "Region",
                },
            )

            style_chart(
                governorate_fig
            )

            st.plotly_chart(
                governorate_fig,
                use_container_width=True,
            )

        # ----------------------------------------------------
        # Revenue vs profit
        # ----------------------------------------------------

        with geo2:

            regional_scatter = px.scatter(
                governorate_summary,
                x="total_revenue",
                y="total_profit",
                size="total_orders",
                color="region",
                hover_name="governorate",
                title="Revenue vs Profit by Governorate",
                labels={
                    "total_revenue":
                        "Revenue (TND)",
                    "total_profit":
                        "Profit (TND)",
                    "total_orders":
                        "Orders",
                },
            )

            style_chart(
                regional_scatter
            )

            st.plotly_chart(
                regional_scatter,
                use_container_width=True,
            )

        # ----------------------------------------------------
        # Segment orders
        # ----------------------------------------------------

        segment_orders = pd.DataFrame(
            {
                "Segment": [
                    "Consumer",
                    "Corporate",
                    "Home Office",
                ],
                "Orders": [
                    governorate_summary[
                        "consumer_orders"
                    ].sum(),
                    governorate_summary[
                        "corporate_orders"
                    ].sum(),
                    governorate_summary[
                        "home_office_orders"
                    ].sum(),
                ],
            }
        )

        regional_segment_fig = px.pie(
            segment_orders,
            names="Segment",
            values="Orders",
            hole=0.45,
            title="Order Distribution by Customer Segment",
        )

        style_chart(
            regional_segment_fig
        )

        st.plotly_chart(
            regional_segment_fig,
            use_container_width=True,
        )

        with st.expander(
            "View Regional Data"
        ):
            st.dataframe(
                governorate_summary.sort_values(
                    "total_revenue",
                    ascending=False,
                ),
                use_container_width=True,
                hide_index=True,
            )

    # ========================================================
    # PRODUCTS
    # ========================================================

    with product_tab:

        st.subheader(
            "Product Performance"
        )

        product_filter1, product_filter2 = (
            st.columns(2)
        )

        categories = sorted(
            product_df["category"]
            .dropna()
            .unique()
            .tolist()
        )

        selected_category = (
            product_filter1.selectbox(
                "Product Category",
                ["All"] + categories,
            )
        )

        product_metric = (
            product_filter2.selectbox(
                "Rank Products By",
                [
                    "Revenue",
                    "Profit",
                    "Units Sold",
                ],
            )
        )

        products_view = product_df.copy()

        if selected_category != "All":
            products_view = products_view[
                products_view["category"]
                == selected_category
            ]

        metric_map = {
            "Revenue": "total_revenue",
            "Profit": "total_profit",
            "Units Sold": "total_units_sold",
        }

        selected_metric = metric_map[
            product_metric
        ]

        top_products = (
            products_view
            .nlargest(
                15,
                selected_metric,
            )
            .sort_values(
                selected_metric
            )
        )

        product1, product2 = st.columns(2)

        # ----------------------------------------------------
        # Product ranking
        # ----------------------------------------------------

        with product1:

            product_bar = px.bar(
                top_products,
                x=selected_metric,
                y="product_name",
                color="category",
                orientation="h",
                title=(
                    f"Top Products by "
                    f"{product_metric}"
                ),
                hover_data=[
                    "sub_category",
                    "total_units_sold",
                    "total_revenue",
                    "total_profit",
                    "avg_profit_margin",
                ],
            )

            style_chart(product_bar)

            st.plotly_chart(
                product_bar,
                use_container_width=True,
            )

        # ----------------------------------------------------
        # Product efficiency
        # ----------------------------------------------------

        with product2:

            product_scatter = px.scatter(
                products_view,
                x="total_units_sold",
                y="total_revenue",
                size="total_units_sold",
                color="category",
                hover_name="product_name",
                hover_data=[
                    "sub_category",
                    "total_profit",
                    "avg_profit_margin",
                ],
                title="Units Sold vs Revenue",
                labels={
                    "total_units_sold":
                        "Units Sold",
                    "total_revenue":
                        "Revenue (TND)",
                },
            )

            style_chart(
                product_scatter
            )

            st.plotly_chart(
                product_scatter,
                use_container_width=True,
            )

        # ----------------------------------------------------
        # Category profitability
        # ----------------------------------------------------

        category_summary = (
            products_view
            .groupby(
                "category",
                as_index=False,
            )
            .agg(
                revenue=(
                    "total_revenue",
                    "sum",
                ),
                profit=(
                    "total_profit",
                    "sum",
                ),
                units=(
                    "total_units_sold",
                    "sum",
                ),
            )
        )

        category_summary[
            "profit_margin"
        ] = (
            category_summary["profit"]
            / category_summary["revenue"]
            * 100
        )

        category_fig = px.bar(
            category_summary,
            x="category",
            y="profit",
            title="Profit Contribution by Category",
            hover_data=[
                "revenue",
                "units",
                "profit_margin",
            ],
            labels={
                "category": "Category",
                "profit": "Profit (TND)",
            },
        )

        style_chart(category_fig)

        st.plotly_chart(
            category_fig,
            use_container_width=True,
        )

        with st.expander(
            "View Product Data"
        ):
            st.dataframe(
                products_view.sort_values(
                    selected_metric,
                    ascending=False,
                ),
                use_container_width=True,
                hide_index=True,
            )

    # ========================================================
    # CUSTOMERS
    # ========================================================

    with customer_tab:

        st.subheader(
            "Customer Intelligence"
        )

        # ----------------------------------------------------
        # Filters
        # ----------------------------------------------------

        filter1, filter2, filter3 = (
            st.columns(3)
        )

        segments = sorted(
            customer_df["segment"]
            .dropna()
            .unique()
            .tolist()
        )

        value_tiers = sorted(
            customer_df[
                "customer_value_tier"
            ]
            .dropna()
            .unique()
            .tolist()
        )

        loyalty_tiers = sorted(
            customer_df[
                "customer_loyalty_tier"
            ]
            .dropna()
            .unique()
            .tolist()
        )

        selected_segment = (
            filter1.selectbox(
                "Customer Segment",
                ["All"] + segments,
            )
        )

        selected_value = (
            filter2.selectbox(
                "Value Tier",
                ["All"] + value_tiers,
            )
        )

        selected_loyalty = (
            filter3.selectbox(
                "Loyalty Tier",
                ["All"] + loyalty_tiers,
            )
        )

        customers_view = customer_df.copy()

        if selected_segment != "All":
            customers_view = customers_view[
                customers_view["segment"]
                == selected_segment
            ]

        if selected_value != "All":
            customers_view = customers_view[
                customers_view[
                    "customer_value_tier"
                ]
                == selected_value
            ]

        if selected_loyalty != "All":
            customers_view = customers_view[
                customers_view[
                    "customer_loyalty_tier"
                ]
                == selected_loyalty
            ]

        if customers_view.empty:

            st.warning(
                "No customers match these filters."
            )

        else:

            # ------------------------------------------------
            # Customer KPIs
            # ------------------------------------------------

            customer_kpi1, customer_kpi2, \
                customer_kpi3, customer_kpi4 = (
                    st.columns(4)
                )

            customer_kpi1.metric(
                "Customers",
                f"{customers_view['customer_id'].nunique():,.0f}",
            )

            customer_kpi2.metric(
                "Customer Spend",
                money(
                    customers_view[
                        "total_spent"
                    ].sum()
                ),
            )

            customer_kpi3.metric(
                "Generated Profit",
                money(
                    customers_view[
                        "total_profit_generated"
                    ].sum()
                ),
            )

            customer_kpi4.metric(
                "Avg Order Value",
                money(
                    customers_view[
                        "avg_order_value"
                    ].mean()
                ),
            )

            # ------------------------------------------------
            # Best customers
            # ------------------------------------------------

            customer1, customer2 = (
                st.columns(2)
            )

            with customer1:

                best_customers = (
                    customers_view
                    .nlargest(
                        15,
                        "total_profit_generated",
                    )
                    .sort_values(
                        "total_profit_generated"
                    )
                )

                best_customer_fig = px.bar(
                    best_customers,
                    x="total_profit_generated",
                    y="customer_name",
                    color="segment",
                    orientation="h",
                    title="Most Profitable Customers",
                    hover_data=[
                        "region",
                        "governorate",
                        "total_spent",
                        "total_orders",
                        "avg_order_value",
                        "customer_value_tier",
                        "customer_loyalty_tier",
                    ],
                    labels={
                        "customer_name":
                            "Customer",
                        "total_profit_generated":
                            "Profit (TND)",
                        "segment":
                            "Segment",
                    },
                )

                style_chart(
                    best_customer_fig
                )

                st.plotly_chart(
                    best_customer_fig,
                    use_container_width=True,
                )

            # ------------------------------------------------
            # Spend vs profit
            # ------------------------------------------------

            with customer2:

                customer_scatter = px.scatter(
                    customers_view,
                    x="total_spent",
                    y="total_profit_generated",
                    size="total_orders",
                    color="segment",
                    hover_name="customer_name",
                    hover_data=[
                        "region",
                        "governorate",
                        "avg_order_value",
                        "total_units_bought",
                        "product_variety",
                        "customer_value_tier",
                        "customer_loyalty_tier",
                    ],
                    title="Customer Spend vs Profit",
                    labels={
                        "total_spent":
                            "Total Spend (TND)",
                        "total_profit_generated":
                            "Profit Generated (TND)",
                        "total_orders":
                            "Orders",
                    },
                )

                style_chart(
                    customer_scatter
                )

                st.plotly_chart(
                    customer_scatter,
                    use_container_width=True,
                )

            # ------------------------------------------------
            # Segment summary
            # ------------------------------------------------

            segment_summary = (
                customers_view
                .groupby(
                    "segment",
                    as_index=False,
                )
                .agg(
                    customers=(
                        "customer_id",
                        "nunique",
                    ),
                    total_spent=(
                        "total_spent",
                        "sum",
                    ),
                    total_profit=(
                        "total_profit_generated",
                        "sum",
                    ),
                    avg_orders=(
                        "total_orders",
                        "mean",
                    ),
                    avg_order_value=(
                        "avg_order_value",
                        "mean",
                    ),
                )
            )

            segment_profit_fig = px.bar(
                segment_summary,
                x="segment",
                y="total_profit",
                color="segment",
                title="Profit Contribution by Customer Segment",
                hover_data=[
                    "customers",
                    "total_spent",
                    "avg_orders",
                    "avg_order_value",
                ],
                labels={
                    "segment":
                        "Customer Segment",
                    "total_profit":
                        "Profit (TND)",
                },
            )

            style_chart(
                segment_profit_fig
            )

            st.plotly_chart(
                segment_profit_fig,
                use_container_width=True,
            )

            # ------------------------------------------------
            # Loyalty / value tier
            # ------------------------------------------------

            tier1, tier2 = st.columns(2)

            with tier1:

                value_summary = (
                    customers_view
                    .groupby(
                        "customer_value_tier",
                        as_index=False,
                    )
                    .agg(
                        customers=(
                            "customer_id",
                            "nunique",
                        )
                    )
                )

                value_fig = px.pie(
                    value_summary,
                    names="customer_value_tier",
                    values="customers",
                    hole=0.45,
                    title="Customers by Value Tier",
                )

                style_chart(value_fig)

                st.plotly_chart(
                    value_fig,
                    use_container_width=True,
                )

            with tier2:

                loyalty_summary = (
                    customers_view
                    .groupby(
                        "customer_loyalty_tier",
                        as_index=False,
                    )
                    .agg(
                        customers=(
                            "customer_id",
                            "nunique",
                        )
                    )
                )

                loyalty_fig = px.pie(
                    loyalty_summary,
                    names="customer_loyalty_tier",
                    values="customers",
                    hole=0.45,
                    title="Customers by Loyalty Tier",
                )

                style_chart(
                    loyalty_fig
                )

                st.plotly_chart(
                    loyalty_fig,
                    use_container_width=True,
                )

            # ------------------------------------------------
            # Customer table
            # ------------------------------------------------

            st.subheader(
                "Customer Details"
            )

            st.dataframe(
                customers_view[
                    [
                        "customer_name",
                        "segment",
                        "region",
                        "governorate",
                        "customer_value_tier",
                        "customer_loyalty_tier",
                        "total_orders",
                        "total_spent",
                        "avg_order_value",
                        "total_profit_generated",
                        "avg_discount_used",
                        "total_units_bought",
                        "product_variety",
                        "customer_lifespan_days",
                        "last_order_date",
                    ]
                ].sort_values(
                    "total_profit_generated",
                    ascending=False,
                ),
                use_container_width=True,
                hide_index=True,
            )


except Exception as error:

    st.error(
        "Unable to query DuckLake"
    )

    st.exception(error)