import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from include.helpers.ducklake_init import attach_ducklake_and_set_secrets


st.set_page_config(
    page_title="Tunisia Online Sales Lakehouse Dashboard",
    layout="wide",
)


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
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        ducklake_name=os.getenv("DUCKLAKE_NAME", "mahdi_ducklake"),
        data_path=os.getenv("DATA_PATH", "s3://lakehouse-project/"),
        postgres_secret_name=os.getenv(
            "DUCKDB_SECRET",
            "__default_postgres",
        ),
    )

    return conn


def load_monthly_sales(conn, catalog_name: str) -> pd.DataFrame:
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
            revenue_mom_pct
        FROM "{catalog_name}".gold.sales_monthly_kpis
        ORDER BY order_year, order_month
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

    return df


def load_regional_sales(conn, catalog_name: str) -> pd.DataFrame:
    return conn.execute(
        f"""
        SELECT
            governorate,
            SUM(total_revenue) AS total_revenue,
            SUM(total_profit) AS total_profit,
            SUM(total_orders) AS total_orders,
            SUM(unique_customers) AS unique_customers
        FROM "{catalog_name}".gold.region_kpis
        GROUP BY governorate
        ORDER BY total_revenue DESC
        """
    ).df()


def load_top_products(conn, catalog_name: str) -> pd.DataFrame:
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
        ORDER BY total_revenue DESC
        LIMIT 10
        """
    ).df()


def load_customer_segments(conn, catalog_name: str) -> pd.DataFrame:
    return conn.execute(
        f"""
        SELECT
            customer_value_tier,
            COUNT(*) AS customer_count,
            SUM(total_spent) AS total_spent,
            SUM(total_profit_generated) AS total_profit
        FROM "{catalog_name}".gold.customer_kpis
        GROUP BY customer_value_tier
        ORDER BY total_spent DESC
        """
    ).df()


st.title("Tunisia Sales Lakehouse")

try:
    conn = get_connection()
    catalog_name = os.getenv("DUCKLAKE_NAME", "mahdi_ducklake")

    monthly_df = load_monthly_sales(conn, catalog_name)
    regional_df = load_regional_sales(conn, catalog_name)
    product_df = load_top_products(conn, catalog_name)
    customer_df = load_customer_segments(conn, catalog_name)

    st.success("Connected successfully to DuckLake")

    if monthly_df.empty:
        st.warning("The monthly Gold table is empty.")
        st.stop()

    total_revenue = monthly_df["monthly_revenue"].sum()
    total_profit = monthly_df["monthly_profit"].sum()
    total_orders = monthly_df["monthly_orders"].sum()
    latest_active_customers = monthly_df.iloc[-1]["active_customers"]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total revenue", f"{total_revenue:,.2f} TND")
    col2.metric("Total profit", f"{total_profit:,.2f} TND")
    col3.metric("Total orders", f"{total_orders:,.0f}")
    col4.metric("Latest active customers", f"{latest_active_customers:,.0f}")

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Monthly Trends",
            "Regional Performance",
            "Products",
            "Customers",
        ]
    )

    with tab1:
        revenue_figure = px.line(
            monthly_df,
            x="period",
            y=["monthly_revenue", "monthly_profit"],
            markers=True,
            title="Monthly Revenue and Profit",
            labels={
                "period": "Month",
                "value": "Amount (TND)",
                "variable": "Metric",
            },
        )

        st.plotly_chart(
            revenue_figure,
            use_container_width=True,
        )

        st.dataframe(
            monthly_df,
            use_container_width=True,
        )

    with tab2:
        regional_figure = px.bar(
            regional_df,
            x="governorate",
            y="total_revenue",
            title="Revenue by Governorate",
            labels={
                "governorate": "Governorate",
                "total_revenue": "Revenue (TND)",
            },
        )

        st.plotly_chart(
            regional_figure,
            use_container_width=True,
        )

        st.dataframe(
            regional_df,
            use_container_width=True,
        )

    with tab3:
        product_figure = px.bar(
            product_df,
            x="total_revenue",
            y="product_name",
            orientation="h",
            title="Top 10 Products by Revenue",
            labels={
                "product_name": "Product",
                "total_revenue": "Revenue (TND)",
            },
        )

        st.plotly_chart(
            product_figure,
            use_container_width=True,
        )

        st.dataframe(
            product_df,
            use_container_width=True,
        )

    with tab4:
        customer_figure = px.pie(
            customer_df,
            names="customer_value_tier",
            values="customer_count",
            title="Customers by Value Tier",
        )

        st.plotly_chart(
            customer_figure,
            use_container_width=True,
        )

        st.dataframe(
            customer_df,
            use_container_width=True,
        )

except Exception as error:
    st.error("Unable to query DuckLake")
    st.exception(error)