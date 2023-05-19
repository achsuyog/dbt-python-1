import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col


def model(dbt, session):
    dbt.config(materialized='table',)
    df = dbt.source("customer_data", "customer")
    df_new = df.select(df["CUSTOMERID"].alias("customer_id"), df.customer_name, df.customer_type)
    return df_new
