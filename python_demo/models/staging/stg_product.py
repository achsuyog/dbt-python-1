import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col


def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.source("product_data", "product")
    df_new = df.select(df["itm_id"].alias("itm_id"), df.itm_desc, df.category)
    return df_new
