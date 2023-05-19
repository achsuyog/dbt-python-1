import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col


def model(dbt, session):
    # dbt.config(materialized='table')
    df = dbt.ref('stg_customers')
    df_new = df.select(df.customer_id, df.customer_name, df.customer_type)
    return df_new

# def model(dbt, session):
#     dbt.config(materialized='incremental')
#     df = dbt.ref('stg_customers')
#     df_new = df.filter(df.customer_type == 'Non-food Retailer (NFR)')
#     max_cus_key_sql = "select max(cus_key)"
#     df_new = df_new.select((f.seq2(0) + 1).alias("customer_key"), df_new.customer_name,
#                            df.customer_type)
#     return df_new
