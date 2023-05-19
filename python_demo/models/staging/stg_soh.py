import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col


def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.source("soh_data", "soh")
    df_new = df.select(df["itm_id"], df.loc_id.alias("store_id"), f.dateadd('day', f.lit(1), f.current_date()).alias("day_id"),
                       f.lit('9999-12-31').alias("end_day_id"),df.unit_price.alias("f_unit_prc"), df.available_stock.alias("f_oh_qty"))
    return df_new


