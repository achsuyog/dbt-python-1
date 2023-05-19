import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col


def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.ref('stg_soh')
    df_store = dbt.ref('tgt_store')
    df_product = dbt.ref('tgt_product')
    df_new = df.join(df_store, df_store.store == df.store_id).join(df_product, df_product.itm_id == df.itm_id).select(
        df_product.itm_key, df_store.store_key, df.day_id.alias("day_key"), df.end_day_id.alias("end_day_key"),
        df.store_id,
        df.day_id,
        df.end_day_id,
        df.f_unit_prc,
        df.f_oh_qty)
    return df_new
