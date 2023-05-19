def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.ref('stg_product')
    df_new = df.select(df.itm_id, df.itm_desc, df.category)
    return df_new