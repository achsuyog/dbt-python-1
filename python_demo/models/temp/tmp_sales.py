import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col


def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.ref('stg_sales')
    df_store = dbt.ref('tgt_store')
    df_customer = dbt.ref('tgt_customers')
    df_new = df.join(df_store, df.store_id == df_store.store).join(df_customer, df.customerid ==
                                                                   df_customer.customer_id).select(
        df_store.store_key.alias("store_key"),
        df_customer.customer_key.alias("customer_key"),
        df.TXN_ID
        , df.STORE_ID
        , df.CUSTOMERID
        , df.TRAN_TYPE
        , df.INVOICE_DATE
        , df.TOTAL_REVENUE
        , df.TOTAL_DISCOUNT
        , df.TOTAL_UNITS
        , df.TOTAL_COST
        , df.TOTAL_TAX
        , df.TOTAL_BILLS
        )
    return df_new
