def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.source("sales_data", "sales")
    df_new = df.select(df.TXN_ID
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
