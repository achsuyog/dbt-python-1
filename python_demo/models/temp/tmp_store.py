def model(dbt, session):
    dbt.config(materialized='table')
    df = dbt.ref("stg_store")
    df_new = df.select(df.STORE
                       , df.STORE_NAME
                       , df.STORE_NAME10
                       , df.STORE_NAME3
                       , df.STORE_NAME_SECONDARY
                       , df.STORE_CLASS
                       , df.STORE_MGR_NAME
                       , df.STORE_OPEN_DATE
                       , df.STORE_CLOSE_DATE
                       , df.ACQUIRED_DATE
                       , df.REMODEL_DATE
                       , df.FAX_NUMBER
                       , df.PHONE_NUMBER
                       , df.EMAIL
                       , df.TOTAL_SQUARE_FT
                       , df.SELLING_SQUARE_FT
                       , df.LINEAR_DISTANCE
                       , df.VAT_REGION
                       , df.VAT_INCLUDE_IND
                       , df.STOCKHOLDING_IND
                       , df.CHANNEL_ID
                       , df.STORE_FORMAT
                       , df.MALL_NAME
                       , df.DISTRICT
                       , df.TRANSFER_ZONE
                       , df.DEFAULT_WH
                       , df.STOP_ORDER_DAYS
                       , df.START_ORDER_DAYS
                       , df.CURRENCY_CODE
                       , df.LANG
                       , df.TRAN_NO_GENERATED
                       , df.INTEGRATED_POS_IND
                       , df.ORIG_CURRENCY_CODE
                       , df.DUNS_NUMBER
                       , df.DUNS_LOC
                       , df.SISTER_STORE
                       , df.TSF_ENTITY_ID
                       , df.ORG_UNIT_ID
                       , df.AUTO_RCV
                       , df.REMERCH_IND
                       , df.STORE_TYPE
                       , df.WF_CUSTOMER_ID
                       , df.TIMEZONE_NAME
                       , df.CUSTOMER_ORDER_LOC_IND
                       , df.CREATE_ID
                       , df.CREATE_DATETIME
                       )
    return df_new
