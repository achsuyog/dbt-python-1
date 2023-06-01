{%- set source_model = "v_stg_loc" -%}
{%- set src_pk = "DISTRICT_REGION_PK" -%}
{%- set src_fk = ["DST_ID", "RGN_ID"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}