{% macro model_generation (source_name,table,record_id=none) %}


    {{log ("--------enter-----")}}
    {{log ("-------------")}}
    {{log (target.name)}}
    {{log ("-------------")}}
    select
        {{
            dbt_utils.star(
                source(source_name, table), except=["ID"], quote_identifiers=False
            )
        }},
        {%- if target.name == 'default' -%}
        concat('DBT_','{{ record_id }}') as src_name,----instead of record id we can also pass the column name to concate the value of that column to the DBT_ 
                                                    ----eg: select concat('c_custey',c_custkey) as new_col from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;
        {%- elif target.name == 'prod' -%}
        concat('DBT_', 'prod') as src_name,
        {%- endif -%}
        'Processed' as prc_text,
        current_timestamp() as create_dt,
        current_timestamp() as update_dt

    from {{source( source_name, table )}}
 
    
    {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            -- (uses > to include records whose timestamp occurred since the last run of this model)
            where create_dt > (select max(create_dt) from {{ this }})

    {% endif %}


{% endmacro %}


-----Audit table updation post the model run is completed
-- create table HAREESH_DBT.AUDIT_LOG.PIPL_STAT(
-- record INT AUTOINCREMENT,
-- name varchar(257),
-- Status varchar(257),
-- Create_DT timestamp,
-- Update_DT timestamp
-- );



{% macro audit_log_insert(model) %}
    {% set query %}
        INSERT INTO HAREESH_DBT.AUDIT_LOG.PIPL_STAT (NAME, STATUS, CREATE_DT, UPDATE_DT)
        VALUES ('{{ this.name }}', 'Model_execution_started', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
    {% endset %}

  {% do run_query(query) %}


{% endmacro %}

{% macro audit_log_update(model) %}

    {% set query %}
        update HAREESH_DBT.AUDIT_LOG.PIPL_STAT set update_dt = current_timestamp, status = 'Model_execution_completed' where record = (
            select max(record) from HAREESH_DBT.AUDIT_LOG.PIPL_STAT where name = '{{ this.name }}');
    {% endset %}

  {% do run_query(query) %}

{% endmacro %}



