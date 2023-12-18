{% macro model_generation (table,record_id=none) %}

    select 
        *,
        concat('DBT_','{{ record_id }}') as src_name,
        'Processed' as prc_text,
        current_timestamp() as create_dt,
        current_timestamp() as update_dt
    from {{source('snowflake_model',table)}}
    {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            -- (uses > to include records whose timestamp occurred since the last run of this model)
            where create_dt > (select max(create_dt) from {{ this }})

    {% endif %}


{% endmacro %}

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



