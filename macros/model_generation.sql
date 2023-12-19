{% macro model_generation (source_name,table,record_id=none) %}
 
    {{log ("-------------")}}
    {{log (target.name)}}
    {{log ("-------------")}}
    select
        {{
            dbt_utils.star(
                source(source_name,table), except=["ID"], quote_identifiers=False
            )
        }},
        {%- if target.name == 'dev' -%}
        concat('DBT_','{{ record_id }}') as src_name,
        {%- elif target.name == 'default' -%}
        concat('DBT_') as src_name,
        {%- endif -%}
        'Processed' as prc_text,
        current_timestamp() as create_dt,
        current_timestamp() as update_dt

    from {{source(source_name,table)}}
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



