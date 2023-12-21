{{
    config(
    materialized = "incremental",
    pre_hook= audit_log_insert('call_center'),
    post_hook= audit_log_update('call_center')
  )
}}

    
{{
    model_generation ('TPCDS_1', 'call_center', 'AM')
}}


