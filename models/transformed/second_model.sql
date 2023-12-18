{{
    config(
    materialized = "incremental",
    pre_hook= audit_log_insert('LINEITEM'),
    post_hook= audit_log_update('LINEITEM')
  )
}}

    
{{
    model_generation ('LINEITEM','AM')
}}


