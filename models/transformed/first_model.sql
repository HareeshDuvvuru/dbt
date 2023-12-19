{{
    config(
    materialized = "incremental",
    pre_hook= audit_log_insert(),
    post_hook= audit_log_update()
  )
}}

    
{{
    model_generation ('source_1', 'customer', 'CLM')
}}


