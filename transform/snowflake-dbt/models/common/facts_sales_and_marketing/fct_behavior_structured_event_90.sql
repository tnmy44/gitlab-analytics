{{config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['product'],
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','day',1) }}"],
    cluster_by=['behavior_at::DATE']
  )

}}

WITH structured_event_90_days AS (
   
    SELECT *
    FROM {{ ref('fct_behavior_structured_event') }}
    WHERE behavior_at >= DATEADD('day', -90, CURRENT_DATE)
    {% if is_incremental() %}

      AND behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})

    {% endif %}


)

{{ dbt_audit(
    cte_ref="structured_event_90_days",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-05-03",
    updated_date="2024-05-03"
) }}
