{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_event_valid', 'fct_event_valid')
    ])
}},

/*
Aggregate events by date and event
Limit to 24 months of history for performance reasons
*/

fct_event_instance_daily AS (
    
  SELECT
    --Primary Key
    {{ dbt_utils.surrogate_key(['event_date', 'event_name']) }} AS event_instance_daily_pk,
    
    --Foreign Keys
    
    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    event_date,
    event_name,
    data_source,
    
    --Facts
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count
  FROM fct_event_valid
  WHERE DATE_TRUNC('month', event_date) >= DATEADD('month', -24, DATE_TRUNC('month',CURRENT_DATE))
  {{ dbt_utils.group_by(n=4) }}
  
)

{{ dbt_audit(
    cte_ref="fct_event_instance_daily",
    created_by="@iweeks",
    updated_by="@cbraza",
    created_date="2022-04-09",
    updated_date="2023-02-16"
) }}
