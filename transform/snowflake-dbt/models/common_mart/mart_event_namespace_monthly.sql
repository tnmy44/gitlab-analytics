{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('fct_event_namespace_monthly', 'fct_event_namespace_monthly'),
    ('dim_date', 'dim_date')
    ])
}},

fact AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_event_namespace_monthly'), except=["CREATED_BY",
        "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_event_namespace_monthly
  
), 

fact_with_dims AS (

  SELECT
    --Primary Key 
    event_namespace_monthly_pk,
                                          
    --Foreign Keys
    dim_ultimate_parent_namespace_id,
    dim_latest_product_tier_id,
    dim_latest_subscription_id,
    dim_crm_account_id,
    dim_billing_account_id,
    
    --Namespace and plan metadata
    plan_id_at_event_month,
    plan_name_at_event_month,
    plan_was_paid_at_event_month,
    dim_namespace.namespace_type           AS ultimate_parent_namespace_type,
    dim_namespace.namespace_is_internal,
    dim_namespace.namespace_creator_is_blocked,
    dim_namespace.created_at               AS namespace_created_at,
    CAST(dim_namespace.created_at AS DATE) AS namespace_created_date,

    --Date information
    event_calendar_month,
    dim_date.quarter_name                  AS event_calendar_quarter,
    dim_date.year_actual                   AS event_calendar_year,

    --Event information
    event_name,
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_umau,
    data_source,
    event_count,
    user_count,
    event_date_count
  FROM fact
  LEFT JOIN dim_namespace
    ON fact.dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id
  LEFT JOIN dim_date
    ON fact.event_calendar_month = dim_date.date_actual --join on first day of calendar month
  WHERE fact.event_calendar_month < DATE_TRUNC('month', CURRENT_DATE) --exclude current month/incomplete data
        
)

{{ dbt_audit(
    cte_ref="fact_with_dims",
    created_by="@cbraza",
    updated_by="@michellecooper",
    created_date="2023-02-14",
    updated_date="2023-05-12"
) }}
