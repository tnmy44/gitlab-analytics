{{ config(
    materialized="incremental",
    unique_key="crm_opportunity_snapshot_id"
) }}

WITH final AS (

  SELECT {{ dbt_utils.star(from=ref('wk_prep_crm_opportunity'), 
    except=["CREATED_BY", "UPDATED_BY", "MODEL_CREATED_DATE", "MODEL_UPDATED_DATE", "DBT_UPDATED_AT", "DBT_CREATED_AT", "IS_NET_ARR_PIPELINE_CREATED", "IS_ELIGIBLE_OPEN_PIPELINE", "IS_EXCLUDED_FROM_PIPELINE_CREATED", "CREATED_AND_WON_SAME_QUARTER_NET_ARR", "IS_ELIGIBLE_AGE_ANALYSIS", "CYCLE_TIME_IN_DAYS"]) }}
  FROM {{ ref('wk_prep_crm_opportunity') }}
  WHERE is_live = 0
  {% if is_incremental() %}
  
    AND snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

  {% endif %}


)