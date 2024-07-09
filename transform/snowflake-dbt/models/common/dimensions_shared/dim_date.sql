{{ config({
    "alias": "dim_date"
}) }}

WITH dates AS (

  SELECT
    {{ dbt_utils.star(
           from=ref('prep_date'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
  FROM {{ ref('prep_date') }}

)

{{ dbt_audit(
    cte_ref="null",
    created_by="null",
    updated_by="null",
    created_date="null",
    updated_date="null"
) }}