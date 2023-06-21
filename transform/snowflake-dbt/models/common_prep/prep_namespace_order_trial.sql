{{ config(
    tags=["mnpi_exception"]
) }}


WITH trial_orders AS (

  SELECT 

    gl_namespace_id, 
    start_date,
    expired_on,
    created_at,
    updated_at,
    glm_source,
    glm_content,
    trial_entity,
    'trial_histories'                                      AS record_source

  FROM {{ ref('customers_db_trial_histories_source') }} 

  UNION

    SELECT   

      gitlab_namespace_id, 
      order_start_date, 
      order_end_date, 
      order_created_at, 
      order_updated_at, 
      order_source,
      NULL                                                 AS glm_content,
      NULL                                                 AS trial_entity,
    'orders'                                               AS record_source 

    FROM {{ ref('customers_db_orders_source') }} 

    WHERE order_is_trial = True 
      AND gitlab_namespace_id IS NOT NULL
      AND gitlab_namespace_id 
        NOT IN 
          (SELECT gl_namespace_id FROM PROD.legacy.customers_db_trial_histories)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(gitlab_namespace_id) ORDER BY Order_updated_at DESC) = 1


), final AS (
  
    SELECT 

    --Surrogate Key
    {{ dbt_utils.surrogate_key(['gl_namespace_id']) }}  AS dim_namespace_order_trial_sk, 

    --Natural Key
      gl_namespace_id                                   AS dim_namespace_id, 

    --Other Attributes  
      start_date                                        AS order_start_date,
      expired_on                                        AS order_end_date,
      created_at                                        AS order_created_at,
      updated_at                                        AS order_updated_at,
      glm_source,
      glm_content,
      trial_entity
      
    FROM trial_orders
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-06-19",
    updated_date="2023-06-19"
) }}