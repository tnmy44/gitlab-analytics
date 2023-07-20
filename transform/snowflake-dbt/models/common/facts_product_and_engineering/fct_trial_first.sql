{{ config(
    tags=["mnpi_exception", "product"]
) }}


WITH first_trial AS (

  SELECT DISTINCT 

    dim_order_id,
    dim_namespace_id,
    customer_id,
    FIRST_VALUE(subscription_name_slugify) IGNORE NULLS
      OVER (PARTITION BY dim_order_id ORDER BY order_updated_at ASC)        AS first_subscription_name_slugify,
    FIRST_VALUE(customer_id)
      OVER (PARTITION BY dim_order_id ORDER BY order_updated_at DESC)       AS latest_customer_id,
    FIRST_VALUE(dim_namespace_id)
      OVER (PARTITION BY dim_order_id ORDER BY order_updated_at DESC)       AS latest_namespace_id,
    FIRST_VALUE(user_id)
      OVER (PARTITION BY dim_order_id ORDER BY order_updated_at DESC)        AS latest_user_id,
    FIRST_VALUE(namespace_type)
      OVER (PARTITION BY dim_order_id ORDER BY order_updated_at DESC)       AS latest_namespace_type,
    namespace_created_at,
    user_created_at,
    order_created_at,
    order_updated_at,
    trial_start_date,
    trial_end_date,
    is_trial_converted

  FROM {{ ref('fct_trial') }}
  WHERE trial_start_date >= '2019-09-01'

), final AS (

  SELECT 

   --Primary Key-- 
     {{ dbt_utils.surrogate_key(['dim_order_id', 'latest_namespace_id']) }} AS trial_first_pk,

   --Natural Key--
    dim_order_id, 

    --Foreign Keys--
    latest_namespace_id                                                     AS dim_namespace_id,
    latest_customer_id                                                      AS customer_id,
    latest_user_id                                                          AS user_id, 
       
    --Other Attributes                                                                                           
    first_subscription_name_slugify                                         AS subscription_name_slugify, 
    latest_namespace_type                                                   AS namespace_type,
    is_trial_converted,
    IFF(latest_user_id IS NOT NULL, TRUE, FALSE)                            AS is_gitlab_user,
    MIN(user_created_at)                                                    AS user_created_at,
    MIN(namespace_created_at)                                               AS namespace_created_at,  
    MIN(order_created_at)                                                   AS order_created_at,
    MIN(trial_start_date)                                                   AS trial_start_date,
    MAX(trial_end_date)                                                     AS trial_end_date
    
  FROM first_trial
 {{dbt_utils.group_by(9)}}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-07-18",
    updated_date="2023-07-18"
) }}
