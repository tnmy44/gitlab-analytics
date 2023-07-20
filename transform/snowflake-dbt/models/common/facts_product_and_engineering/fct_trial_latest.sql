{{ config(
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_trial', 'fct_trial'),
    ('gitlab_subscriptions','gitlab_dotcom_gitlab_subscriptions_snapshots_base')


]) }}


, latest_trial_per_namespace AS (
  
    SELECT *
    FROM fct_trial
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(dim_namespace_id) ORDER BY order_updated_at DESC, dim_order_id DESC) = 1

)
, estimated_latest_trial_dates AS (
                                     
    SELECT 
      namespace_id, 
      MAX(gitlab_subscription_trial_ends_on)                      AS latest_trial_end_date,
      DATEADD('day', -30, MAX(gitlab_subscription_trial_ends_on)) AS estimated_latest_trial_start_date
    FROM gitlab_subscriptions
    WHERE gitlab_subscription_trial_ends_on IS NOT NULL
    GROUP BY 1

)

, trials_joined AS (

    SELECT
      estimated_latest_trial_dates.namespace_id,
      COALESCE(latest_trial_per_namespace.trial_start_date, 
               estimated_latest_trial_dates.estimated_latest_trial_start_date) AS latest_trial_start_date,
      estimated_latest_trial_dates.latest_trial_end_date,
      latest_trial_per_namespace.subscription_start_date,
      latest_trial_per_namespace.customer_id,
      latest_trial_per_namespace.country,
      latest_trial_per_namespace.company_size,
      latest_trial_per_namespace.user_id,
      latest_trial_per_namespace.is_gitlab_user,
      latest_trial_per_namespace.user_created_at,
      latest_trial_per_namespace.namespace_created_at,
      latest_trial_per_namespace.namespace_type

      
    FROM estimated_latest_trial_dates
    LEFT JOIN latest_trial_per_namespace 
      ON estimated_latest_trial_dates.namespace_id = latest_trial_per_namespace.dim_namespace_id


), joined AS (
  
    SELECT
      trials_joined.namespace_id                             AS dim_namespace_id,
      trials_joined.customer_id,
      trials_joined.country,
      trials_joined.company_size,
        
      trials_joined.user_id                                  AS user_id,
      trials_joined.is_gitlab_user                           AS is_gitlab_user,
      trials_joined.user_created_at                          AS user_created_at,
      
      trials_joined.namespace_created_at                     AS namespace_created_at,
      trials_joined.namespace_type,
      
      trials_joined.latest_trial_start_date, 
      trials_joined.latest_trial_end_date,
      MIN(trials_joined.subscription_start_date)             AS subscription_start_date  

    FROM trials_joined
    {{dbt_utils.group_by(11)}} 
  
), final AS (

  SELECT 
   --Primary Key-- 
     {{ dbt_utils.surrogate_key(['dim_namespace_id']) }} AS trial_latest_pk,

   --Natural Key--
    dim_namespace_id,

    --Foreign Keys--
    customer_id,
    user_id,
       
    --Other Attributes                                                                                           
    is_gitlab_user,
    user_created_at,
    
    country,
    company_size, 
    namespace_created_at,
    namespace_type,
   
    subscription_start_date, 
    latest_trial_start_date, 
    latest_trial_end_date
    
  FROM joined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-07-19",
    updated_date="2023-07-19"
) }}
