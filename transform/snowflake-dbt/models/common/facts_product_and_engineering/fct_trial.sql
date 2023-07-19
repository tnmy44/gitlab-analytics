{{ config(
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('customers', 'customers_db_customers_source'),
    ('namespaces', 'gitlab_dotcom_namespaces'),
    ('orders_snapshots', 'customers_db_orders_snapshots_base'),
    ('users', 'gitlab_dotcom_users')


]) }}


, trials AS (

  SELECT
    *
  FROM orders_snapshots
  WHERE order_is_trial = TRUE


), zuora_subscription_with_positive_mrr_tcv AS (
  
  SELECT DISTINCT
    subscription_name_slugify,
    subscription_start_date
  FROM {{ ref('zuora_base_mrr') }}

  
), ci_minutes_charges AS (
  
  SELECT 
    *
  FROM {{ ref('zuora_rate_plan') }}
  WHERE rate_plan_name = '1,000 CI Minutes'

  
), orders_shapshots_excluding_ci_minutes AS (

  SELECT DISTINCT
   orders_snapshots.order_id                                                                                     AS order_id,
   orders_snapshots.subscription_name_slugify                                                                    AS subscription_name_slugify
  FROM orders_snapshots
  LEFT JOIN ci_minutes_charges
    ON orders_snapshots.subscription_id = ci_minutes_charges.subscription_id
    AND orders_snapshots.product_rate_plan_id = ci_minutes_charges.product_rate_plan_id
  WHERE ci_minutes_charges.subscription_id IS NULL

), converted_trials AS (

  SELECT DISTINCT
    trials.order_id                                                                                              AS order_id,
    orders_shapshots_excluding_ci_minutes.subscription_name_slugify                                              AS subscription_name_slugify,
    zuora_subscription_with_positive_mrr_tcv.subscription_start_date                                             AS subscription_start_date
  FROM trials
  INNER JOIN orders_shapshots_excluding_ci_minutes
    ON trials.order_id = orders_shapshots_excluding_ci_minutes.order_id
  INNER JOIN zuora_subscription_with_positive_mrr_tcv AS subscription
    ON orders_shapshots_excluding_ci_minutes.subscription_name_slugify = subscription.subscription_name_slugify
      AND trials.order_start_date <= subscription.subscription_start_date
  WHERE orders_shapshots_excluding_ci_minutes.subscription_name_slugify IS NOT NULL



), joined AS (
  
  SELECT DISTINCT

    trials.order_id                                                                                             AS dim_order_id, 
    trials.gitlab_namespace_id                                                                                  AS dim_namespace_id,
    trials.product_rate_plan_id                                                                                 AS dim_product_rate_plan_id,
    customers.customer_id                                                                                       AS customer_id,
    users.user_id                                                                                               AS user_id,                                                                                         
    IFF(users.user_id IS NOT NULL, TRUE, FALSE)                                                                 AS is_gitlab_user,
    users.created_at                                                                                            AS user_created_at,
    namespaces.created_at                                                                                       AS namespace_created_at,
    namespaces.namespace_type                                                                                   AS namespace_type,
    IFF(converted_trials.order_id IS NOT NULL, TRUE, FALSE)                                                     AS is_trial_converted,
    converted_trials.subscription_name_slugify                                                                  AS subscription_name_slugify,
    converted_trials.subscription_start_date                                                                    AS subscription_start_date,   
    trials.order_created_at                                                                                     AS order_created_at,
    trials.order_updated_at                                                                                     AS order_updated_at,
    (trials.order_start_date)::DATE                                                                             AS trial_start_date, 
    (trials.order_end_date)::DATE                                                                               AS trial_end_date,
    customers.country                                                                                           AS country,
    customers.company_size                                                                                      AS company_size
    
    
  FROM trials
    INNER JOIN customers 
      ON trials.customer_id = customers.customer_id
    LEFT JOIN namespaces 
      ON trials.gitlab_namespace_id = namespaces.namespace_id
    LEFT JOIN users 
      ON customers.customer_provider_user_id = users.user_id
    LEFT JOIN converted_trials 
      ON trials.order_id = converted_trials.order_id
  
  WHERE trial_start_date IS NOT NULL 
  
), final AS (

  SELECT 
   --Primary Key-- 
     {{ dbt_utils.surrogate_key(['joined.dim_order_id', 'joined.dim_namespace_id', 'joined.dim_product_rate_plan_id', 'joined.trial_start_date', 'joined.trial_end_date', 'joined.subscription_name_slugify', 'joined.order_updated_at']) }} AS trial_pk,

   --Natural Key--
    dim_order_id, 

    --Foreign Keys--
    dim_namespace_id,
    dim_product_rate_plan_id,
    customer_id,
    user_id,
       
    --Other Attributes                                                                                           
    is_gitlab_user,
    user_created_at,
    
    namespace_created_at,
    namespace_type,
  
    is_trial_converted,
    subscription_name_slugify, 
    subscription_start_date,
    country,
    company_size 

    order_created_at,
    order_updated_at,
    trial_start_date, 
    trial_end_date
    
  FROM joined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-06-30",
    updated_date="2023-06-30"
) }}
