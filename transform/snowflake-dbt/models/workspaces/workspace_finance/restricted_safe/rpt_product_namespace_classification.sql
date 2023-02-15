-- namespace classification snippet
{{ config(
    materialized='table',
    )
}}


WITH dn AS (

  SELECT * FROM {{ ref('dim_namespace') }}

),

u as (

  SELECT * FROM {{ ref('dim_user') }}

),

hist as (

SELECT * FROM {{ ref('dim_order_hist') }}

),

pd as (

SELECT * FROM {{ ref('dim_product_detail') }} 

),

ch as (

  SELECT * FROM {{ ref('customers_db_charges_xf') }} 

),

ch as (

  SELECT * FROM {{ ref('customers_db_charges_xf') }} 

),

namespaces as (

  SELECT * FROM {{ ref('gitlab_dotcom_namespaces_xf') }} 

),

m as (

  SELECT * FROM {{ ref('mart_arr') }} 

),

ossedu_1 as (select dn.namespace_is_internal , dn.visibility_level,  dn.namespace_creator_is_blocked, dn.dim_namespace_id, dn.GITLAB_PLAN_TITLE,dn.GITLAB_PLAN_IS_PAID,  u.email_domain--, pd.PRODUCT_RATE_PLAN_CHARGE_NAME
             ,  max(iff(pd.product_rate_plan_id is not null,1,0)) as record_present 
             ,ARRAY_TO_STRING(ARRAYAGG(Distinct pd.PRODUCT_RATE_PLAN_CHARGE_NAME ),',') as product_rate_plan
             , max(IFF(pd.IS_OSS_OR_EDU_RATE_PLAN = TRUE,1,0)) as is_ossedu
             , max(iff(lower(pd.PRODUCT_RATE_PLAN_CHARGE_NAME) like '%edu%' and lower(pd.PRODUCT_RATE_PLAN_CHARGE_NAME) like '%oss%',1,0 )) as is_OSS_or_EDU
             , max(iff(lower(pd.PRODUCT_RATE_PLAN_CHARGE_NAME) like '%oss%',1,0 )) as is_OSS
             , max(iff(lower(pd.PRODUCT_RATE_PLAN_CHARGE_NAME) like '%edu%',1,0 )) as is_EDU
             , max(iff(lower(pd.PRODUCT_RATE_PLAN_CHARGE_NAME) like '%storage%',1,0 )) as is_storage
             , max(iff(lower(pd.PRODUCT_RATE_PLAN_CHARGE_NAME) like '%minutes%',1,0 )) as is_CI
             , max(iff(u.email_domain = 'gitlab.com' ,1,0 )) as is_gitlab_employee
             , max(iff(lower(pd.PRODUCT_RATE_PLAN_CHARGE_NAME) like '%combinator%' or lower(pd.PRODUCT_RATE_PLAN_CHARGE_NAME) like '%startup%' ,1,0 )) as is_startup
from dn 
left join u on u.dim_user_id = dn.creator_id 
left join hist on hist.dim_namespace_id = dn.dim_namespace_id and valid_From::date <= getdate()::Date and coalesce(valid_to,getdate())::date >= getdate()::Date -- there is 1 to many rows for this even with valid from to constraints, but only ~5% duplicate. 
left JOIN pd ON hist.product_rate_plan_id = pd.product_rate_plan_id
where  dn.NAMESPACE_IS_ULTIMATE_PARENT = TRUE									
            group by 1,2,3,4,5,6,7), 
                         


-- understand which namespace paid in the last two months and for what. 
mrr_ns as (
select  c.namespace_id
  , max( case when product_tier_name in ('Storage') then 1 else 0 end) as paid_for_storage
  , max( case when product_tier_name in ('SaaS - Bronze','SaaS - Premium','SaaS - Ultimate') then 1 else 0 end) as paid_for_plan
from (select NAMESPACE_ULTIMATE_PARENT_ID as Namespace_id,   SUBSCRIPTION_NAME_SLUGIFY , min(subscription_start_date) as sd 
      from ch
      inner JOIN namespaces ON ch.current_gitlab_namespace_id::int = namespaces.namespace_id
      where ch.CURRENT_GITLAB_NAMESPACE_ID is not null
      --and ch.product_category IN ('SaaS - Ultimate','SaaS - Premium','SaaS - Bronze')
      group by 1,2)  c
inner join  m  on c.SUBSCRIPTION_NAME_SLUGIFY = m.SUBSCRIPTION_NAME_SLUGIFY and m.product_delivery_type = 'SaaS' 
  and arr_month between dateadd(month,-2,getdate()::date )  and getdate()::date 
  -- and product_tier_name != 'Storage'
  group by 1
  )
    select a.*, case when namespace_is_internal = TRUE then 'internal' 
  when GITLAB_PLAN_IS_PAID = TRUE and coalesce(paid_for_plan,0) = 0 and is_gitlab_employee = 1 then 'internal employee'
  when   GITLAB_PLAN_IS_PAID = TRUE and paid_for_plan = 1 then 'Paid for plan'
  when  paid_for_storage = 1 then 'Paid for storage' 
  when  GITLAB_PLAN_TITLE = 'Open Source Program' then 'Open Source Program' 
  when is_ossedu = 1 or is_startup = 1 then 'OSS EDU or Startup'
  when GITLAB_PLAN_IS_PAID = FALSE and namespace_creator_is_blocked = TRUE then 'Free blocked user'
  when GITLAB_PLAN_IS_PAID = FALSE  then 'Free'
  when GITLAB_PLAN_IS_PAID = TRUE then 'Paid for plan'
  when GITLAB_PLAN_IS_PAID = FALSE then 'free others' else 'others' end as namespace_type_details, 
   case when namespace_is_internal = TRUE then 'internal' 
  when GITLAB_PLAN_IS_PAID = TRUE and coalesce(paid_for_plan,0) = 0 and is_gitlab_employee = 1 then 'internal'
  when   GITLAB_PLAN_IS_PAID = TRUE and paid_for_plan = 1 then 'Paid'
  when  paid_for_storage = 1 then 'Paid' 
  when  GITLAB_PLAN_TITLE = 'Open Source Program' then 'Free' 
  when is_ossedu = 1 or is_startup = 1 then 'Free'
  when GITLAB_PLAN_IS_PAID = FALSE and namespace_creator_is_blocked = TRUE then 'Free'
  when GITLAB_PLAN_IS_PAID = FALSE  then 'Free'
  when GITLAB_PLAN_IS_PAID = TRUE then 'Paid'
  when GITLAB_PLAN_IS_PAID = FALSE then 'free others' else 'others' end as namespace_type
  from ossedu_1 a 
  left join mrr_ns m on m.namespace_id = a.dim_namespace_id