{{ config(
  materialized='table'
) }}


WITH dim_marketing_contact AS (
  SELECT
    gitlab_dotcom_user_id,
    email_address,
    job_title,
    sfdc_parent_sales_segment,
    marketo_sales_segmentation,
    gitlab_dotcom_last_login_date,
    is_marketo_opted_in,
    is_sfdc_opted_out,
    gitlab_dotcom_email_opted_in
  FROM {{ ref('dim_marketing_contact') }}
),

prep_user AS (
  SELECT
    user_id AS gitlab_dotcom_user_id,
    highest_paid_subscription_plan_id,
    account_age,
    is_admin
  FROM {{ ref('prep_user') }}
),

gitlab_dotcom_plans_source AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_plans_source') }}
),

prep_user_plan AS (
  SELECT
    prep_user.*,
    gitlab_dotcom_plans_source.plan_name
  FROM prep_user
  LEFT JOIN gitlab_dotcom_plans_source
    ON prep_user.highest_paid_subscription_plan_id = gitlab_dotcom_plans_source.plan_id
),

dim_marketing_contact_cleaned AS (
  SELECT
    gitlab_dotcom_user_id,
    email_address,
    job_title,
    COALESCE(sfdc_parent_sales_segment, marketo_sales_segmentation) AS sales_segment,
    gitlab_dotcom_last_login_date,
    is_marketo_opted_in,
    is_sfdc_opted_out,
    gitlab_dotcom_email_opted_in
  FROM dim_marketing_contact
),

joined AS (
  SELECT
    pu.plan_name,
    dmc.gitlab_dotcom_user_id,
    dmc.email_address,
    dmc.job_title,
    pu.account_age,
    pu.is_admin,
    dmc.sales_segment,
    dmc.gitlab_dotcom_last_login_date,
    dmc.is_marketo_opted_in,
    dmc.is_sfdc_opted_out,
    dmc.gitlab_dotcom_email_opted_in,
    NOT COALESCE (dmc.is_marketo_opted_in = FALSE OR dmc.is_sfdc_opted_out = TRUE OR dmc.gitlab_dotcom_email_opted_in = FALSE, FALSE) AS is_opted_in
  FROM dim_marketing_contact_cleaned AS dmc
  LEFT JOIN prep_user_plan AS pu
    ON dmc.gitlab_dotcom_user_id = pu.gitlab_dotcom_user_id
)

SELECT *
FROM joined
