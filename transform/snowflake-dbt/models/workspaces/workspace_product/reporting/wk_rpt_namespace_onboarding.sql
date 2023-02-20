{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_namespace','dim_namespace'),
    ('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan'),
	('dim_user', 'dim_user'),
	('customers_db_leads', 'customers_db_leads'),
	('gitlab_dotcom_members', 'gitlab_dotcom_members'),
	('customers_db_trial_histories', 'customers_db_trial_histories'),
	('customers_db_charges_xf', 'customers_db_charges_xf'),
	('mart_event_namespace_daily', 'mart_event_namespace_daily'),
    ('gitlab_dotcom_memberships', 'gitlab_dotcom_memberships'),
	('mart_event_valid', 'mart_event_valid'),
	('fct_usage_storage', 'fct_usage_storage')
    ])
}},

namespaces AS ( --All currently existing namespaces within Gitlab.com. Filters out namespaces with blocked creators, and internal namespaces. Filtered to ultimate parent namespaces.

    SELECT DISTINCT
      dim_namespace.ultimate_parent_namespace_id, -- Keeping this id naming convention for clarity
      dim_namespace.created_at                                    AS namespace_created_at, --timestamp is useful for relative calculations - ex) file created win 1 minute of namespace creation
      dim_namespace.created_at::DATE                              AS namespace_created_date,
      dim_namespace.owner_id,   --user_id of the owner of the namespace - useful for owner specific analysis - marketing ops and business systems have use cases
      dim_namespace.creator_id,
      dim_namespace.namespace_type, -- Not limiting to Group namespaces only to facilitate broader analyses if needed 
      dim_user.setup_for_company,
      dim_namespace.visibility_level,
      dim_namespace.gitlab_plan_id                                AS current_gitlab_plan_id,
      plan.plan_name_modified                                     AS current_gitlab_plan_title,
      dim_namespace.current_project_count,
      DATEDIFF(day, namespace_created_date, current_date)         AS days_since_namespace_creation,
      IFF(pql.namespace_id IS NOT NULL, TRUE, FALSE)              AS is_hand_raise_pql,
      IFF(TIMESTAMPDIFF(minute, members.invite_accepted_at, namespace_created_at) BETWEEN 0 AND 2, TRUE, FALSE)
                                                                  AS is_namespace_created_within_2min_of_creator_invite_acceptance --filterable field specific to growth conversion analysis
    FROM dim_namespace
    INNER JOIN prep_gitlab_dotcom_plan plan
      ON dim_namespace.gitlab_plan_id = plan.dim_plan_id
    LEFT JOIN customers_db_leads AS pql -- legacy schema
      ON pql.namespace_id = dim_namespace.dim_namespace_id
      AND pql.product_interaction = 'Hand Raise PQL' 
    LEFT JOIN dim_user 
      ON dim_user.dim_user_id = dim_namespace.creator_id
    LEFT JOIN gitlab_dotcom_members AS members -- legacy schema
      ON dim_namespace.creator_id = members.user_id
    WHERE namespace_is_internal = FALSE
      AND namespace_creator_is_blocked = FALSE
      AND namespace_is_ultimate_parent = TRUE

), trials AS ( -- Current trial data does not specify what type of trial was started
    
    SELECT DISTINCT
      namespaces.ultimate_parent_namespace_id,
      trials.start_date::DATE                                      AS trial_start_date, 
      DATEDIFF('days', namespace_created_date, trial_start_date)   AS days_since_namespace_creation_at_trial
    FROM namespaces
    INNER JOIN customers_db_trial_histories AS trials -- legacy schema
      ON namespaces.ultimate_parent_namespace_id = trials.gl_namespace_id

), charges AS ( --First paid subscription for ultimate namespace

    SELECT DISTINCT
      namespaces.ultimate_parent_namespace_id,
      charges.subscription_start_date                              AS first_paid_subscription_start_date,
      DATEDIFF('days', namespace_created_date, charges.subscription_start_date)   
                                                                   AS days_since_namespace_creation_at_first_paid_subscription,
      SPLIT_PART(charges.product_category, ' - ', 2)               AS first_paid_plan_name, --product_category: SaaS - Premium, SaaS - Ultimate, etc
      charges.is_purchased_through_subscription_portal             AS first_paid_plan_purchased_through_subscription_portal
    FROM namespaces
    INNER JOIN customers_db_charges_xf AS charges -- legacy schema
      ON namespaces.ultimate_parent_namespace_id = charges.current_gitlab_namespace_id::INT
    WHERE charges.product_category != 'Storage' --exclude storage payments
      AND charges.product_category NOT LIKE 'Self-Managed%' --exclude SM plans
      AND charges.current_gitlab_namespace_id IS NOT NULL
      AND charges.mrr > 0
    QUALIFY ROW_NUMBER() OVER(PARTITION BY namespaces.ultimate_parent_namespace_id ORDER BY charges.subscription_start_date)  = 1

), activation_events AS ( --Event part of the activation definition occuring while namespace was free within 14 days of namespace creation
   
  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    MIN(events.event_date)                                           AS event_activation_date,
    MIN(events.days_since_namespace_creation_at_event_date)          AS days_since_namespace_creation_at_activation_event,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT events.event_name) WITHIN GROUP(ORDER BY events.event_name ASC), ' , ')
                                                                     AS activation_event_array
  FROM namespaces
  INNER JOIN mart_event_namespace_daily AS events
    ON events.dim_ultimate_parent_namespace_id = namespaces.ultimate_parent_namespace_id
  WHERE lower(events.event_name) IN ('merge_request_note_creation', 'todos', 'other_ci_build_creation', 'successful_ci_pipeline_creation', 'issue_note_creation', 'merge_request_creation', 'epic_notes') -- activation events  
    AND events.days_since_namespace_creation_at_event_date BETWEEN 0 AND 13
    AND events.plan_was_paid_at_event_date = FALSE --including activity prior to conversion
  GROUP BY 1
  
), second_billable_member AS ( -- 2nd part of the activation definition - 2nd billable member added within 14 days
-- using a window function to calculate the cumulative number of new members added to a namespace within 14 days so that I can identify the # days it took for a namespace to add a 2nd user

  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    DATEDIFF(day, namespace_created_date, dim_user.created_at::DATE)   AS days_since_namespace_creation_at_2nd_user_add, --2nd user add date
    COUNT(DISTINCT dim_user.dim_user_id)                               AS count_billable_members, -- # of users added on the date of the 2nd add - could be 1 or 2 - field is not in final output
    SUM(count_billable_members) OVER (PARTITION BY namespaces.ultimate_parent_namespace_id ORDER BY days_since_namespace_creation_at_2nd_user_add)
                                                                       AS total_users_added   -- always 2 - field is not in final output    
  FROM namespaces
  INNER JOIN gitlab_dotcom_memberships AS mships -- legacy shema
    ON namespaces.ultimate_parent_namespace_id = mships.ultimate_parent_id
  INNER JOIN dim_user -- including all users with a membership to the ultimate parent regardless of creator status
    ON dim_user.dim_user_id = mships.user_id
    AND dim_user.is_blocked_user = FALSE
    AND mships.is_billable = TRUE -- Optional based on use case - excluding guests, bots, other use cases that would not be counted towards a subscription if one were to exist
  LEFT JOIN charges -- joining charges info to exclude users joining namespaces with a subscription before they were created 
    ON namespaces.ultimate_parent_namespace_id = charges.ultimate_parent_namespace_id
  WHERE IFNULL(charges.first_paid_subscription_start_date,CURRENT_DATE) >= dim_user.created_at::DATE -- excluding users joining namespaces with a subscription before they were created 
    AND namespace_created_date <= dim_user.created_at::DATE -- excluding users created prior to namespace creation 
    AND days_since_namespace_creation_at_2nd_user_add <= 13
  GROUP BY 1,2 --grouping by namespace and days since namespace creation when a user was added
    QUALIFY total_users_added = 2

), d60_retention AS ( --return event between 60 - 90 days
       
   SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id
  FROM namespaces
  INNER JOIN mart_event_namespace_daily AS events
    ON events.dim_ultimate_parent_namespace_id = namespaces.ultimate_parent_namespace_id
  WHERE days_since_namespace_creation_at_event_date BETWEEN 59 AND 89
       
), valuable_signup AS ( --counting namespaces with billable members who initially sign up with a business email domain and are created prior to any paid subscription

  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    MAX(IFF(dim_user.dim_user_id = namespaces.creator_id, 1, 0))        AS creator_is_valuable_signup_numeric,
    IFF(creator_is_valuable_signup_numeric = 1, TRUE, FALSE)            AS creator_is_valuable_signup
  FROM namespaces
  INNER JOIN gitlab_dotcom_memberships AS mships
    ON namespaces.ultimate_parent_namespace_id = mships.ultimate_parent_id
  INNER JOIN dim_user -- including all users with a membership to the ultimate parent regardless of creator status
    ON dim_user.dim_user_id = mships.user_id
    AND dim_user.is_blocked_user = FALSE
    AND mships.is_billable = TRUE -- Optional based on use case - excluding guests, bots, other use cases that would not be counted towards a subscription if one were to exist
  LEFT JOIN charges -- joining charges info to exclude users joining namespaces with a subscription before they were created 
    ON namespaces.ultimate_parent_namespace_id = charges.ultimate_parent_namespace_id
  WHERE IFNULL(charges.first_paid_subscription_start_date,CURRENT_DATE) >= dim_user.created_at::DATE -- excluding users joining namespaces with a subscription before they were created 
    AND dim_user.email_domain_classification IS NULL -- indicates a business email domain
  GROUP BY 1
    
), stage_adoption AS (

    SELECT DISTINCT
      namespaces.ultimate_parent_namespace_id,
      stage_name,
      MIN(days_since_namespace_creation_at_event_date)                   AS days_since_namespace_creation_at_first_event_date,
      MAX(days_since_namespace_creation_at_event_date)                   AS days_since_namespace_creation_at_latest_event_date,
      -- COUNT(DISTINCT days_since_namespace_creation_at_event_date)        AS stage_usage_days, 
      -- COUNT(DISTINCT dim_user_id)                                        AS stage_users, --I don't think it makes sense to include these fields in a wide model like this, but open to suggestions
      ROW_NUMBER() OVER (PARTITION BY namespaces.ultimate_parent_namespace_id ORDER BY MIN(events.event_created_at) ASC) 
                                                                         AS stage_order_adopted,-- Sequential order that this stage was adopted initially at the ultimate namespace level
      CONCAT(stage_order_adopted, ' ', stage_name)                       AS stage_name_order -- This will allow me to create an ordered array 
    FROM namespaces
    INNER JOIN mart_event_valid AS events -- Using this model to remove events triggered in Learn Gitlab projects (Growth use case)
      ON namespaces.ultimate_parent_namespace_id = events.dim_ultimate_parent_namespace_id
    WHERE is_smau = TRUE
      AND events.project_is_learn_gitlab != TRUE -- Filters out events caused by default Learn GitLab project creation.
      AND days_since_namespace_creation_at_event_date >= 0 -- Excluding events that occur before namespace creation
    GROUP BY 1,2

), stage_aggregation AS (

  SELECT DISTINCT 
    ultimate_parent_namespace_id,
    MIN(IFF(stage_adoption.stage_name = 'plan', days_since_namespace_creation_at_first_event_date, null))                      
                                                                         AS days_since_namespace_creation_at_first_plan_event_date,
    MIN(IFF(stage_adoption.stage_name = 'secure', days_since_namespace_creation_at_first_event_date, null))   
                                                                         AS days_since_namespace_creation_at_first_secure_event_date,
    MIN(IFF(stage_adoption.stage_name = 'create', days_since_namespace_creation_at_first_event_date, null)) 
                                                                         AS days_since_namespace_creation_at_first_create_event_date,
    MIN(IFF(stage_adoption.stage_name = 'verify', days_since_namespace_creation_at_first_event_date, null)) 
                                                                         AS days_since_namespace_creation_at_first_verify_event_date,
    MIN(IFF(stage_adoption.stage_name = 'release', days_since_namespace_creation_at_first_event_date, null))  
                                                                         AS days_since_namespace_creation_at_first_release_event_date,
    MAX(IFF(stage_adoption.stage_name = 'plan', days_since_namespace_creation_at_latest_event_date, null))      
                                                                         AS days_since_namespace_creation_at_latest_plan_event_date,
    MAX(IFF(stage_adoption.stage_name = 'secure', days_since_namespace_creation_at_latest_event_date, null))   
                                                                         AS days_since_namespace_creation_at_latest_secure_event_date,
    MAX(IFF(stage_adoption.stage_name = 'create', days_since_namespace_creation_at_latest_event_date, null)) 
                                                                         AS days_since_namespace_creation_at_latest_create_event_date,
    MAX(IFF(stage_adoption.stage_name = 'verify', days_since_namespace_creation_at_latest_event_date, null)) 
                                                                         AS days_since_namespace_creation_at_latest_verify_event_date,
    MAX(IFF(stage_adoption.stage_name = 'release', days_since_namespace_creation_at_latest_event_date, null))  
                                                                         AS days_since_namespace_creation_at_latest_release_event_date,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT stage_name_order) WITHIN GROUP(ORDER BY stage_name_order ASC), ' , ')  
                                                                         AS stage_adoption_order,
    COUNT(DISTINCT stage_name)                                           AS total_stages_adopted --total stages adopted
  FROM stage_adoption
  GROUP BY 1

), creator_attributes AS ( --ultimate namespace creator attributes

  SELECT DISTINCT
    namespaces.ultimate_parent_namespace_id,
    role                                                                  AS namespace_creator_role,
    jobs_to_be_done                                                       AS namespace_creator_jtbd
  FROM namespaces
  INNER JOIN dim_user -- including all users with a membership to the ultimate parent regardless of creator status
    ON namespaces.creator_id = dim_user.dim_user_id

), billable_members AS ( --billable members calculated to match user limit calculations
    
  SELECT DISTINCT  
    namespaces.ultimate_parent_namespace_id, -- ultimate parent namespace
    COUNT(DISTINCT mships.user_id)                                         AS billable_member_count
  FROM namespaces
  INNER JOIN gitlab_dotcom_memberships AS mships  
    ON mships.ultimate_parent_id = namespaces.ultimate_parent_namespace_id 
  WHERE mships.is_billable = TRUE                                                           
  GROUP BY 1
            
), storage AS ( --current month storage snapshot
    
  SELECT DISTINCT 
    namespaces.ultimate_parent_namespace_id,
    storage_gib 
  FROM namespaces
  INNER JOIN fct_usage_storage AS storage 
    ON storage.ultimate_parent_namespace_id = namespaces.ultimate_parent_namespace_id
  WHERE snapshot_month = DATE_TRUNC('month', GETDATE())::DATE --current month storage snapshot

), base AS (

    SELECT
      namespaces.*,
      trials.trial_start_date,
      trials.days_since_namespace_creation_at_trial,
      charges.first_paid_subscription_start_date,
      charges.days_since_namespace_creation_at_first_paid_subscription,
      charges.first_paid_plan_name,
      COALESCE(charges.first_paid_plan_purchased_through_subscription_portal, FALSE)
                                                                           AS is_first_paid_plan_purchased_through_subscription_portal,
      activation_events.days_since_namespace_creation_at_activation_event,
      activation_events.activation_event_array,
      CASE WHEN DATEDIFF(days, namespace_created_date, current_date()) < 59 THEN NULL -- If namespace is not at day 59 yet, then null
           WHEN d60_retention.ultimate_parent_namespace_id IS NOT NULL THEN TRUE
           ELSE FALSE                                         END          AS acitivity_between_d60_d90,
      COALESCE(valuable_signup.creator_is_valuable_signup, FALSE)          AS creator_is_valuable_signup, 
      IFF(valuable_signup.ultimate_parent_namespace_id IS NOT NULL, TRUE, FALSE)  
                                                                           AS namespace_contains_valuable_signup,
      second_billable_member.days_since_namespace_creation_at_2nd_user_add,
      IFF(second_billable_member.ultimate_parent_namespace_id IS NOT NULL OR activation_events.ultimate_parent_namespace_id IS NOT NULL, TRUE, FALSE)
                                                                           AS has_team_activation,
      creator_attributes.namespace_creator_role,
      creator_attributes.namespace_creator_jtbd,
      days_since_namespace_creation_at_first_plan_event_date,
      days_since_namespace_creation_at_first_secure_event_date,
      days_since_namespace_creation_at_first_create_event_date,
      days_since_namespace_creation_at_first_verify_event_date,
      days_since_namespace_creation_at_first_release_event_date,
      days_since_namespace_creation - days_since_namespace_creation_at_latest_plan_event_date
                                                                            AS days_since_latest_plan_event_date,
      days_since_namespace_creation - days_since_namespace_creation_at_latest_secure_event_date
                                                                            AS days_since_latest_secure_event_date,
      days_since_namespace_creation - days_since_namespace_creation_at_latest_create_event_date
                                                                            AS days_since_latest_create_event_date,
      days_since_namespace_creation - days_since_namespace_creation_at_latest_verify_event_date
                                                                            AS days_since_latest_verify_event_date,
      days_since_namespace_creation - days_since_namespace_creation_at_latest_release_event_date
                                                                            AS days_since_latest_release_event_date,
      stage_adoption_order,
      COALESCE(total_stages_adopted, 0)                                     AS total_stages_adopted, --stages per ultimate namespace up to current
      billable_members.billable_member_count                                AS current_billable_member_count,
      storage.storage_gib                                                   AS current_month_storage_gib
    FROM namespaces 
    LEFT JOIN trials
      ON namespaces.ultimate_parent_namespace_id = trials.ultimate_parent_namespace_id
    LEFT JOIN charges
      ON namespaces.ultimate_parent_namespace_id = charges.ultimate_parent_namespace_id
    LEFT JOIN activation_events
      ON namespaces.ultimate_parent_namespace_id = activation_events.ultimate_parent_namespace_id
    LEFT JOIN d60_retention
      ON namespaces.ultimate_parent_namespace_id = d60_retention.ultimate_parent_namespace_id
    LEFT JOIN valuable_signup
      ON namespaces.ultimate_parent_namespace_id = valuable_signup.ultimate_parent_namespace_id
    LEFT JOIN second_billable_member
      ON namespaces.ultimate_parent_namespace_id = second_billable_member.ultimate_parent_namespace_id
    LEFT JOIN creator_attributes
      ON namespaces.ultimate_parent_namespace_id = creator_attributes.ultimate_parent_namespace_id
    LEFT JOIN stage_aggregation
      ON namespaces.ultimate_parent_namespace_id = stage_aggregation.ultimate_parent_namespace_id
    LEFT JOIN billable_members
      ON namespaces.ultimate_parent_namespace_id = billable_members.ultimate_parent_namespace_id
    LEFT JOIN storage
      ON namespaces.ultimate_parent_namespace_id = storage.ultimate_parent_namespace_id    

)


{{ dbt_audit(
    cte_ref="base",
    created_by="@eneuberger",
    updated_by="@eneuberger",
    created_date="2023-02-14",
    updated_date="2023-02-14"
) }}