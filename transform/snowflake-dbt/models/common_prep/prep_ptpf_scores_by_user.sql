{{ simple_cte([
    ('prep_namespace', 'prep_namespace'),
    ('gitlab_dotcom_users_source', 'gitlab_dotcom_users_source')
    ])
}}

, ptpf_scores AS (

    SELECT
      namespace_id,
      score_date,
      score,
      decile,
      score_group,
      insights
    FROM {{ ref('ptpf_scores_source') }}

), score_dates AS (
    
    SELECT DISTINCT score_date
    FROM ptpf_scores
  
), last_dates AS (
  
    SELECT
      FIRST_VALUE(score_date) OVER(ORDER BY score_date DESC)  AS last_score_date,
      NTH_VALUE(score_date, 2) OVER(ORDER BY score_date DESC) AS after_last_score_date
    FROM score_dates
    LIMIT 1

), ptpf_scores_last AS (

    SELECT *
    FROM ptpf_scores
    WHERE score_date IN (SELECT last_score_date FROM last_dates)
  
), ptpf_scores_last_2 AS (
  
    SELECT *
    FROM ptpf_scores
    WHERE score_date IN (SELECT after_last_score_date FROM last_dates)

), namespace_creator_ptpf_score AS (

    SELECT
      COALESCE(users.notification_email, users.email) AS email_address,
      ptpf_scores_last.namespace_id,
      ptpf_scores_last.score,
      ptpf_scores_last.insights,
      ptpf_scores_last.score_group,
      ptpf_scores_last.score_date::DATE                    AS score_date
    FROM prep_namespace
    INNER JOIN gitlab_dotcom_users_source users
      ON prep_namespace.creator_id = users.user_id
    INNER JOIN ptpf_scores_last
      ON prep_namespace.dim_namespace_id = ptpf_scores_last.namespace_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY score DESC) = 1

), namespace_creator_ptpf_score_last_2 AS (

    SELECT
      COALESCE(users.notification_email, users.email) AS email_address,
      ptpf_scores_last_2.insights,
      ptpf_scores_last_2.score_group,
      ptpf_scores_last_2.score_date
    FROM prep_namespace
    INNER JOIN gitlab_dotcom_users_source users
      ON prep_namespace.creator_id = users.user_id
    INNER JOIN ptpf_scores_last_2
      ON prep_namespace.dim_namespace_id = ptpf_scores_last_2.namespace_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY score DESC) = 1

)

SELECT
  {{ dbt_utils.surrogate_key(['namespace_creator_ptpf_score.email_address']) }} AS dim_marketing_contact_id,
  namespace_creator_ptpf_score.namespace_id,
  namespace_creator_ptpf_score.score,
  namespace_creator_ptpf_score.insights,
  namespace_creator_ptpf_score.score_group,
  namespace_creator_ptpf_score.score_date,
  namespace_creator_ptpf_score_last_2.insights          AS past_insights,
  namespace_creator_ptpf_score_last_2.score_group       AS past_score_group,
  namespace_creator_ptpf_score_last_2.score_date::DATE  AS past_score_date
FROM namespace_creator_ptpf_score
LEFT JOIN namespace_creator_ptpf_score_last_2
  ON namespace_creator_ptpf_score.email_address = namespace_creator_ptpf_score_last_2.email_address
WHERE namespace_creator_ptpf_score.score_group >= 4
