{{ simple_cte([
    ('ptpl_scores', 'ptpl_scores_source'),
    ('sfdc_lead_source', 'sfdc_lead_source')
    ])
}}

, score_dates AS (
    
    SELECT DISTINCT score_date
    FROM ptpl_scores
  
), last_dates AS (
  
    SELECT
      FIRST_VALUE(score_date) OVER(ORDER BY score_date DESC)  AS last_score_date,
      NTH_VALUE(score_date, 2) OVER(ORDER BY score_date DESC) AS after_last_score_date
    FROM score_dates
    LIMIT 1

), ptpl_scores_last AS (

    SELECT *
    FROM ptpl_scores
    WHERE score_date IN (SELECT last_score_date FROM last_dates)
  
), ptpl_scores_last_2 AS (
  
    SELECT *
    FROM ptpl_scores
    WHERE score_date IN (SELECT after_last_score_date FROM last_dates)

), namespace_creator_ptpl_score AS (

    SELECT
      leads.lead_email                                     AS email_address,
      ptpl_scores_last.lead_id,
      ptpl_scores_last.score,
      ptpl_scores_last.score_group,
      ptpl_scores_last.insights,
      ptpl_scores_last.score_date::DATE                    AS score_date
    FROM sfdc_lead_source leads
    INNER JOIN ptpl_scores_last
      ON ptpl_scores_last.lead_id = leads.lead_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY ptpl_scores_last.score DESC) = 1

), namespace_creator_ptpl_score_last_2 AS (

    SELECT
      leads.lead_email                                     AS email_address,
      ptpl_scores_last_2.insights,
      ptpl_scores_last_2.score_group,
      ptpl_scores_last_2.score_date
    FROM sfdc_lead_source leads
    INNER JOIN ptpl_scores_last_2
      ON leads.lead_id = ptpl_scores_last_2.lead_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY email_address ORDER BY ptpl_scores_last_2.score DESC) = 1

)

SELECT
  {{ dbt_utils.surrogate_key(['namespace_creator_ptpl_score.email_address']) }} AS dim_marketing_contact_id,
  namespace_creator_ptpl_score.lead_id,
  namespace_creator_ptpl_score.score,
  namespace_creator_ptpl_score.score_group,
  namespace_creator_ptpl_score.insights,
  namespace_creator_ptpl_score.score_date,
  namespace_creator_ptpl_score_last_2.insights          AS past_insights,
  namespace_creator_ptpl_score_last_2.score_group       AS past_score_group,
  namespace_creator_ptpl_score_last_2.score_date::DATE  AS past_score_date
FROM namespace_creator_ptpl_score
LEFT JOIN namespace_creator_ptpl_score_last_2
  ON namespace_creator_ptpl_score.email_address = namespace_creator_ptpl_score_last_2.email_address
