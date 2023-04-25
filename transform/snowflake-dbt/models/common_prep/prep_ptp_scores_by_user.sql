{% set columns = ["dim_marketing_contact_id", "namespace_id", "score_group", "score", "score_date", "insights", "past_insights", "past_score_group", "past_score_date"] %}

{{ simple_cte([
    ('prep_ptpf_scores_by_user', 'prep_ptpf_scores_by_user'),
    ('prep_ptpt_scores_by_user', 'prep_ptpt_scores_by_user')
    ])
}}

, dedup AS (

    SELECT
      {% for column in columns %}
      CASE
        WHEN prep_ptpt_scores_by_user.score_group >= 4
          THEN prep_ptpt_scores_by_user.{{column}}
        WHEN prep_ptpf_scores_by_user.score_group >= 4
          THEN prep_ptpf_scores_by_user.{{column}}
        WHEN prep_ptpt_scores_by_user.dim_marketing_contact_id IS NOT NULL
          THEN prep_ptpt_scores_by_user.{{column}}
        ELSE
          prep_ptpf_scores_by_user.{{column}}
      END {{column}},
      {% endfor %}
      CASE
        WHEN prep_ptpt_scores_by_user.score_group >= 4
          THEN NULL
        WHEN prep_ptpf_scores_by_user.score_group >= 4
          THEN prep_ptpf_scores_by_user.days_since_trial_start
        WHEN prep_ptpt_scores_by_user.dim_marketing_contact_id IS NOT NULL
          THEN NULL
        ELSE
          prep_ptpf_scores_by_user.days_since_trial_start
      END days_since_trial_start,
      CASE
        WHEN prep_ptpt_scores_by_user.score_group >= 4
          THEN 'Trial'
        WHEN prep_ptpf_scores_by_user.score_group >= 4
          THEN 'Free'
        WHEN prep_ptpt_scores_by_user.dim_marketing_contact_id IS NOT NULL
          THEN 'Trial'
        ELSE 'Free'
      END ptp_source
    FROM prep_ptpt_scores_by_user
    FULL OUTER JOIN prep_ptpf_scores_by_user
      ON prep_ptpt_scores_by_user.dim_marketing_contact_id = prep_ptpf_scores_by_user.dim_marketing_contact_id

)

SELECT *
FROM dedup
