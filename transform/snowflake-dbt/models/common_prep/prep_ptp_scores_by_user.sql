{% set columns = ["dim_marketing_contact_id", "score_group", "score", "score_date", "insights", "past_insights", "past_score_group", "past_score_date"] %}

{{ simple_cte([
    ('prep_ptpf_scores_by_user', 'prep_ptpf_scores_by_user'),
    ('prep_ptpt_scores_by_user', 'prep_ptpt_scores_by_user'),
    ('prep_ptpl_scores_by_user', 'prep_ptpl_scores_by_user')
    ])
}}

, base AS (


    SELECT dim_marketing_contact_id
    FROM prep_ptpf_scores_by_user

    UNION 

    SELECT dim_marketing_contact_id
    FROM prep_ptpl_scores_by_user

    UNION

    SELECT dim_marketing_contact_id
    FROM prep_ptpt_scores_by_user

)

, dedup AS (

    SELECT
      CASE
        WHEN prep_ptpt_scores_by_user.score_group >= 4
          THEN 'Trial'
        WHEN prep_ptpf_scores_by_user.score_group >= 5
          THEN 'Free'
        WHEN prep_ptpl_scores_by_user.score_group >= 5
          THEN 'Lead'
        WHEN prep_ptpf_scores_by_user.score_group >= 4
          THEN 'Free'
        WHEN prep_ptpl_scores_by_user.score_group >= 4
          THEN 'Lead'
        WHEN prep_ptpt_scores_by_user.dim_marketing_contact_id IS NOT NULL
          THEN 'Trial'
        WHEN prep_ptpf_scores_by_user.dim_marketing_contact_id IS NOT NULL
          THEN 'Free'
        ELSE  'Lead'
      END AS ptp_source,
      {% for column in columns %}
      CASE
        WHEN ptp_source = 'Trial'
          THEN prep_ptpt_scores_by_user.{{column}}
        WHEN ptp_source = 'Free'
          THEN prep_ptpf_scores_by_user.{{column}}
        WHEN ptp_source = 'Lead'
          THEN prep_ptpl_scores_by_user.{{column}}
      END AS {{column}},
      {% endfor %}
      CASE
        WHEN ptp_source = 'Trial'
          THEN 'Namespace'
        WHEN ptp_source = 'Free'
          THEN 'Namespace'
        WHEN ptp_source = 'Lead'
          THEN 'Lead'
      END AS model_grain,
      CASE
        WHEN ptp_source = 'Trial'
          THEN prep_ptpt_scores_by_user.namespace_id
        WHEN ptp_source = 'Free'
          THEN prep_ptpf_scores_by_user.namespace_id
        WHEN ptp_source = 'Lead'
          THEN prep_ptpl_scores_by_user.lead_id
      END AS model_grain_id,
      CASE
        WHEN ptp_source = 'Free'
          THEN prep_ptpf_scores_by_user.days_since_trial_start
        ELSE NULL
      END AS days_since_trial_start
    FROM base
    LEFT JOIN prep_ptpt_scores_by_user
      ON base.dim_marketing_contact_id = prep_ptpt_scores_by_user.dim_marketing_contact_id
    LEFT JOIN prep_ptpf_scores_by_user
      ON base.dim_marketing_contact_id = prep_ptpf_scores_by_user.dim_marketing_contact_id
    LEFT JOIN prep_ptpl_scores_by_user
      ON base.dim_marketing_contact_id = prep_ptpl_scores_by_user.dim_marketing_contact_id

)

SELECT *
FROM dedup
