{% set columns = ["dim_marketing_contact_id", "namespace_id", "score_group", "score", "score_date", "past_score_group", "past_score_date"] %}

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
          THEN 'PtP Trial'
        WHEN prep_ptpf_scores_by_user.score_group >= 4
          THEN 'PtP Free'
        WHEN prep_ptpt_scores_by_user.dim_marketing_contact_id IS NOT NULL
          THEN 'PtP Trial'
        ELSE 'PtP Free'
      END source_model
    FROM prep_ptpt_scores_by_user
    FULL OUTER JOIN prep_ptpf_scores_by_user
      ON prep_ptpt_scores_by_user.dim_marketing_contact_id = prep_ptpf_scores_by_user.dim_marketing_contact_id

)

SELECT *
FROM dedup
