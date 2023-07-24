WITH ci_lookback AS (

  SELECT
    date_day                                  AS date_day,
    NULL                                      AS gcp_project_id,
    NULL                                      AS gcp_service_description,
    NULL                                      AS gcp_sku_description,
    'continuous_integration'                  AS infra_label,
    NULL                                      AS env_label,
    NULL                                      AS runner_label,
    LOWER(ci_runners_pl_lookback.pl_category) AS pl_category,
    ci_runners_pl_lookback.pl_percent         AS pl_percent,
    'continous_integration_lookback'          AS from_mapping
  FROM {{ ref ('ci_runners_pl_lookback') }}

),

flex_cud AS (

  SELECT
    date_day                                        AS date_day,
    'gitlab-production'                             AS gcp_project_id,
    NULL                                            AS gcp_service_description,
    'Commitment - dollar based v1: GCE for 3 years' AS gcp_sku_description,
    'shared'                                        AS infra_label,
    NULL                                            AS env_label,
    NULL                                            AS runner_label,
    LOWER(flex_cud_lookback.pl_category)            AS pl_category,
    flex_cud_lookback.pl_percent                    AS pl_percent,
    'flex_cud_lookback'                             AS from_mapping
  FROM {{ ref ('flex_cud_lookback') }}

),


cte_append AS (SELECT *
  FROM ci_lookback
  UNION ALL
  SELECT *
  FROM flex_cud
)

SELECT
  date_day,
  gcp_project_id,
  gcp_service_description,
  gcp_sku_description,
  infra_label,
  env_label,
  runner_label,
  LOWER(pl_category)           AS pl_category,
  pl_percent,
  LISTAGG(DISTINCT from_mapping, ' || ') WITHIN GROUP (
    ORDER BY from_mapping ASC) AS from_mapping
FROM cte_append
{{ dbt_utils.group_by(n=9) }}
