WITH jp_ss AS (
  SELECT *
  FROM {{ source('workday', 'job_profiles_snapshots_source') }}
  WHERE NOT _fivetran_deleted
    AND job_workday_id IS NOT NULL
),

jp_hist AS (
  SELECT *
  FROM {{ source('workday', 'job_profiles_historical_source') }}
),

jp_stage AS (
  SELECT
    jp_ss.report_effective_date,
    job_workday_id,
    job_code,
    job_profile,
    management_level,
    job_level,
    job_family,
    is_job_profile_active
  FROM jp_ss
  WHERE jp_ss.report_effective_date > (
      SELECT MAX(jp_hist.report_effective_date) AS max_hist_date
      FROM jp_hist
    )

  UNION

  SELECT
    report_effective_date,
    job_workday_id,
    job_code,
    job_profile,
    management_level,
    job_level,
    job_family,
    is_job_profile_active
  FROM jp_hist
),

jp AS (
  SELECT
    job_workday_id,
    ROW_NUMBER() OVER (
      PARTITION BY job_workday_id ORDER BY report_effective_date ASC
    )                                                             AS record_rank_asc,
    ROW_NUMBER() OVER (
      PARTITION BY job_workday_id ORDER BY report_effective_date DESC
    )                                                             AS record_rank_desc,
    IFF(record_rank_asc = 1, '1900-01-01', report_effective_date) AS valid_from,
    COALESCE(LAG(report_effective_date) OVER (
      PARTITION BY job_workday_id ORDER BY report_effective_date DESC
    ), '2099-01-01')                                              AS valid_to,
    job_code,
    job_profile,
    management_level,
    job_level::FLOAT                                              AS job_level,
    job_family,
    inactive                                                      AS is_job_profile_active
  FROM jp_stage
)

SELECT
  job_workday_id,
  job_code,
  job_profile,
  management_level,
  job_level,
  job_family,
  is_job_profile_active,
  valid_from,
  valid_to
FROM jp
