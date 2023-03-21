WITH key_talent AS (

  SELECT *
  FROM {{ ref('assess_talent_source') }}

),

performance AS (

  SELECT *
  FROM {{ ref('performance_growth_potential_source') }}

),

key_talent_no_gaps AS (

  SELECT
    t1.employee_id,
    t1.key_talent,
    t1.effective_date AS valid_from,
    COALESCE(MIN(t2.effective_date), NULL) AS valid_to
  FROM key_talent t1
  LEFT JOIN key_talent t2
    ON t1.employee_id = t2.employee_id
    AND t1.effective_date < t2.effective_date
  {{ dbt_utils.group_by(n=3)}}
),

performance_no_gaps AS (

  SELECT
    t1.employee_id,
    t1.growth_potential_rating,
    t1.performance_rating,
    t1.review_period_end_date AS valid_from,
    COALESCE(MIN(t2.review_period_end_date), NULL) AS valid_to
  FROM performance t1
  LEFT JOIN performance t2
    ON t1.employee_id = t2.employee_id
    AND t1.review_period_end_date < t2.review_period_end_date
  {{ dbt_utils.group_by(n=4)}}
),

final AS (

  SELECT
    key_talent_no_gaps.employee_id,
    key_talent_no_gaps.key_talent,
    performance_no_gaps.growth_potential_rating,
    performance_no_gaps.performance_rating,
    GREATEST(key_talent_no_gaps.valid_from, performance_no_gaps.valid_from) AS valid_from,
    LEAST(key_talent_no_gaps.valid_to, performance_no_gaps.valid_to)        AS valid_to
  FROM performance_no_gaps 
  LEFT JOIN key_talent_no_gaps 
    ON key_talent_no_gaps.employee_id = performance_no_gaps.employee_id
    AND (
        CASE
          WHEN performance_no_gaps.valid_from >= key_talent_no_gaps.valid_from AND performance_no_gaps.valid_from < key_talent_no_gaps.valid_to THEN TRUE
          WHEN performance_no_gaps.valid_to > key_talent_no_gaps.valid_from AND performance_no_gaps.valid_to <= key_talent_no_gaps.valid_to THEN TRUE
          WHEN key_talent_no_gaps.valid_from >= performance_no_gaps.valid_from AND key_talent_no_gaps.valid_from < performance_no_gaps.valid_to THEN TRUE
          ELSE FALSE
        END) = TRUE
)

SELECT *
FROM final
