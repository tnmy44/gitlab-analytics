WITH
groups AS (
  SELECT
    value:name::VARCHAR             AS group_name,
    value:adopted::INT              AS adopted,
    value:notAdopted::INT           AS not_adopted,
    jsontext:aggregatedAt::DATETIME AS aggregated_at
  FROM
    {{ source('pajamas_adoption_scanner', 'adoption_by_group') }},
    LATERAL FLATTEN(input => jsontext:groups)
),

bounds AS (
  SELECT
    jsontext:bounds:low             AS lower_bound,
    jsontext:bounds:high            AS upper_bound,
    jsontext:aggregatedAt::DATETIME AS aggregated_at
  FROM
    {{ source('pajamas_adoption_scanner', 'adoption_by_group') }}
),

minimum_findings_table AS (
  SELECT
    jsontext:minimumFindings        AS minimum_findings,
    jsontext:aggregatedAt::DATETIME AS aggregated_at
  FROM
    {{ source('pajamas_adoption_scanner', 'adoption_by_group') }}
),

groups_joined AS (


  SELECT
    groups.group_name,
    groups.adopted,
    groups.not_adopted,
    bounds.lower_bound,
    bounds.upper_bound,
    minimum_findings_table.minimum_findings,
    groups.aggregated_at
  FROM groups
  INNER JOIN bounds
    ON groups.aggregated_at = bounds.aggregated_at
  INNER JOIN minimum_findings_table
    ON groups.aggregated_at = minimum_findings_table.aggregated_at
)

SELECT *
FROM
  groups_joined
