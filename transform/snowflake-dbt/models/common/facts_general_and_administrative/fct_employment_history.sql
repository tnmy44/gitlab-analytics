WITH prep_employee_history AS (
  SELECT *
  FROM {{ ref('prep_employee_history') }}
),

final AS (

  SELECT *

  FROM prep_employee_history

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@rakhireddy',
    updated_by='@rakhireddy',
    created_date='2024-08-14',
    updated_date='2024-08-14',
) }}