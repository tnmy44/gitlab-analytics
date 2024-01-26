WITH base AS (

    SELECT * 
    FROM {{ source('salesforce_sandbox', 'user_role') }}

)

SELECT *
FROM base


UNION ALL

SELECT * 
FROM {{ ref ('fy25_sfdc_sandbox_user_roles_hierarchy') }}