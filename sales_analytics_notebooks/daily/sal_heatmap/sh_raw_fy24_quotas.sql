WITH sfdc_users AS (

    SELECT *
    FROM prod.workspace_sales.sfdc_users_xf
    WHERE employee_number IS NOT NULL
        AND is_active = 1

), quotas AS (

    SELECT *
    FROM raw.sales_analytics.ae_quotas_unpivoted

), final AS (


    SELECT
        sfdc_users.user_id,
        quotas.employee_id,
        quotas.name                                           AS sfdc_name,
        quotas.start_date,
        sfdc_users.role_type                                  AS role_type,

        LOWER(
            sfdc_users.key_bu_subbu_division_asm
            || '_' || sfdc_users.role_type || '_' || TO_VARCHAR(sfdc_users.employee_number)
        )                                                                                                 AS sal_heatmap_key,
        LOWER(sfdc_users.key_bu_subbu)                                                                    AS sal_region_key,
        LOWER(sfdc_users.business_unit)                                                                   AS business_unit,
        LOWER(sfdc_users.sub_business_unit)                                                               AS sub_business_unit,
        LOWER(sfdc_users.division)                                                                        AS division,
        LOWER(sfdc_users.asm)                                                                             AS asm,

        CASE
            WHEN LOWER(sfdc_users.title) LIKE '%strategic account%' AND DATEADD(MONTH, 9, quotas.start_date) <= '2022-08-15'
                THEN 'Ramped'
            WHEN LOWER(sfdc_users.title) LIKE '%account executive%mid market%' AND DATEADD(MONTH, 4, quotas.start_date) <= '2022-06-15'
                THEN 'Ramped'
            WHEN LOWER(sfdc_users.title) LIKE '%smb account executive%' AND DATEADD(MONTH, 3, quotas.start_date) <= '2022-05-15'
                THEN 'Ramped'
            ELSE 'Ramping'
        END                                                                                               AS ramp_status,

        quotas.cfy_q1,
        quotas.cfy_q2,
        quotas.cfy_q3,
        quotas.cfy_q4,


        quotas.cfy_q1 + quotas.cfy_q2 + quotas.cfy_q3 + quotas.cfy_q4 AS cfy_total,

        quotas.nfy_q1,
        quotas.nfy_q2,
        quotas.nfy_q3,
        quotas.nfy_q4,

        quotas.nfy_q1 + quotas.nfy_q2 + quotas.nfy_q3 + quotas.nfy_q4 AS nfy_total

    FROM quotas
    INNER JOIN sfdc_users
        ON quotas.employee_id = sfdc_users.employee_number
)

select *
from final