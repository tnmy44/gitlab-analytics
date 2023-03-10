{{ config(alias='report_opportunity_stage_progression') }}

WITH sfdc_opportunity_snapshot_history_xf AS (

    SELECT snapshot_date,
    	opportunity_id,
    	stage_name  AS new_stage_name,
    	LAG(stage_name,1) OVER(PARTITION BY opportunity_id ORDER BY snapshot_date) AS old_stage_name,
        net_arr
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf
    WHERE snapshot_fiscal_year > 2021

), date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM prod.workspace_sales.date_details

), stage_delta AS (

    SELECT snapshot_date,
    	opportunity_id,
        old_stage_name,
    	new_stage_name,
        net_arr
    FROM sfdc_opportunity_snapshot_history_xf hist
    WHERE new_stage_name <> old_stage_name

), stage_delta_final AS (

    SELECT *,
        LAG(snapshot_date,1) OVER(PARTITION BY opportunity_id ORDER BY snapshot_date) 	AS old_stage_date,
        DATEDIFF(DAY,coalesce(old_stage_date,snapshot_date),snapshot_date)			    AS days_in_old_stage
    FROM stage_delta

), final AS (

    SELECT delta.*,
        fiscal.fiscal_year                 AS old_stage_fiscal_year,
        fiscal.fiscal_quarter_name_fy      AS old_stage_fiscal_quarter_name,
        fiscal.first_day_of_fiscal_quarter AS old_stage_fiscal_quarter_date
    FROM stage_delta_final delta
        INNER JOIN date_details fiscal
            ON delta.old_stage_date = fiscal.date_actual

)

SELECT *
FROM final