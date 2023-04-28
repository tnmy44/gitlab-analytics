{{ config(alias='report_opportunity_stage_open_snapshot') }}

WITH date_details AS (

    SELECT *
    --FROM prod.workspace_sales.date_details
    FROM {{ ref('wk_sales_date_details') }}

), opportunity_snapshot AS (

    SELECT *
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}

-- NF: Exclude non eligible deals for stage progresssion
), stage_progression AS (

    SELECT *
    --FROM prod.restricted_safe_workspace_sales.report_opportunity_stage_progression
    FROM {{ref('wk_sales_report_opportunity_stage_progression')}}
    WHERE is_eligible_cycle_time_analysis_flag = 1
      AND sales_type != 'Renewal'
      AND deal_group IN ('1. New', '2. Growth')

), open_deals AS (

    SELECT *,
            CASE
               WHEN stage_name IN ('4-Proposal',
                                 '5-Negotiating',
                                 '3-Technical Evaluation',
                                 '0-Pending Acceptance',
                                 '1-Discovery',
                                 '2-Scoping',
                                 '6-Awaiting Signature',
                                 '7-Closing')
                   THEN 1
                ELSE 0
            END                                     AS stage_is_open_flag
    FROM stage_progression
    WHERE is_eligible_cycle_time_analysis_flag = 1
      AND sales_type != 'Renewal'
      AND deal_group IN ('1. New', '2. Growth')

), report_dates AS (

    SELECT *
    FROM date_details
    WHERE(first_day_of_fiscal_quarter = date_actual
            OR date_actual = DATEADD(DAY,-1,CURRENT_DATE))
    AND fiscal_year IN (2022,2023,2024)
    AND date_actual < CURRENT_DATE

), specific_date_open_pipeline AS (

    SELECT prog.opportunity_id,
           prog.stage_rank,
           prog.cycle_time_category,
           prog.stage_name,
           prog.stage_date,
           prog.days_in_previous_stage,
           prog.sales_type,
           prog.order_type_stamped,
           prog.deal_size,
           prog.deal_group,
           prog.sales_qualified_source,
           prog.stage_is_open_flag,

           -- keys
           prog.key_bu,
           prog.key_bu_subbu,

           -- report dates
           report_dates.date_actual                 AS report_date,
           report_dates.first_day_of_fiscal_quarter AS report_fiscal_quarter_date,
           report_dates.fiscal_quarter_name_fy      AS report_fiscal_quarter_name,
           report_dates.fiscal_year                 AS report_fiscal_year,

           ROW_NUMBER() OVER (PARTITION BY prog.opportunity_id,
                                        report_dates.date_actual
                                ORDER BY prog.stage_date DESC,
                                    prog.stage_rank DESC) AS rank_stage_date,
           ROW_NUMBER() OVER (PARTITION BY prog.opportunity_id,
                                        report_dates.date_actual
                                ORDER BY prog.cycle_time_category DESC,
                                    prog.stage_rank DESC) AS rank_category_date
    FROM open_deals prog
        CROSS JOIN report_dates
    WHERE prog.stage_date <= report_dates.date_actual

    -- include only the last stage change before the cutoff date
    QUALIFY rank_stage_date = 1

), final AS (

    SELECT prog.*,
           snap.net_arr,
           snap.snapshot_date,
           CASE
               WHEN prog.stage_is_open_flag = 1
                THEN DATEDIFF(DAY, stage_date, report_date)
                ELSE 0
           END AS days_in_current_stage
    FROM specific_date_open_pipeline prog
        INNER JOIN opportunity_snapshot snap
        ON prog.opportunity_id = snap.opportunity_id
        -- make sure updates made it through
        AND prog.report_date = snap.snapshot_date
        -- multiple stages hitting the same day
        AND prog.stage_name = snap.stage_name

)

SELECT *
FROM final