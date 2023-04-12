{{ config(alias='report_opportunity_stage_progression') }}

WITH stage_dates AS (

    SELECT *
    --FROM prod.workspace_sales.report_opportunity_stages_dates
    FROM {{ ref('wk_sales_report_opportunity_stages_dates') }}

), date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM prod.workspace_sales.date_details

), opportunity AS (

    SELECT  *,
            CASE
                WHEN stage_name IN ('0-Pending Acceptance')
                    THEN '0. Acceptance'
                WHEN stage_name IN ('1-Discovery','2-Scoping')
                    THEN '1. Early'
                WHEN stage_name IN ('3-Technical Evaluation','4-Proposal')
                    THEN '2. Middle'
                WHEN stage_name IN ('5-Negotiating','6-Awaiting Signature')
                    THEN '3. Late'
                ELSE '4. Closed'
            END                 AS cycle_time_category
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf

), progression AS (

    SELECT opportunity_id,
            0                       AS stage_rank,
            0                       AS cycle_time_category_rank,
           '0. Acceptance'          AS cycle_time_category,
           '0-Pending Acceptance'   AS stage_name,
           stage_0_date             AS stage_date,
           NULL                     AS days_in_previous_stage,
           was_stage_0_skipped_flag AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            1                       AS stage_rank,
            1                       AS cycle_time_category_rank,
           '1. Early'               AS cycle_time_category,
           '1-Discovery'            AS stage_name,
           stage_1_date             AS stage_date,
           days_in_stage_0          AS days_in_previous_stage,
           was_stage_1_skipped_flag AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            2                       AS stage_rank,
            1                       AS cycle_time_category_rank,
           '1. Early'               AS cycle_time_category,
           '2-Scoping'              AS stage_name,
           stage_2_date             AS stage_date,
           days_in_stage_1          AS days_in_previous_stage,
           was_stage_2_skipped_flag AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            3                       AS stage_rank,
            2                       AS cycle_time_category_rank,
           '2. Middle'              AS cycle_time_category,
           '3-Technical Evaluation' AS stage_name,
           stage_3_date             AS stage_date,
           days_in_stage_2          AS days_in_previous_stage,
           was_stage_3_skipped_flag AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            4                       AS stage_rank,
            2                       AS cycle_time_category_rank,
           '2. Middle'              AS cycle_time_category,
           '4-Proposal'             AS stage_name,
           stage_4_date             AS stage_date,
           days_in_stage_3          AS days_in_previous_stage,
           was_stage_4_skipped_flag AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            5                       AS stage_rank,
            3                       AS cycle_time_category_rank,
           '3. Late'                AS cycle_time_category,
           '5-Negotiating'          AS stage_name,
           stage_5_date             AS stage_date,
           days_in_stage_4          AS days_in_previous_stage,
           was_stage_5_skipped_flag AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            6                       AS stage_rank,
            3                       AS cycle_time_category_rank,
           '3. Late'                AS cycle_time_category,
           '6-Awaiting Signature'   AS stage_name,
           stage_6_date             AS stage_date,
           days_in_stage_5          AS days_in_previous_stage,
           was_stage_6_skipped_flag AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            7                       AS stage_rank,
            3                       AS cycle_time_category_rank,
           '3. Late'                AS cycle_time_category,
           '7-Closing'              AS stage_name,
           stage_7_date             AS stage_date,
           days_in_stage_6          AS days_in_previous_stage,
           was_stage_7_skipped_flag AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            8                       AS stage_rank,
            4                       AS cycle_time_category_rank,
           '4. Closed'              AS cycle_time_category,
           'Closed Won'             AS stage_name,
           stage_8_won_date         AS stage_date,
           days_in_stage_7          AS days_in_previous_stage,
           0                        AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            9                       AS stage_rank,
            4                       AS cycle_time_category_rank,
           '4. Closed'              AS cycle_time_category,
           '8-Closed Lost'          AS stage_name,
           stage_8_lost_date        AS stage_date,
           COALESCE(
               days_in_stage_7,
               days_in_stage_6,
               days_in_stage_5,
               days_in_stage_4,
               days_in_stage_3,
               days_in_stage_2,
               days_in_stage_1,
               days_in_stage_0
               )                    AS days_in_previous_stage,
           0                        AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            10                      AS stage_rank,
            4                       AS cycle_time_category_rank,
           '4. Closed'              AS cycle_time_category,
           '9-Unqualified'          AS stage_name,
           stage_9_date             AS stage_date,
           COALESCE(
               days_in_stage_7,
               days_in_stage_6,
               days_in_stage_5,
               days_in_stage_4,
               days_in_stage_3,
               days_in_stage_2,
               days_in_stage_1,
               days_in_stage_0
               )                    AS days_in_previous_stage,
           0                        AS was_stage_skipped_flag
    FROM stage_dates
    UNION
    SELECT opportunity_id,
            11                      AS stage_rank,
            4                       AS cycle_time_category_rank,
           '4. Closed'              AS cycle_time_category,
           '10-Duplicate'           AS stage_name,
           stage_10_date            AS stage_date,
           COALESCE(
               days_in_stage_7,
               days_in_stage_6,
               days_in_stage_5,
               days_in_stage_4,
               days_in_stage_3,
               days_in_stage_2,
               days_in_stage_1,
               days_in_stage_0
               )                    AS days_in_previous_stage,
           0                        AS was_stage_skipped_flag
    FROM stage_dates
    
), final AS (

    SELECT  prog.*,
            report.date_actual                       AS report_date,
            report.fiscal_quarter_name_fy            AS report_fiscal_quarter_name,
            report.first_day_of_fiscal_quarter       AS report_fiscal_quarter_date,
            report.week_of_fiscal_quarter_normalised AS report_week_of_fiscal_quarter_normalised,
            report.day_of_fiscal_quarter_normalised  AS report_day_of_fisca_quarter_normalised,
            report.fiscal_year                       AS report_fiscal_year,
            report.month_of_fiscal_year              AS report_fiscal_month,

           -- Do not calculate age in stage for closed won deals.
           -- for other stages, assume that if it is not the current stage
           -- there will be other lines in the model, so we use the stage rank
           -- and the LEAD function to get the next one and retrieve the value.
            CASE
                WHEN prog.cycle_time_category != 'Closed'
                    THEN
                        COALESCE(
                        LEAD(prog.days_in_previous_stage,1)
                            OVER ( PARTITION BY prog.opportunity_id ORDER BY prog.stage_rank),
                                DATEDIFF(day, prog.stage_date,CURRENT_DATE)
                            )
                ELSE NULL
            END AS days_in_current_stage,

            -- adding opportunity keys for reporting
            opp.is_eligible_cycle_time_analysis_flag,
            opp.sales_type,
            opp.order_type_stamped,
            opp.deal_size,
            opp.deal_group,
            opp.sales_qualified_source,

            -- business keys
            opp.key_bu,
            opp.key_bu_subbu,
            -- other categorization fields
            opp.net_arr             AS current_net_arr,
            opp.booked_net_arr      AS current_booked_net_arr,
            opp.stage_name          AS current_stage_name,
            opp.cycle_time_category AS current_cycle_time_category,
    
            opp.close_fiscal_quarter_name AS current_close_fiscal_quarter_name,
            opp.close_fiscal_year         AS current_close_fiscal_year,
    
            CASE
                WHEN prog.stage_name = opp.stage_name
                    THEN 1
                ELSE 0
            END                                     AS is_current_stage_flag,

            -- cycle time categories might span multiple stages
            CASE
                WHEN prog.cycle_time_category = opp.cycle_time_category
                    THEN 1
                ELSE 0
            END                                     AS is_current_cycle_time_category_flag,

            -- status flags
            opp.is_open,
            opp.is_closed,
            opp.is_lost,
            opp.is_won,
            opp.is_renewal
    FROM progression prog
        INNER JOIN date_details report
            ON report.date_actual = prog.stage_date
        INNER JOIN opportunity opp
            ON opp.opportunity_id = prog.opportunity_id
)

SELECT *
FROM final