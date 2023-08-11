/*

Pipeline Generated CQ Historical AGGREGATED

*/

WITH date_details AS (

    SELECT *
    FROM prod.workspace_sales.date_details

),

sfdc_account_xf AS (

    SELECT
        account_id,
        industry
    FROM prod.restricted_safe_workspace_sales.sfdc_accounts_xf


),

sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf

),

current_quarter_date AS (

    SELECT
        fiscal_year                      AS current_fiscal_year,
        date_actual                      AS current_calendar_date,
        fiscal_quarter_name_fy           AS current_fiscal_quarter_name,
        first_day_of_fiscal_quarter      AS current_fiscal_quarter_date,
        day_of_fiscal_quarter_normalised AS current_day_of_fiscal_quarter_normalized
    FROM date_details
    WHERE date_actual = (
        SELECT MAX(snapshot_date)
        FROM sfdc_opportunity_snapshot_history_xf
    )
),

opty_base AS (

    SELECT

        opty.*,
        calculated_deal_size AS deal_size_bin

    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf AS opty
    CROSS JOIN current_quarter_date
    INNER JOIN sfdc_account_xf AS account
        ON account.account_id = opty.account_id
    -- only capture current fiscal quarter
    WHERE
        current_quarter_date.current_fiscal_quarter_name = opty.pipeline_created_fiscal_quarter_name
        AND is_eligible_created_pipeline_flag = 1

),

snap_base AS (

    SELECT
        snap.*,

        calculated_deal_size AS deal_size_bin

    FROM sfdc_opportunity_snapshot_history_xf AS snap
    INNER JOIN current_quarter_date
        ON snap.snapshot_day_of_fiscal_quarter_normalised = 90

    INNER JOIN sfdc_account_xf AS account
        ON account.account_id = snap.account_id
    -- Exclude the current quarter so we could leverage the opportunity object
    -- the idea is to have the current quarter match and do not create confusion due
    -- timing issues.
    WHERE
        snap.snapshot_fiscal_quarter_date >= DATEADD(MONTH, -18, CURRENT_DATE)
        AND snap.snapshot_fiscal_quarter_date != current_quarter_date.current_fiscal_quarter_date
        AND snap.pipeline_created_fiscal_quarter_date = snap.snapshot_fiscal_quarter_date
        AND is_eligible_created_pipeline_flag = 1

),

opty_aggregated AS (
    SELECT

        -------
        -------
        -- DIMENSIONS

        owner_id,
        opportunity_owner,

        account_id,
        account_name,

        report_opportunity_user_business_unit,
        report_opportunity_user_sub_business_unit,
        report_opportunity_user_division,
        report_opportunity_user_asm,
        report_opportunity_user_role_type,

        deal_size_bin,
        age_bin,
        partner_category,
        sales_qualified_source,
        stage_name,
        order_type_stamped,
        deal_group,
        sales_type,
        forecast_category_name,
        product_category_tier,
        product_category_deployment,
        industry,

        parent_crm_account_upa_country_name,

        is_web_portal_purchase,
        is_open,
        is_eligible_open_pipeline_flag,
        is_stage_1_plus,
        is_stage_3_plus,
        fpa_master_bookings_flag,

        CASE
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) < 3
                THEN 'CQ'
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) < 6
                THEN 'CQ+1'
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) < 9
                THEN 'CQ+2'
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) < 12
                THEN 'CQ+3'
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) >= 12
                THEN 'CQ+4 >'
        END                                  AS pipeline_landing_quarter,
        -----------------------------------------------
        CURRENT_DATE                         AS snapshot_date,
        pipeline_created_fiscal_quarter_date AS pipeline_created_date,

        -----------------------------------------------
        -- Dimensions for Detail / Aggregated

        SUM(net_arr)                         AS net_arr,
        SUM(booked_net_arr)                  AS booked_net_arr,
        SUM(open_1plus_net_arr)              AS open_1plus_net_arr,

        SUM(calculated_deal_count)           AS deal_count,
        SUM(booked_deal_count)               AS booked_deal_count,
        AVG(cycle_time_in_days)              AS age_in_days

    FROM opty_base
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31

),

snap_aggregated AS (
    SELECT

        -------
        -------
        -- DIMENSIONS

        owner_id,
        opportunity_owner,

        account_id,
        account_name,

        report_opportunity_user_business_unit,
        report_opportunity_user_sub_business_unit,
        report_opportunity_user_division,
        report_opportunity_user_asm,
        report_opportunity_user_role_type,

        deal_size_bin,
        age_bin,
        partner_category,
        sales_qualified_source,
        stage_name,
        order_type_stamped,
        deal_group,
        sales_type,
        forecast_category_name,
        product_category_tier,
        product_category_deployment,
        industry,

        parent_crm_account_upa_country_name,

        is_web_portal_purchase,
        is_open,
        is_eligible_open_pipeline_flag,
        is_stage_1_plus,
        is_stage_3_plus,
        fpa_master_bookings_flag,

        CASE
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) < 3
                THEN 'CQ'
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) < 6
                THEN 'CQ+1'
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) < 9
                THEN 'CQ+2'
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) < 12
                THEN 'CQ+3'
            WHEN DATEDIFF(MONTH, pipeline_created_fiscal_quarter_date, close_fiscal_quarter_date) >= 12
                THEN 'CQ+4 >'
        END                                  AS pipeline_landing_quarter,
        -----------------------------------------------
        snapshot_fiscal_quarter_date         AS snapshot_date,
        pipeline_created_fiscal_quarter_date AS pipeline_created_date,

        -----------------------------------------------
        -- Dimensions for Detail / Aggregated

        SUM(net_arr)                         AS net_arr,
        SUM(booked_net_arr)                  AS booked_net_arr,
        SUM(open_1plus_net_arr)              AS open_1plus_net_arr,

        SUM(calculated_deal_count)           AS deal_count,
        SUM(booked_deal_count)               AS booked_deal_count,
        AVG(cycle_time_in_days)              AS age_in_days

    FROM snap_base
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31
),

aggregated AS (

    SELECT *
    FROM snap_aggregated
    UNION
    SELECT *
    FROM opty_aggregated

),

final AS (

    SELECT
        aggregated.*,

        COALESCE(aggregated.pipeline_created_date = current_fiscal_quarter_date, FALSE)                    AS is_cfq_flag,

        COALESCE(aggregated.pipeline_created_date = DATEADD(MONTH, 3, current_fiscal_quarter_date), FALSE) AS is_cfq_plus_1_flag,

        COALESCE(aggregated.pipeline_created_date = DATEADD(MONTH, 6, current_fiscal_quarter_date), FALSE) AS is_cfq_plus_2_flag,

        COALESCE(
            pipeline_created_date <= current_fiscal_quarter_date
            AND pipeline_created_date >= DATEADD(MONTH, -15, current_fiscal_quarter_date), FALSE
        )                                                                                                  AS is_pipe_gen_range_flag,
        COALESCE(
            aggregated.is_open = TRUE
            AND aggregated.is_stage_1_plus = 1,
            FALSE
        )                                                                                                  AS is_open_stage_1_plus,

        COALESCE(
            aggregated.is_open = TRUE
            AND aggregated.is_stage_3_plus = 1,
            FALSE
        )                                                                                                  AS is_open_stage_3_plus,

        pipeline_date.fiscal_year                                                                          AS pipeline_created_fiscal_year,
        pipeline_date.fiscal_quarter_name_fy                                                               AS pipeline_created_fiscal_quarter_name,

        LOWER(
            CONCAT(
                aggregated.report_opportunity_user_business_unit,
                '_', aggregated.report_opportunity_user_sub_business_unit,
                '_', aggregated.report_opportunity_user_division,
                '_', aggregated.report_opportunity_user_asm,
                '_', aggregated.sales_qualified_source,
                '_', aggregated.deal_group
            )
        )                                                                                                  AS key_bu_subbu_division_asm_sqs_ot,

        LOWER(
            CONCAT(
                aggregated.report_opportunity_user_business_unit,
                '_', aggregated.report_opportunity_user_sub_business_unit
            )
        )                                                                                                  AS key_bu_subbu
    FROM aggregated
    CROSS JOIN current_quarter_date
    LEFT JOIN date_details AS pipeline_date
        ON pipeline_date.date_actual = aggregated.pipeline_created_date
    WHERE (
        net_arr != 0
        OR booked_net_arr != 0
    )
)

SELECT *
FROM final
