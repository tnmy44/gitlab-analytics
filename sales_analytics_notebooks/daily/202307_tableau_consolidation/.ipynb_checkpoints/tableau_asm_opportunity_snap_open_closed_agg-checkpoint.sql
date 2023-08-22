/*

Open / Closed Won Historical AGGREGATED

*/
WITH date_details AS (

    SELECT *
    FROM prod.workspace_sales.date_details

), current_date AS (

    SELECT
        fiscal_year                      AS current_fiscal_year,
        date_actual                      AS current_calendar_date,
        current_fiscal_quarter_name_fy   AS current_fiscal_quarter_name,
        first_day_of_fiscal_quarter      AS current_fiscal_quarter_date,
        day_of_fiscal_quarter_normalised AS current_day_of_fiscal_quarter_normalized
    FROM date_details
    WHERE date_actual = DATEADD(DAY,-1,CURRENT_DATE)

), base AS (

    SELECT snap.*,

        CASE
            WHEN snap.cycle_time_in_days BETWEEN 1 AND 29
                THEN '[0,30)'
            WHEN snap.cycle_time_in_days BETWEEN 30 AND 179
                THEN '[30,180)'
            WHEN snap.cycle_time_in_days BETWEEN 179 AND 364
                THEN '[180,365)'
            WHEN snap.cycle_time_in_days > 364
                THEN '[365+)'
            ELSE 'Other'
        END                  AS age_bin,

        calculated_deal_size AS deal_size_bin

    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf snap
        INNER JOIN current_date
            ON snap.snapshot_day_of_fiscal_quarter_normalised = current_day_of_fiscal_quarter_normalized
            -- for open pipeline CQ and Booked pipeline
            AND snap.close_fiscal_quarter_date = snap.snapshot_fiscal_quarter_date
    WHERE snap.snapshot_fiscal_quarter_date >= DATEADD(MONTH,-18, current_date)

), aggregated AS (
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
        --report_opportunity_user_role_type,

        deal_size_bin,
        age_bin,
        --partner_category,
        sales_qualified_source,
        stage_name,
        order_type_stamped,
        deal_group,
        sales_type,
        forecast_category_name,
        product_category_tier,
        product_category_deployment,

        parent_crm_account_upa_country_name,

        is_web_portal_purchase,
        is_open,
        is_stage_1_plus,
        is_stage_3_plus,
        fpa_master_bookings_flag,

        -----------------------------------------------
        close_fiscal_quarter_date               AS close_date,
        snapshot_fiscal_quarter_date            AS snapshot_date,
        created_fiscal_quarter_date             AS created_date,
        pipeline_created_fiscal_quarter_date    AS pipeline_created_date,

        -----------------------------------------------
        -- Dimensions for Detail / Aggregated

        SUM(net_arr)                                AS net_arr,
        SUM(booked_net_arr)                         AS booked_net_arr,
        SUM(open_1plus_net_arr)                     AS open_1plus_net_arr,

        SUM(calculated_deal_count)                  AS deal_count,
        SUM(booked_deal_count)                      AS booked_deal_count,
        AVG(cycle_time_in_days)                     AS age_in_days

    FROM base
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28
), final AS (

    SELECT 
        aggregated.*,

        COALESCE(aggregated.close_date >= current_fiscal_quarter_date
                     AND aggregated.close_date <= DATEADD(MONTH, 15, current_fiscal_quarter_date),
                 FALSE)                                                                         AS is_open_pipeline_range_flag,
        COALESCE(aggregated.close_date <= current_fiscal_quarter_date
                     AND aggregated.close_date >= DATEADD(MONTH, -15, current_fiscal_quarter_date),
                 FALSE)                                                                         AS is_bookings_range_flag,

        COALESCE(aggregated.is_open = TRUE
                     AND aggregated.is_stage_1_plus = 1,
                 FALSE)                                                                         AS is_open_stage_1_plus,

        COALESCE(aggregated.is_open = TRUE
                     AND aggregated.is_stage_3_plus = 1,
                 FALSE)                                                                         AS is_open_stage_3_plus,

        close_date.fiscal_year                                                                 AS close_fiscal_year,
        close_date.fiscal_quarter_name_fy                                                      AS close_fiscal_quarter_name,

        LOWER(
                           CONCAT(
                                   aggregated.report_opportunity_user_business_unit,
                                   '_', aggregated.report_opportunity_user_sub_business_unit,
                                   '_', aggregated.report_opportunity_user_division,
                                   '_', aggregated.report_opportunity_user_asm,
                                   '_', aggregated.sales_qualified_source,
                                   '_', aggregated.deal_group
                               )
                       )                                                                        AS key_bu_subbu_division_asm_sqs_ot,

        LOWER(
                           CONCAT(
                                   aggregated.report_opportunity_user_business_unit,
                                   '_', aggregated.report_opportunity_user_sub_business_unit
                               )
                       )                                                                        AS key_bu_subbu
    FROM aggregated
    CROSS JOIN current_date
    LEFT JOIN date_details AS close_date
        ON close_date.date_actual = aggregated.close_date
    WHERE (
                net_arr != 0
                  OR booked_net_arr != 0
              )
)

SELECT *
FROM final
