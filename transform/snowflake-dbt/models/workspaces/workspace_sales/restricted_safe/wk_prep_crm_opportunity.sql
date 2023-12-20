{{ simple_cte([
    ('prep_crm_opportunity', 'prep_crm_opportunity'),
    ('sfdc_opportunity_snapshots_source', 'sfdc_opportunity_snapshots_source'),
    ('sfdc_opportunity_source', 'sfdc_opportunity_source'),
    ('sfdc_account_snapshot', 'prep_crm_account_daily_snapshot'),
    ('dim_date', 'dim_date')
]) }},

sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL

),

snapshot_dates AS (

    SELECT *
    FROM dim_date
    WHERE date_actual::DATE >= '2020-02-01' -- Restricting snapshot model to only have data from this date forward. More information https://gitlab.com/gitlab-data/analytics/-/issues/14418#note_1134521216
      AND date_actual < CURRENT_DATE

    {% if is_incremental() %}

      AND date_actual > (SELECT MAX(snapshot_date) FROM {{ this }} WHERE is_live = 0)

    {% endif %}

), live_date AS (

    SELECT *
    FROM dim_date
    WHERE date_actual = CURRENT_DATE

),

sfdc_opportunity_snapshot AS (

    SELECT
      sfdc_opportunity_snapshots_source.opportunity_id                                                              AS dim_crm_opportunity_id,
      sfdc_opportunity_snapshots_source.order_type_stamped                                                          AS order_type_snapshot,
      {{ sales_qualified_source_cleaning('sfdc_opportunity_snapshots_source.sales_qualified_source') }}             AS sales_qualified_source_snapshot,
      sfdc_opportunity_snapshots_source.user_segment_stamped                                                        AS crm_opp_owner_sales_segment_stamped_snapshot,
      sfdc_opportunity_snapshots_source.user_geo_stamped                                                            AS crm_opp_owner_geo_stamped_snapshot,
      sfdc_opportunity_snapshots_source.user_region_stamped                                                         AS crm_opp_owner_region_stamped_snapshot,
      sfdc_opportunity_snapshots_source.user_area_stamped                                                           AS crm_opp_owner_area_stamped_snapshot,
      sfdc_opportunity_snapshots_source.user_segment_geo_region_area_stamped                                        AS crm_opp_owner_sales_segment_geo_region_area_stamped_snapshot,
      sfdc_opportunity_snapshots_source.user_business_unit_stamped                                                  AS crm_opp_owner_business_unit_stamped_snapshot,
      sfdc_opportunity_snapshots_source.is_edu_oss                                                                  AS is_edu_oss_snapshot,
      sfdc_opportunity_snapshots_source.opportunity_category                                                        AS opportunity_category_snapshot,
      sfdc_opportunity_snapshots_source.is_deleted                                                                  AS is_deleted_snapshot,
      sfdc_opportunity_snapshots_source.forecast_category_name                                                      AS forecast_category_name_snapshot,
      sfdc_opportunity_snapshots_source.fpa_master_bookings_flag                                                    AS fpa_master_bookings_flag_snapshot,

      CASE
       WHEN sfdc_opportunity_snapshots_source.order_type = '1. New - First Order'
         THEN '1. New'
       WHEN sfdc_opportunity_snapshots_source.order_type IN ('2. New - Connected', '3. Growth', '5. Churn - Partial','6. Churn - Final','4. Contraction')
         THEN '2. Growth'
       ELSE '3. Other'
     END                                                                                                            AS deal_group_snapshot,
      
      IFF(LOWER(sfdc_opportunity_snapshots_source.sales_type) like '%renewal%', 1, 0)                               AS is_renewal_snapshot,
      sfdc_opportunity_snapshots_source.is_closed                                                                   AS is_closed_snapshot,
      sfdc_opportunity_snapshots_source.is_web_portal_purchase                                                      AS is_web_portal_purchase_snapshot,
      sfdc_opportunity_snapshots_source.is_won                                                                      AS is_won_snapshot,
      sfdc_opportunity_snapshots_source.stage_name                                                                  AS stage_name_snapshot,
      sfdc_opportunity_snapshots_source.fiscal_quarter_name_fy                                                      AS pipeline_created_fiscal_quarter_name_snapshot,
      sfdc_opportunity_snapshots_source.first_day_of_fiscal_quarter                                                 AS pipeline_created_fiscal_quarter_date_snapshot,
      sfdc_opportunity_snapshots_source.fiscal_quarter_name_fy                                                      AS close_fiscal_quarter_name_snapshot,
      sfdc_opportunity_snapshots_source.order_type_grouped                                                          AS order_type_grouped_snapshot,
      sfdc_opportunity_snapshots_source.created_date::DATE                                                          AS created_date_snapshot,
      sfdc_opportunity_snapshots_source.sales_accepted_date::DATE                                                   AS sales_accepted_date_snapshot,
      sfdc_opportunity_snapshots_source.close_date::DATE                                                            AS close_date_snapshot,
      sfdc_opportunity_snapshots_source.net_arr                                                                     AS net_arr_snapshot,
      {{ dbt_utils.surrogate_key(['sfdc_opportunity_snapshots_source.opportunity_id','snapshot_dates.date_id'])}}   AS crm_opportunity_snapshot_id,
      sfdc_account_snapshot.is_jihu_account,
      CASE
        WHEN sfdc_opportunity_snapshots_source.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 
                                                              'Closed Won', '10-Duplicate')
            THEN 0
        ELSE 1
      END                                                                                                           AS is_open_snapshot,
      sfdc_opportunity_snapshots_source.iacv_created_date                                                           AS arr_created_date_snapshot,                                                                    
      0 AS is_live
    FROM sfdc_opportunity_snapshots_source
    INNER JOIN snapshot_dates
      ON sfdc_opportunity_snapshots_source.dbt_valid_from::DATE <= snapshot_dates.date_actual
        AND (sfdc_opportunity_snapshots_source.dbt_valid_to::DATE > snapshot_dates.date_actual OR sfdc_opportunity_snapshots_source.dbt_valid_to IS NULL)
    LEFT JOIN sfdc_account_snapshot
      ON sfdc_opportunity_snapshots_source.account_id = sfdc_account_snapshot.dim_crm_account_id
        AND snapshot_dates.date_id = sfdc_account_snapshot.snapshot_id
    LEFT JOIN snapshot_dates AS close_date
      ON sfdc_opportunity_snapshots_source.close_date = close_date.date_actual
    WHERE sfdc_opportunity_snapshots_source.account_id IS NOT NULL
      AND sfdc_opportunity_snapshots_source.is_deleted = FALSE
    

), sfdc_opportunity_live AS (

    SELECT
      {{ dbt_utils.surrogate_key(['sfdc_opportunity_snapshots_source.opportunity_id','snapshot_dates.date_id'])}}   AS crm_opportunity_snapshot_id,
      sfdc_opportunity_source.opportunity_id                                                                        AS dim_crm_opportunity_id,
      sfdc_opportunity_source.order_type_stamped                                                                    AS order_type_live,
      {{ sales_qualified_source_cleaning('sfdc_opportunity_source.sales_qualified_source') }}                       AS sales_qualified_source_live,
      sfdc_opportunity_source.user_segment_stamped                                                                  AS crm_opp_owner_sales_segment_stamped_live,
      sfdc_opportunity_source.user_geo_stamped                                                                      AS crm_opp_owner_geo_stamped_live,
      sfdc_opportunity_source.user_region_stamped                                                                   AS crm_opp_owner_region_stamped_live,
      sfdc_opportunity_source.user_area_stamped                                                                     AS crm_opp_owner_area_stamped_live,
      sfdc_opportunity_source.user_segment_geo_region_area_stamped                                                  AS crm_opp_owner_sales_segment_geo_region_area_stamped_live,
      sfdc_opportunity_source.user_business_unit_stamped                                                            AS crm_opp_owner_business_unit_stamped_live,
      sfdc_opportunity_source.is_edu_oss                                                                            AS is_edu_oss_live,
      sfdc_opportunity_source.opportunity_category                                                                  AS opportunity_category_live,
      sfdc_opportunity_source.is_deleted                                                                            AS is_deleted_live,      
      sfdc_opportunity_source.forecast_category_name                                                                AS forecast_category_name_live,
      sfdc_opportunity_source.fpa_master_bookings_flag_live                                                         AS fpa_master_bookings_flag_live,

      CASE
       WHEN sfdc_opportunity_source.order_type = '1. New - First Order'
         THEN '1. New'
       WHEN sfdc_opportunity_source.order_type IN ('2. New - Connected', '3. Growth', '5. Churn - Partial','6. Churn - Final','4. Contraction')
         THEN '2. Growth'
       ELSE '3. Other'
     END                                                                                                            AS deal_group_live,
      IFF(LOWER(sfdc_opportunity_source.sales_type) like '%renewal%', 1, 0)                                         AS is_renewal_live,
      sfdc_opportunity_source.is_closed                                                                             AS is_closed_live,
      sfdc_opportunity_source.is_web_portal_purchase                                                                AS is_web_portal_purchase_live,
      sfdc_opportunity_source.is_won                                                                                AS is_won_live,
      sfdc_opportunity_source.stage_name                                                                            AS stage_name_live,
      sfdc_opportunity_source.fiscal_quarter_name_fy                                                                AS pipeline_created_fiscal_quarter_name_live,
      sfdc_opportunity_source.first_day_of_fiscal_quarter                                                           AS pipeline_created_fiscal_quarter_date_live,
      sfdc_opportunity_source.fiscal_quarter_name_fy                                                                AS close_fiscal_quarter_name_live,
      live_date.order_type_grouped                                                                                  AS order_type_grouped_live,
      sfdc_opportunity_source.created_date::DATE                                                                    AS created_date_live,
      sfdc_opportunity_source.sales_accepted_date::DATE                                                             AS sales_accepted_date_live,
      sfdc_opportunity_source.close_date::DATE                                                                      AS close_date_live,
      sfdc_opportunity_source.net_arr                                                                               AS net_arr_live,
      {{ dbt_utils.surrogate_key(['sfdc_opportunity_source.opportunity_id',"'99991231'"])}}                         AS crm_opportunity_snapshot_id,
      sfdc_account.is_jihu_account                                                                                  AS is_jihu_account_live,
      sfdc_opportunity_source.iacv_created_date                                                                     AS arr_created_date_live,   
      CASE
        WHEN sfdc_opportunity_source.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 
                                                    'Closed Won', '10-Duplicate')
            THEN 0
        ELSE 1
      END                                                                                                           AS is_open_live,
      1                                                                                                             AS is_live
    FROM sfdc_opportunity_source
    LEFT JOIN live_date
      ON CURRENT_DATE() = live_date.date_actual
    LEFT JOIN sfdc_account
      ON sfdc_opportunity_source.account_id= sfdc_account.account_id
    WHERE sfdc_opportunity_source.account_id IS NOT NULL
      AND sfdc_opportunity_source.is_deleted = FALSE

)


SELECT 
  prep_crm_opportunity.*,
  COALESCE(
    sfdc_opportunity.is_pipeline_created_eligible,
    CASE
      WHEN sfdc_opportunity_live.order_type_live IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
        AND sfdc_opportunity_live.is_edu_oss_live  = 0
        AND prep_crm_opportunity.arr_created_fiscal_quarter_date IS NOT NULL
        AND sfdc_opportunity_live.opportunity_category_live  IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')
        -- 20211222 Adjusted to remove the ommitted filter
        AND sfdc_opportunity_snapshot.stage_name_snapshot NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
        AND (net_arr > 0
          OR sfdc_opportunity_live.opportunity_category_live  = 'Credit')
        -- 20220128 Updated to remove webdirect SQS deals
        AND sfdc_opportunity_live.sales_qualified_source_live   != 'Web Direct Generated'
        AND sfdc_opportunity_live.is_jihu_account_live = 0
      THEN 1
      ELSE 0
    END
  )                                                                                                                 AS is_net_arr_pipeline_created_combined,
  CASE
    WHEN LOWER(sfdc_opportunity_live.order_type_grouped_live) LIKE ANY ('%growth%', '%new%')
      AND sfdc_opportunity_live.is_edu_oss_live = 0
      AND prep_crm_opportunity.is_stage_1_plus = 1
      AND sfdc_opportunity_snapshot.forecast_category_name_snapshot != 'Omitted'
      AND sfdc_opportunity_snapshot.is_open_snapshot = 1
      THEN 1
      ELSE 0
  END                                                                                                               AS is_eligible_open_pipeline_combined,
  CASE
    WHEN
    prep_crm_opportunity.dim_parent_crm_account_id IN (
      '001610000111bA3',
      '0016100001F4xla',
      '0016100001CXGCs',
      '00161000015O9Yn',
      '0016100001b9Jsc'
    )
    AND prep_crm_opportunity.close_date < '2020-08-01'
    THEN 1
  -- NF 2021 - Pubsec extreme deals
    WHEN
      prep_crm_opportunity.dim_crm_opportunity_id IN ('0064M00000WtZKUQA3', '0064M00000Xb975QAB')
      AND (prep_crm_opportunity.snapshot_date < '2021-05-01' OR prep_crm_opportunity.is_live = 1)
      THEN 1
    -- exclude vision opps from FY21-Q2
    WHEN prep_crm_opportunity.arr_created_fiscal_quarter_name = 'FY21-Q2'
      AND prep_crm_opportunity.snapshot_day_of_fiscal_quarter_normalised = 90
      AND sfdc_opportunity_snapshot.stage_name_snapshot IN (
        '00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
      )
      THEN 1
    -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
    WHEN
      prep_crm_opportunity.dim_crm_opportunity_id IN (
        '0064M00000ZGpfQQAT', '0064M00000ZGpfVQAT', '0064M00000ZGpfGQAT'
      )
      THEN 1
      -- remove test accounts
      WHEN
        prep_crm_opportunity.dim_crm_account_id = '0014M00001kGcORQA0'
        THEN 1
      --remove test accounts
      WHEN (prep_crm_opportunity.dim_parent_crm_account_id = ('0016100001YUkWVAA1')
          OR prep_crm_opportunity.dim_crm_account_id IS NULL)
        THEN 1
      -- remove jihu accounts
      WHEN sfdc_opportunity_live.is_jihu_account_live = 1
        THEN 1
      -- remove deleted opps
      WHEN sfdc_opportunity_snapshot.is_deleted_snapshot = 1
        THEN 1
        ELSE 0
  END                                                                                                               AS is_excluded_from_pipeline_created_combined,
  CASE
    WHEN prep_crm_opportunity.arr_created_fiscal_quarter_date = prep_crm_opportunity.close_fiscal_quarter_date
      AND is_net_arr_pipeline_created_combined = 1
        THEN net_arr_snapshot
    ELSE 0
  END                                                                                                               AS created_and_won_same_quarter_net_arr_combined,
  fpa_master_bookings_flag_snapshot,
  CASE
    WHEN sfdc_opportunity_live.is_edu_oss_live = 0
      AND sfdc_opportunity_snapshot.is_deleted_snapshot = 0
      AND sfdc_opportunity_snapshot.is_renewal_snapshot = 0
      AND sfdc_opportunity_live.order_type_live IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
      AND sfdc_opportunity_live.opportunity_category IN ('Standard','Ramp Deal','Decommissioned')
      AND sfdc_opportunity_snapshot.is_web_portal_purchase_snapshot = 0
        THEN 1
      ELSE 0
  END                                                                                                               AS is_eligible_age_analysis_combined,
  CASE
    WHEN sfdc_opportunity_snapshot.is_renewal_snapshot = 1 AND sfdc_opportunity_snapshot.is_closed_snapshot = 1
        THEN DATEDIFF(day, arr_created_date_snapshot, close_date_snapshot)
    WHEN sfdc_opportunity_snapshot.is_renewal_snapshot = 0 AND sfdc_opportunity_snapshot.is_closed_snapshot = 1
        THEN DATEDIFF(day, sfdc_opportunity_snapshot.created_date_snapshot, close_date_snapshot)
      WHEN sfdc_opportunity_snapshot.is_renewal_snapshot = 1 AND sfdc_opportunity_snapshot.is_open_snapshot = 1
        THEN DATEDIFF(day, arr_created_date_snapshot, prep_crm_opportunity.snapshot_date)
    WHEN sfdc_opportunity_snapshot.is_renewal_snapshot = 0 AND sfdc_opportunity_snapshot.is_open_snapshot = 1
        THEN DATEDIFF(day, sfdc_opportunity_snapshot.created_date_snapshot, prep_crm_opportunity.snapshot_date)
  END                                                                                                               AS cycle_time_in_days_combined
FROM prep_crm_opportunity
LEFT JOIN sfdc_opportunity_snapshot
  ON sfdc_opportunity_snapshot.dim_crm_opportunity_id = prep_crm_opportunity.dim_crm_opportunity_id
LEFT JOIN sfdc_opportunity_live
  ON sfdc_opportunity_live.dim_crm_opportunity_id = prep_crm_opportunity.dim_crm_opportunity_id
