{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('wk_prep_crm_user_daily_snapshot', 'wk_prep_crm_user_daily_snapshot'),
    ('wk_prep_crm_opportunity', 'wk_prep_crm_opportunity')

]) }}

, fiscal_months AS (

    SELECT DISTINCT
      fiscal_month_name_fy,
      fiscal_year,
      first_day_of_month
    FROM dim_date

), sheetload_sales_funnel_targets_matrix_source AS (

    SELECT 
      sheetload_sales_funnel_targets_matrix_source.*
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source') }}

), sheetload_sales_funnel_partner_alliance_targets_matrix_source AS (

    SELECT 
      sheetload_sales_funnel_partner_alliance_targets_matrix_source.*
    FROM {{ ref('sheetload_sales_funnel_partner_alliance_targets_matrix_source') }}
  
), user_hierarchy_source AS (

    SELECT 
      DISTINCT 
      dim_date.fiscal_year,
      wk_prep_crm_user_daily_snapshot.crm_user_sales_segment              AS user_segment,
      wk_prep_crm_user_daily_snapshot.crm_user_geo                        AS user_geo,
      wk_prep_crm_user_daily_snapshot.crm_user_region                     AS user_region,
      wk_prep_crm_user_daily_snapshot.crm_user_area                       AS user_area,
      wk_prep_crm_user_daily_snapshot.crm_user_business_unit              AS user_business_unit,
      wk_prep_crm_user_daily_snapshot.dim_crm_user_hierarchy_sk
    FROM wk_prep_crm_user_daily_snapshot
    INNER JOIN dim_date 
      ON wk_prep_crm_user_daily_snapshot.snapshot_id = dim_date.date_id
    WHERE wk_prep_crm_user_daily_snapshot.crm_user_sales_segment IS NOT NULL
      AND wk_prep_crm_user_daily_snapshot.crm_user_geo IS NOT NULL
      AND wk_prep_crm_user_daily_snapshot.crm_user_region IS NOT NULL
      AND wk_prep_crm_user_daily_snapshot.crm_user_area IS NOT NULL

), user_hierarchy_sheetload AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the TOPO model, we will union in the distinct hierarchy values from the file.
*/

    SELECT DISTINCT 
      fiscal_months.fiscal_year,
      sheetload_sales_funnel_targets_matrix_source.user_segment,
      sheetload_sales_funnel_targets_matrix_source.user_geo,
      sheetload_sales_funnel_targets_matrix_source.user_region,
      sheetload_sales_funnel_targets_matrix_source.user_area,
      CASE
        WHEN fiscal_months.fiscal_year >= 2024
          THEN 'COMM'
        ELSE 'none'
      END AS user_business_unit,
      CASE
        WHEN fiscal_months.fiscal_year BETWEEN '2021' AND '2023'
          THEN CONCAT(sheetload_sales_funnel_targets_matrix_source.user_segment, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_area
                      )
        WHEN fiscal_months.fiscal_year >= 2024 AND LOWER(user_business_unit) = 'comm'
          THEN CONCAT(user_business_unit, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_segment, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_area
                      )
        WHEN fiscal_months.fiscal_year >= 2024 AND LOWER(user_business_unit) = 'entg'
          THEN CONCAT(user_business_unit, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_area, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_segment
                      )
        END                                                                                                                           AS dim_crm_user_hierarchy_sk
    FROM sheetload_sales_funnel_targets_matrix_source
    INNER JOIN fiscal_months
      ON sheetload_sales_funnel_targets_matrix_source.month = fiscal_months.fiscal_month_name_fy
    WHERE sheetload_sales_funnel_targets_matrix_source.user_area != 'N/A'
      AND sheetload_sales_funnel_targets_matrix_source.user_segment IS NOT NULL
      AND sheetload_sales_funnel_targets_matrix_source.user_geo IS NOT NULL
      AND sheetload_sales_funnel_targets_matrix_source.user_region IS NOT NULL
      AND sheetload_sales_funnel_targets_matrix_source.user_area IS NOT NULL

), user_hierarchy_sheetload_partner_alliance AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the TOPO model, we will union in the distinct hierarchy values from the partner and alliance file.
*/

    SELECT DISTINCT 
      fiscal_months.fiscal_year,
      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment,
      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo,
      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region,
      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area,
      'none' AS user_business_unit,
      CASE
        WHEN fiscal_months.fiscal_year BETWEEN '2021' AND '2023'
          THEN CONCAT(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment,
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area
                      )
        WHEN fiscal_months.fiscal_year >= 2024 AND LOWER(user_business_unit) = 'comm'
          THEN CONCAT(user_business_unit,
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment, 
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area
                      )
        WHEN fiscal_months.fiscal_year >= 2024 AND LOWER(user_business_unit) = 'entg'
          THEN CONCAT(user_business_unit,
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area, 
                      '-',
                      sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment
                      )
        END                                                                                                                           AS dim_crm_user_hierarchy_sk
    FROM sheetload_sales_funnel_partner_alliance_targets_matrix_source
    INNER JOIN fiscal_months
      ON sheetload_sales_funnel_partner_alliance_targets_matrix_source.month = fiscal_months.fiscal_month_name_fy
    WHERE sheetload_sales_funnel_partner_alliance_targets_matrix_source.area != 'N/A'
      AND sheetload_sales_funnel_partner_alliance_targets_matrix_source.area IS NOT NULL

), user_hierarchy_stamped_opportunity AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the stamped opportunities, we will union in the distinct hierarchy values from the stamped opportunities.
*/

    SELECT DISTINCT
      dim_date.fiscal_year,
      wk_prep_crm_opportunity.user_segment_stamped                  AS user_segment,
      wk_prep_crm_opportunity.user_geo_stamped                      AS user_geo,
      wk_prep_crm_opportunity.user_region_stamped                   AS user_region,
      wk_prep_crm_opportunity.user_area_stamped                     AS user_area,
      wk_prep_crm_opportunity.user_business_unit_stamped            AS user_business_unit,
      wk_prep_crm_opportunity.dim_crm_opp_owner_hierarchy_sk        AS dim_crm_user_hierarchy_sk
    FROM wk_prep_crm_opportunity
    INNER JOIN dim_date 
      ON wk_prep_crm_opportunity.close_date = dim_date.date_actual
  
), unioned AS (
/*
  Full outer join with all three hierarchy sources and coalesce the fields, prioritizing the SFDC versions to maintain consistency in how the hierarchy appears
  The full outer join will allow all possible hierarchies to flow in from all three sources
*/

    SELECT *
    FROM user_hierarchy_source
  
    UNION
 
    SELECT *
    FROM user_hierarchy_sheetload

    UNION

    SELECT *
    FROM  user_hierarchy_sheetload_partner_alliance

    UNION

    SELECT *
    FROM user_hierarchy_stamped_opportunity

), pre_fy24_hierarchy AS (

    SELECT DISTINCT
      user_segment,
      user_geo,
      user_region,
      user_area,
      NULL        AS user_business_unit,
      dim_crm_user_hierarchy_sk
    FROM unioned 
    WHERE fiscal_year < 2024

), fy24_and_beyond_hierarchy AS (

    SELECT DISTINCT
      user_segment,
      user_geo,
      user_region,
      user_area,
      user_business_unit,
      dim_crm_user_hierarchy_sk
    FROM unioned 
    WHERE fiscal_year >= 2024

), final_unioned AS (


    SELECT *
    FROM pre_fy24_hierarchy

    UNION ALL 

    SELECT *
    FROM fy24_and_beyond_hierarchy

), final AS (

    SELECT DISTINCT 
      {{ dbt_utils.surrogate_key(['dim_crm_user_hierarchy_sk']) }}                    AS dim_crm_user_hierarchy_stamped_id,
      dim_crm_user_hierarchy_sk,
      user_business_unit                                                              AS crm_opp_owner_business_unit_stamped,
      {{ dbt_utils.surrogate_key(['user_business_unit']) }}                           AS dim_crm_opp_owner_business_unit_stamped_id,
      user_segment                                                                    AS crm_opp_owner_sales_segment_stamped,
      {{ dbt_utils.surrogate_key(['user_segment']) }}                                 AS dim_crm_opp_owner_sales_segment_stamped_id,
      user_geo                                                                        AS crm_opp_owner_geo_stamped,
      {{ dbt_utils.surrogate_key(['user_geo']) }}                                     AS dim_crm_opp_owner_geo_stamped_id,
      user_region                                                                     AS crm_opp_owner_region_stamped,
      {{ dbt_utils.surrogate_key(['user_region']) }}                                  AS dim_crm_opp_owner_region_stamped_id,
      user_area                                                                       AS crm_opp_owner_area_stamped,
      {{ dbt_utils.surrogate_key(['user_area']) }}                                    AS dim_crm_opp_owner_area_stamped_id,
      CASE
          WHEN user_segment IN ('Large', 'PubSec') THEN 'Large'
          ELSE user_segment
      END                                                                             AS crm_opp_owner_sales_segment_stamped_grouped,
      {{ sales_segment_region_grouped('user_segment', 'user_geo', 'user_region') }}   AS crm_opp_owner_sales_segment_region_stamped_grouped
    FROM final_unioned

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-01-17",
    updated_date="2023-01-17"
) }}