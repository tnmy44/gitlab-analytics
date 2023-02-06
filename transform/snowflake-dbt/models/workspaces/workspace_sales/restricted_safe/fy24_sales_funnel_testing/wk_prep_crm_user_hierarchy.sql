{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('wk_prep_crm_user_daily_snapshot', 'wk_prep_crm_user_daily_snapshot'),
    ('wk_prep_crm_opportunity', 'wk_prep_crm_opportunity'),
    ('wk_prep_sales_funnel_target', 'wk_prep_sales_funnel_target'),
    ('wk_prep_sales_funnel_partner_alliance_target', 'wk_prep_sales_funnel_partner_alliance_target')

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
      AND IFF(dim_date.fiscal_year < dim_date.current_fiscal_year,dim_date.date_actual = dim_date.last_day_of_fiscal_year, 1=1) -- take only the last valid hierarchy of the fiscal year for previous fiscal years

), user_hierarchy_sheetload AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the TOPO model, we will union in the distinct hierarchy values from the file.
*/

    SELECT DISTINCT 
      wk_prep_sales_funnel_target.fiscal_year,
      wk_prep_sales_funnel_target.user_segment,
      wk_prep_sales_funnel_target.user_geo,
      wk_prep_sales_funnel_target.user_region,
      wk_prep_sales_funnel_target.user_area,
      wk_prep_sales_funnel_target.user_business_unit,
      wk_prep_sales_funnel_target.dim_crm_user_hierarchy_sk
    FROM wk_prep_sales_funnel_target
    WHERE wk_prep_sales_funnel_target.user_area != 'N/A'
      AND wk_prep_sales_funnel_target.user_segment IS NOT NULL
      AND wk_prep_sales_funnel_target.user_geo IS NOT NULL
      AND wk_prep_sales_funnel_target.user_region IS NOT NULL
      AND wk_prep_sales_funnel_target.user_area IS NOT NULL

), user_hierarchy_sheetload_partner_alliance AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the TOPO model, we will union in the distinct hierarchy values from the partner and alliance file.
*/

    SELECT DISTINCT 
      wk_prep_sales_funnel_partner_alliance_target.fiscal_year,
      wk_prep_sales_funnel_partner_alliance_target.user_segment,
      wk_prep_sales_funnel_partner_alliance_target.user_geo,
      wk_prep_sales_funnel_partner_alliance_target.user_region,
      wk_prep_sales_funnel_partner_alliance_target.user_area,
      wk_prep_sales_funnel_partner_alliance_target.user_business_unit,
      wk_prep_sales_funnel_partner_alliance_target.dim_crm_user_hierarchy_sk
    FROM wk_prep_sales_funnel_partner_alliance_target
    WHERE wk_prep_sales_funnel_partner_alliance_target.user_area != 'N/A'
      AND wk_prep_sales_funnel_partner_alliance_target.area IS NOT NULL

), user_hierarchy_stamped_opportunity AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the stamped opportunities, we will union in the distinct hierarchy values from the stamped opportunities.
*/

    SELECT DISTINCT
      dim_date.fiscal_year,
      wk_prep_crm_opportunity.user_segment_stamped                      AS user_segment,
      wk_prep_crm_opportunity.user_geo_stamped                          AS user_geo,
      wk_prep_crm_opportunity.user_region_stamped                       AS user_region,
      wk_prep_crm_opportunity.user_area_stamped                         AS user_area,
      wk_prep_crm_opportunity.user_business_unit_stamped                AS user_business_unit,
      wk_prep_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk    AS dim_crm_user_hierarchy_sk
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
      fiscal_year,
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
      fiscal_year,
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

), current_fiscal_year AS(

    SELECT DISTINCT
      fiscal_year
    FROM dim_date
    WHERE date_actual = '2023-02-02' -- Pretend like its 2024

), final AS (

    SELECT DISTINCT 
      {{ dbt_utils.surrogate_key(['final_unioned.dim_crm_user_hierarchy_sk', 'final_unioned.fiscal_year']) }}                   AS dim_crm_user_hierarchy_id,
      final_unioned.dim_crm_user_hierarchy_sk,
      final_unioned.fiscal_year,
      final_unioned.user_business_unit                                                                                          AS crm_user_business_unit,
      {{ dbt_utils.surrogate_key(['final_unioned.user_business_unit']) }}                                                       AS dim_crm_user_business_unit_id,
      final_unioned.user_segment                                                                                                AS crm_user_sales_segment,
      {{ dbt_utils.surrogate_key(['final_unioned.user_segment']) }}                                                             AS dim_crm_user_sales_segment_id,
      final_unioned.user_geo                                                                                                    AS crm_user_geo,
      {{ dbt_utils.surrogate_key(['final_unioned.user_geo']) }}                                                                 AS dim_crm_user_geo_id,
      final_unioned.user_region                                                                                                 AS crm_user_region,
      {{ dbt_utils.surrogate_key(['final_unioned.user_region']) }}                                                              AS dim_crm_user_region_id,
      final_unioned.user_area                                                                                                   AS crm_user_area,
      {{ dbt_utils.surrogate_key(['final_unioned.user_area']) }}                                                                AS dim_crm_user_area_id,
      CASE
          WHEN final_unioned.user_segment IN ('Large', 'PubSec') THEN 'Large'
          ELSE final_unioned.user_segment
      END                                                                                                                       AS crm_user_sales_segment_grouped,
      {{ sales_segment_region_grouped('final_unioned.user_segment', 'final_unioned.user_geo', 'final_unioned.user_region') }}   AS crm_user_sales_segment_region_grouped,
      IFF(final_unioned.fiscal_year = current_fiscal_year.fiscal_year, 1, 0)                                                    AS is_current_crm_user_hierarchy
    FROM final_unioned
    LEFT JOIN current_fiscal_year
      ON final_unioned.fiscal_year = current_fiscal_year.fiscal_year

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-01-17",
    updated_date="2023-02-01"
) }}