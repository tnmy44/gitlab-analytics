{{ simple_cte([
      ('sfdc_user_hierarchy_live', 'prep_crm_user_hierarchy_live'),
      ('sfdc_user_hierarchy_stamped', 'prep_crm_user_hierarchy_stamped'),
      ('sales_qualified_source', 'prep_sales_qualified_source'),
      ('order_type', 'prep_order_type'),
      ('alliance_type', 'prep_alliance_type_scd'),
      ('channel_type', 'prep_channel_type'),
      ('date_details_source', 'date_details_source')
])}}

, date AS (

   SELECT DISTINCT
     fiscal_month_name_fy,
     fiscal_year,
     first_day_of_month
   FROM date_details_source

), sheetload_sales_funnel_partner_alliance_targets_matrix_source AS (

    SELECT 
      sheetload_sales_funnel_partner_alliance_targets_matrix_source.*,
      {{ channel_type('sheetload_sales_funnel_partner_alliance_targets_matrix_source.sqs_bucket_engagement', 'sheetload_sales_funnel_partner_alliance_targets_matrix_source.order_type') }}   AS channel_type
    FROM {{ ref('sheetload_sales_funnel_partner_alliance_targets_matrix_source') }}

), target_matrix AS (

    SELECT
      sheetload_sales_funnel_partner_alliance_targets_matrix_source.*,
      date.first_day_of_month,
      date.fiscal_year,
      {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}                                                           AS dim_sales_qualified_source_id,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                                                   AS dim_order_type_id,
      {{ get_keyed_nulls('alliance_type.dim_alliance_type_id') }}                                                                             AS dim_alliance_type_id,
      {{ get_keyed_nulls('channel_type.dim_channel_type_id') }}                                                                               AS dim_channel_type_id,
      CONCAT(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment, 
             '-',
             sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo, 
             '-', 
             sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region, 
             '-', 
             sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area)                                                         AS crm_opp_owner_segment_geo_region_area
    FROM sheetload_sales_funnel_partner_alliance_targets_matrix_source
    LEFT JOIN date
      ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_partner_alliance_targets_matrix_source.month") }} = {{ sales_funnel_text_slugify("date.fiscal_month_name_fy") }}
    LEFT JOIN sales_qualified_source
      ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_partner_alliance_targets_matrix_source.sales_qualified_source") }} = {{ sales_funnel_text_slugify("sales_qualified_source.sales_qualified_source_name") }}
    LEFT JOIN order_type
      ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_partner_alliance_targets_matrix_source.order_type") }} = {{ sales_funnel_text_slugify("order_type.order_type_name") }}
    LEFT JOIN alliance_type
      ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_partner_alliance_targets_matrix_source.alliance_partner") }} = {{ sales_funnel_text_slugify("alliance_type.alliance_type_name") }}
    LEFT JOIN channel_type
      ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_partner_alliance_targets_matrix_source.channel_type") }} = {{ sales_funnel_text_slugify("channel_type.channel_type_name") }}

), user_hierarchy AS (
/* 
For FY23 and beyond, targets in the sheetload file were set at the user_segment_geo_region_area grain, so we join to the stamped hierarchy on the user_segment_geo_region_area.
*/

    SELECT *
    FROM sfdc_user_hierarchy_stamped
    WHERE is_last_user_hierarchy_in_fiscal_year = 1

), unioned_targets AS (

    SELECT
      target_matrix.kpi_name,
      target_matrix.first_day_of_month,
      target_matrix.dim_sales_qualified_source_id,
      target_matrix.sales_qualified_source,
      target_matrix.dim_order_type_id,
      target_matrix.order_type, 
      target_matrix.fiscal_year,
      target_matrix.allocated_target,
      target_matrix.channel_type,
      target_matrix.dim_channel_type_id,
      target_matrix.alliance_partner,
      target_matrix.dim_alliance_type_id,
      user_hierarchy.crm_opp_owner_sales_segment_geo_region_area_stamped,
      user_hierarchy.dim_crm_user_hierarchy_stamped_id,
      user_hierarchy.dim_crm_opp_owner_sales_segment_stamped_id,
      user_hierarchy.crm_opp_owner_sales_segment_stamped,
      user_hierarchy.dim_crm_opp_owner_geo_stamped_id,
      user_hierarchy.crm_opp_owner_geo_stamped,
      user_hierarchy.dim_crm_opp_owner_region_stamped_id,
      user_hierarchy.crm_opp_owner_region_stamped,
      user_hierarchy.dim_crm_opp_owner_area_stamped_id,
      user_hierarchy.crm_opp_owner_area_stamped
    FROM target_matrix
    LEFT JOIN user_hierarchy
      ON target_matrix.crm_opp_owner_segment_geo_region_area = user_hierarchy.crm_opp_owner_sales_segment_geo_region_area_stamped
        AND target_matrix.fiscal_year = user_hierarchy.fiscal_year

), final_targets AS (

     SELECT

     {{ dbt_utils.surrogate_key(['unioned_targets.crm_opp_owner_sales_segment_geo_region_area_stamped', 
                                 'unioned_targets.fiscal_year', 
                                 'unioned_targets.kpi_name', 
                                 'unioned_targets.first_day_of_month', 
                                 'unioned_targets.sales_qualified_source',
                                 'unioned_targets.order_type',
                                 'unioned_targets.dim_channel_type_id',
                                 'unioned_targets.dim_alliance_type_id'
                                 ]) }}                                                                                              AS sales_funnel_partner_alliance_target_id,
     unioned_targets.kpi_name,
     unioned_targets.first_day_of_month,
     unioned_targets.fiscal_year,
     unioned_targets.sales_qualified_source,
     unioned_targets.dim_sales_qualified_source_id,
     unioned_targets.alliance_partner,
     unioned_targets.dim_alliance_type_id,
     unioned_targets.order_type,
     unioned_targets.dim_order_type_id,
     unioned_targets.channel_type,
     unioned_targets.dim_channel_type_id,
     unioned_targets.crm_opp_owner_sales_segment_geo_region_area_stamped                                                            AS crm_user_sales_segment_geo_region_area,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id, unioned_targets.dim_crm_user_hierarchy_stamped_id)           AS dim_crm_user_hierarchy_live_id,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_sales_segment_id, unioned_targets.dim_crm_opp_owner_sales_segment_stamped_id)   AS dim_crm_user_sales_segment_id,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_geo_id, unioned_targets.dim_crm_opp_owner_geo_stamped_id)                       AS dim_crm_user_geo_id,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_region_id, unioned_targets.dim_crm_opp_owner_region_stamped_id)                 AS dim_crm_user_region_id,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_area_id, unioned_targets.dim_crm_opp_owner_area_stamped_id)                     AS dim_crm_user_area_id,
     unioned_targets.dim_crm_user_hierarchy_stamped_id,
     unioned_targets.dim_crm_opp_owner_sales_segment_stamped_id,
     unioned_targets.dim_crm_opp_owner_geo_stamped_id,
     unioned_targets.dim_crm_opp_owner_region_stamped_id,
     unioned_targets.dim_crm_opp_owner_area_stamped_id,
     SUM(unioned_targets.allocated_target)                                                                                          AS allocated_target

    FROM unioned_targets
    LEFT JOIN sfdc_user_hierarchy_live
      ON unioned_targets.dim_crm_user_hierarchy_stamped_id = sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id
    {{ dbt_utils.group_by(n=23) }}

)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-04-08",
    updated_date="2023-02-08"
) }}
