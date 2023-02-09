{{ simple_cte([
      ('sfdc_user_hierarchy_live', 'prep_crm_user_hierarchy_live'),
      ('sfdc_user_hierarchy_stamped', 'prep_crm_user_hierarchy_stamped'),
      ('sales_qualified_source', 'prep_sales_qualified_source'),
      ('order_type', 'prep_order_type'),
      ('date_details_source', 'date_details_source')
])}}

, date AS (

   SELECT DISTINCT
     fiscal_month_name_fy,
     fiscal_year,
     first_day_of_month
   FROM date_details_source

), target_matrix AS (

    SELECT
      sheetload_sales_funnel_targets_matrix_source.*,
      date.first_day_of_month,
      date.fiscal_year,
      {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}   AS dim_sales_qualified_source_id,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                           AS dim_order_type_id,
      CONCAT(sheetload_sales_funnel_targets_matrix_source.user_segment, 
             '-',
             sheetload_sales_funnel_targets_matrix_source.user_geo, 
             '-', 
             sheetload_sales_funnel_targets_matrix_source.user_region, 
             '-', 
             sheetload_sales_funnel_targets_matrix_source.user_area)                  AS crm_opp_owner_segment_geo_region_area
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source' )}}
    LEFT JOIN date
      ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_targets_matrix_source.month") }} = {{ sales_funnel_text_slugify("date.fiscal_month_name_fy") }}
    LEFT JOIN sales_qualified_source
      ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_targets_matrix_source.opportunity_source") }} = {{ sales_funnel_text_slugify("sales_qualified_source.sales_qualified_source_name") }}
    LEFT JOIN order_type
      ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_targets_matrix_source.order_type") }} = {{ sales_funnel_text_slugify("order_type.order_type_name") }}

), unioned_targets AS (

    SELECT
      target_matrix.kpi_name,
      target_matrix.first_day_of_month,
      target_matrix.dim_sales_qualified_source_id,
      target_matrix.opportunity_source,
      target_matrix.dim_order_type_id,
      target_matrix.order_type, 
      target_matrix.fiscal_year,
      target_matrix.allocated_target,
      sfdc_user_hierarchy_stamped.crm_opp_owner_sales_segment_geo_region_area_stamped,
      sfdc_user_hierarchy_stamped.dim_crm_user_hierarchy_stamped_id,
      sfdc_user_hierarchy_stamped.dim_crm_opp_owner_sales_segment_stamped_id,
      sfdc_user_hierarchy_stamped.crm_opp_owner_sales_segment_stamped,
      sfdc_user_hierarchy_stamped.dim_crm_opp_owner_geo_stamped_id,
      sfdc_user_hierarchy_stamped.crm_opp_owner_geo_stamped,
      sfdc_user_hierarchy_stamped.dim_crm_opp_owner_region_stamped_id,
      sfdc_user_hierarchy_stamped.crm_opp_owner_region_stamped,
      sfdc_user_hierarchy_stamped.dim_crm_opp_owner_area_stamped_id,
      sfdc_user_hierarchy_stamped.crm_opp_owner_area_stamped
    FROM target_matrix
    LEFT JOIN sfdc_user_hierarchy_stamped
      ON target_matrix.crm_opp_owner_segment_geo_region_area = sfdc_user_hierarchy_stamped.crm_opp_owner_sales_segment_geo_region_area_stamped
        AND target_matrix.fiscal_year = sfdc_user_hierarchy_stamped.fiscal_year

), final_targets AS (

     SELECT

     {{ dbt_utils.surrogate_key(['unioned_targets.crm_opp_owner_sales_segment_geo_region_area_stamped', 
                                 'unioned_targets.fiscal_year', 
                                 'unioned_targets.kpi_name', 
                                 'unioned_targets.first_day_of_month', 
                                 'unioned_targets.opportunity_source',
                                 'unioned_targets.order_type',
                                 ]) }}                           AS sales_funnel_target_id,
     unioned_targets.kpi_name,
     unioned_targets.first_day_of_month,
     unioned_targets.fiscal_year,
     unioned_targets.opportunity_source                                                                                               AS sales_qualified_source,
     unioned_targets.dim_sales_qualified_source_id,
     unioned_targets.order_type,
     unioned_targets.dim_order_type_id,
     unioned_targets.crm_opp_owner_sales_segment_geo_region_area_stamped                                                              AS crm_user_sales_segment_geo_region_area,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id, unioned_targets.dim_crm_user_hierarchy_stamped_id)             AS dim_crm_user_hierarchy_live_id,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_sales_segment_id, unioned_targets.dim_crm_opp_owner_sales_segment_stamped_id)     AS dim_crm_user_sales_segment_id,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_geo_id, unioned_targets.dim_crm_opp_owner_geo_stamped_id)                         AS dim_crm_user_geo_id,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_region_id, unioned_targets.dim_crm_opp_owner_region_stamped_id)                   AS dim_crm_user_region_id,
     COALESCE(sfdc_user_hierarchy_live.dim_crm_user_area_id, unioned_targets.dim_crm_opp_owner_area_stamped_id)                       AS dim_crm_user_area_id,
     unioned_targets.dim_crm_user_hierarchy_stamped_id,
     unioned_targets.dim_crm_opp_owner_sales_segment_stamped_id,
     unioned_targets.dim_crm_opp_owner_geo_stamped_id,
     unioned_targets.dim_crm_opp_owner_region_stamped_id,
     unioned_targets.dim_crm_opp_owner_area_stamped_id,
     SUM(unioned_targets.allocated_target)                                                                                            AS allocated_target

    FROM unioned_targets
    LEFT JOIN sfdc_user_hierarchy_live
      ON unioned_targets.dim_crm_user_hierarchy_stamped_id = sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id
    {{ dbt_utils.group_by(n=19) }}

)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@mcooperDD",
    updated_by="@michellecooper",
    created_date="2020-12-18",
    updated_date="2023-02-08"
) }}
