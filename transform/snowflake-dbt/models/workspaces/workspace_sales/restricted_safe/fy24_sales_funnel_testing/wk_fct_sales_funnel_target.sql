{{ simple_cte([
      ('wk_prep_crm_user_hierarchy', 'wk_prep_crm_user_hierarchy'),
      ('sales_qualified_source', 'prep_sales_qualified_source'),
      ('order_type', 'prep_order_type'),
      ('date_details_source', 'date_details_source'),
      ('wk_prep_sales_funnel_target', 'wk_prep_sales_funnel_target')
])}}

, final_targets AS (

    SELECT
      {{ dbt_utils.surrogate_key(['wk_prep_sales_funnel_target.dim_crm_user_hierarchy_sk', 
                                 'wk_prep_sales_funnel_target.fiscal_year', 
                                 'wk_prep_sales_funnel_target.kpi_name', 
                                 'wk_prep_sales_funnel_target.first_day_of_month', 
                                 'wk_prep_sales_funnel_target.opportunity_source',
                                 'wk_prep_sales_funnel_target.order_type',
                                 ]) }}                                                AS sales_funnel_target_id,
     wk_prep_sales_funnel_target.kpi_name,
     wk_prep_sales_funnel_target.first_day_of_month,
     wk_prep_sales_funnel_target.fiscal_year,
     wk_prep_sales_funnel_target.opportunity_source                                   AS sales_qualified_source,
     {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}    AS dim_sales_qualified_source_id,
     wk_prep_sales_funnel_target.order_type,
     {{ get_keyed_nulls('order_type.dim_order_type_id') }}                            AS dim_order_type_id,
     wk_prep_sales_funnel_target.area                                                 AS crm_user_sales_segment_geo_region_area,
     wk_prep_crm_user_hierarchy.dim_crm_user_hierarchy_stamped_id                     AS dim_crm_user_hierarchy_live_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_business_unit_stamped_id            AS dim_crm_user_business_unit_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_sales_segment_stamped_id            AS dim_crm_user_sales_segment_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_geo_stamped_id                      AS dim_crm_user_geo_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_region_stamped_id                   AS dim_crm_user_region_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_area_stamped_id                     AS dim_crm_user_area_id,
     wk_prep_crm_user_hierarchy.dim_crm_user_hierarchy_stamped_id,
     wk_prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk,
     wk_prep_sales_funnel_target.dim_crm_user_hierarchy_sk AS target_dim_crm_user_hierarchy_sk,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_business_unit_stamped_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_sales_segment_stamped_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_geo_stamped_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_region_stamped_id,
     wk_prep_crm_user_hierarchy.dim_crm_opp_owner_area_stamped_id,
     SUM(wk_prep_sales_funnel_target.allocated_target)                                AS allocated_target
    FROM wk_prep_sales_funnel_target
    LEFT JOIN sales_qualified_source
      ON {{ sales_funnel_text_slugify("wk_prep_sales_funnel_target.opportunity_source") }} = {{ sales_funnel_text_slugify("sales_qualified_source.sales_qualified_source_name") }}
    LEFT JOIN order_type
      ON {{ sales_funnel_text_slugify("wk_prep_sales_funnel_target.order_type") }} = {{ sales_funnel_text_slugify("order_type.order_type_name") }}
    LEFT JOIN wk_prep_crm_user_hierarchy
      ON wk_prep_sales_funnel_target.dim_crm_user_hierarchy_sk = wk_prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk
    {{ dbt_utils.group_by(n=22)}}

)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-01-23",
    updated_date="2023-01-23"
) }}
