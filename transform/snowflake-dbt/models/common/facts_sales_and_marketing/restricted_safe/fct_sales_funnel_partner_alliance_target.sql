{{ simple_cte([
      ('prep_user_hierarchy', 'prep_crm_user_hierarchy'),
      ('sales_qualified_source', 'prep_sales_qualified_source'),
      ('order_type', 'prep_order_type'),
      ('alliance_type', 'prep_alliance_type_scd'),
      ('channel_type', 'prep_channel_type'),
      ('date_details_source', 'date_details_source'),
      ('partner_category', 'prep_partner_category')
])}}

, date AS (

   SELECT DISTINCT
     fiscal_month_name_fy,
     fiscal_year,
     first_day_of_month
   FROM date_details_source

), prep_sales_funnel_partner_alliance_target AS (

    SELECT 
      prep_sales_funnel_partner_alliance_target.*,
      {{ channel_type('prep_sales_funnel_partner_alliance_target.sqs_bucket_engagement', 'prep_sales_funnel_partner_alliance_target.order_type') }}   AS channel_type
    FROM {{ ref('prep_sales_funnel_partner_alliance_target') }}

), final_targets AS (

    SELECT
      {{ dbt_utils.surrogate_key(['prep_sales_funnel_partner_alliance_target.area', 
                                 'date.fiscal_year', 
                                 'prep_sales_funnel_partner_alliance_target.kpi_name', 
                                 'prep_sales_funnel_partner_alliance_target.first_day_of_month', 
                                 'prep_sales_funnel_partner_alliance_target.sales_qualified_source',
                                 'prep_sales_funnel_partner_alliance_target.order_type',
                                 'channel_type.dim_channel_type_id',
                                 'alliance_type.dim_alliance_type_id',
                                 'partner_category.dim_partner_category_id'
                                 ]) }}                                                                    AS sales_funnel_partner_alliance_target_id,
      prep_sales_funnel_partner_alliance_target.kpi_name,
      date.first_day_of_month,
      date.fiscal_year,
      prep_sales_funnel_partner_alliance_target.sales_qualified_source,
      {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}                       AS dim_sales_qualified_source_id,
      prep_sales_funnel_partner_alliance_target.alliance_partner,
      {{ get_keyed_nulls('alliance_type.dim_alliance_type_id') }}                                         AS dim_alliance_type_id,
      prep_sales_funnel_partner_alliance_target.partner_category,
      {{ get_keyed_nulls('partner_category.dim_partner_category_id') }}                                   AS dim_partner_category_id,
      prep_sales_funnel_partner_alliance_target.order_type,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                               AS dim_order_type_id,
      prep_sales_funnel_partner_alliance_target.channel_type,
      {{ get_keyed_nulls('channel_type.dim_channel_type_id') }}                                           AS dim_channel_type_id,
      prep_user_hierarchy.dim_crm_user_hierarchy_sk                                                       AS dim_crm_user_hierarchy_sk,
      prep_sales_funnel_partner_alliance_target.area                                                      AS crm_user_sales_segment_geo_region_area,
      prep_user_hierarchy.dim_crm_user_hierarchy_id,
      prep_user_hierarchy.dim_crm_user_sales_segment_id,
      prep_user_hierarchy.dim_crm_user_geo_id,
      prep_user_hierarchy.dim_crm_user_region_id,
      prep_user_hierarchy.dim_crm_user_area_id,
      prep_user_hierarchy.dim_crm_user_hierarchy_id                                                       AS dim_crm_user_hierarchy_stamped_id,
      prep_user_hierarchy.dim_crm_user_sales_segment_id                                                   AS dim_crm_opp_owner_sales_segment_stamped_id,
      prep_user_hierarchy.dim_crm_user_geo_id                                                             AS dim_crm_opp_owner_geo_stamped_id,
      prep_user_hierarchy.dim_crm_user_region_id                                                          AS dim_crm_opp_owner_region_stamped_id,
      prep_user_hierarchy.dim_crm_user_area_id                                                            AS dim_crm_opp_owner_area_stamped_id,
      SUM(prep_sales_funnel_partner_alliance_target.allocated_target)                                     AS allocated_target
    FROM prep_sales_funnel_partner_alliance_target
    LEFT JOIN date
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_partner_alliance_target.month") }} = {{ sales_funnel_text_slugify("date.fiscal_month_name_fy") }}
    LEFT JOIN sales_qualified_source
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_partner_alliance_target.sales_qualified_source") }} = {{ sales_funnel_text_slugify("sales_qualified_source.sales_qualified_source_name") }}
    LEFT JOIN order_type
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_partner_alliance_target.order_type") }} = {{ sales_funnel_text_slugify("order_type.order_type_name") }}
    LEFT JOIN alliance_type
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_partner_alliance_target.alliance_partner") }} = {{ sales_funnel_text_slugify("alliance_type.alliance_type_name") }}
    LEFT JOIN channel_type
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_partner_alliance_target.channel_type") }} = {{ sales_funnel_text_slugify("channel_type.channel_type_name") }}
    LEFT JOIN partner_category
      ON {{ sales_funnel_text_slugify("prep_sales_funnel_partner_alliance_target.partner_category") }} = {{ sales_funnel_text_slugify("partner_category.partner_category_name") }}
    LEFT JOIN prep_user_hierarchy
      ON prep_sales_funnel_partner_alliance_target.dim_crm_user_hierarchy_sk = prep_user_hierarchy.dim_crm_user_hierarchy_sk
        AND prep_sales_funnel_partner_alliance_target.fiscal_year = prep_user_hierarchy.fiscal_year
    {{ dbt_utils.group_by(n=26) }}


)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-04-08",
    updated_date="2023-03-10"
) }}
