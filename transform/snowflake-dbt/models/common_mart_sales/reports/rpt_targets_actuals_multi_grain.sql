{{ config(
    tags=["mnpi_exception"]
) }}


WITH unioned AS (

  {{ union_tables(
    [
        ref('mart_crm_opportunity_7th_day_weekly_snapshot'),
        ref('mart_crm_opportunity_7th_day_weekly_snapshot_aggregate'),
        ref('mart_targets_actuals_7th_day_weekly_snapshot')
    ],
    filters={
        'mart_crm_opportunity_7th_day_weekly_snapshot': 'is_current_snapshot_quarter = true',
        'mart_crm_opportunity_7th_day_weekly_snapshot_aggregate': 'is_current_snapshot_quarter = false'
    }
) }}

)

SELECT 
  unioned.*,
<<<<<<< HEAD
  NULL AS day_of_week,
  NULL AS first_day_of_fiscal_year,
  NULL AS first_day_of_week,
  NULL AS fiscal_month_name_fy,
  NULL AS fiscal_quarter_name_fy,
  NULL AS last_day_of_fiscal_quarter,
  NULL AS last_day_of_fiscal_year,
  NULL AS last_day_of_month,
  NULL AS last_day_of_week,
  NULL AS dim_order_type_current_id,
=======

  --fields to be removed once Tableau is updated
  NULL AS sao_crm_opp_owner_sales_segment_stamped,
  NULL AS sao_crm_opp_owner_sales_segment_stamped_grouped,
  NULL AS sao_crm_opp_owner_geo_stamped,
  NULL AS sao_crm_opp_owner_region_stamped,
  NULL AS sao_crm_opp_owner_area_stamped,
  NULL AS sao_crm_opp_owner_segment_region_stamped_grouped,
  NULL AS sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
  NULL AS crm_opp_owner_stamped_name,
  NULL AS crm_account_owner_stamped_name,
  NULL AS crm_opp_owner_sales_segment_stamped,
  NULL AS crm_opp_owner_sales_segment_stamped_grouped,
  NULL AS crm_opp_owner_geo_stamped,
  NULL AS crm_opp_owner_region_stamped,
  NULL AS crm_opp_owner_area_stamped,
  NULL AS crm_opp_owner_business_unit_stamped,
  NULL AS crm_opp_owner_sales_segment_region_stamped_grouped,
  NULL AS crm_opp_owner_sales_segment_geo_region_area_stamped,
  NULL AS crm_opp_owner_user_role_type_stamped,
  NULL AS opp_owner_name,
  NULL AS crm_user_sales_segment,
  NULL AS crm_user_sales_segment_grouped,
  NULL AS crm_user_geo,
  NULL AS crm_user_region,
  NULL AS crm_user_area,
  NULL AS crm_user_business_unit,
  NULL AS crm_user_sales_segment_region_grouped,
  NULL AS account_owner_name,
  NULL AS crm_account_user_sales_segment,
  NULL AS crm_account_user_sales_segment_grouped,
  NULL AS crm_account_user_geo,
  NULL AS crm_account_user_region,
  NULL AS crm_account_user_area,
  NULL AS crm_account_user_sales_segment_region_grouped,
  NULL AS crm_current_account_set_sales_segment,
  NULL AS crm_current_account_set_geo,
  NULL AS crm_current_account_set_region,
  NULL AS crm_current_account_set_area,
  NULL AS crm_current_account_set_business_unit,
  NULL AS crm_current_account_set_role_name,
  NULL AS crm_current_account_set_role_level_1,
  NULL AS crm_current_account_set_role_level_2,
  NULL AS crm_current_account_set_role_level_3,
  NULL AS crm_current_account_set_role_level_4,
  NULL AS crm_current_account_set_role_level_5,
  NULL AS opportunity_owner_user_segment, 
  NULL AS account_owner_user_segment, 
  NULL AS account_owner_user_geo, 
  NULL AS account_owner_user_region, 
  NULL AS account_owner_user_area, 
>>>>>>> 35575be0c6 (Update aggregate lineage)
  MAX(CASE 
        WHEN source != 'targets_actuals' AND snapshot_date < CURRENT_DATE() THEN snapshot_date 
        ELSE NULL 
    END) OVER ()                                                        AS max_snapshot_date, -- We want to ensure we have the max_snapshot_date that comes from the actuals in every row but excluding the future dates we have in the targets data 
  FLOOR((DATEDIFF(day, current_first_day_of_fiscal_quarter, max_snapshot_date) / 7)) 
                                                                        AS most_recent_snapshot_week
FROM unioned 
WHERE snapshot_fiscal_quarter_date >= DATEADD(QUARTER, -9, CURRENT_DATE())

