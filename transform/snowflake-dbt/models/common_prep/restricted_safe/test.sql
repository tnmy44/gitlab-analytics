UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_sales_segment, sfdc_opportunity_live.crm_opp_owner_sales_segment_stamped)
      ) AS report_segment,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_geo, sfdc_opportunity_live.crm_opp_owner_geo_stamped)
      ) AS report_geo,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_region, sfdc_opportunity_live.crm_opp_owner_region_stamped)
      ) AS report_region,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_area, sfdc_opportunity_live.crm_opp_owner_area_stamped)
      ) AS report_area,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_role, sfdc_opportunity_live.opportunity_owner_role)
      ) AS report_role_name,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_role_level_1, sfdc_opportunity_live.crm_opp_owner_role_level_1)
      ) AS report_role_level_1,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_role_level_2, sfdc_opportunity_live.crm_opp_owner_role_level_2)
      ) AS report_role_level_2,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_role_level_3, sfdc_opportunity_live.crm_opp_owner_role_level_3)
      ) AS report_role_level_3,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_role_level_4, sfdc_opportunity_live.crm_opp_owner_role_level_4)
      ) AS report_role_level_4,
      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.crm_account_owner_role_level_5, sfdc_opportunity_live.crm_opp_owner_role_level_5)
      ) AS report_role_level_5,
    CASE
        WHEN close_fiscal_year < 2024
                      ) 
      END AS dim_crm_opp_owner_stamped_hierarchy_sk, 

      UPPER(
        IFF(sfdc_opportunity.close_date < close_date.current_first_day_of_fiscal_year, sfdc_opportunity.dim_crm_user_hierarchy_account_user_sk, dim_crm_opp_owner_stamped_hierarchy_sk)
      ) AS dim_crm_current_account_set_hierarchy_sk,

      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.dim_crm_user_hierarchy_account_user_sk, 
        CONCAT(

                      UPPER(COALESCE(sfdc_opportunity_live.opportunity_owner_role, sfdc_opportunity_live.opportunity_account_owner_role)),
                      '-',
                      close_fiscal_year_live
                      ) )
      ) AS dim_crm_current_account_set_hierarchy_live_sk,

      DATEDIFF(MONTH, arr_created_fiscal_quarter_date, close_fiscal_quarter_date) AS arr_created_to_close_diff,