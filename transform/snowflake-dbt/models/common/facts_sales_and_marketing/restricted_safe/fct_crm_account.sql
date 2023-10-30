{{ simple_cte([
    ('prep_crm_account', 'prep_crm_account'),
    ('prep_crm_user_hierarchy', 'prep_crm_user_hierarchy')
    ])

}}, final AS (

    SELECT 
      --primary key
      prep_crm_account.dim_crm_account_id,

      --surrogate keys
      prep_crm_account.dim_parent_crm_account_id,
      prep_crm_account.dim_crm_user_id,
      prep_crm_account.merged_to_account_id,
      prep_crm_account.record_type_id,
      prep_crm_account.crm_account_owner_id,
      prep_crm_account.proposed_crm_account_owner_id,
      prep_crm_account.technical_account_manager_id,
      prep_crm_account.master_record_id,
      prep_crm_account.dim_crm_person_primary_contact_id,
      prep_crm_account.dim_crm_parent_account_hierarchy_sk,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_business_unit_id') }} AS dim_crm_parent_account_business_unit_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_sales_segment_id') }} AS dim_crm_parent_account_sales_segment_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_geo_id') }}           AS dim_crm_parent_account_geo_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_region_id') }}        AS dim_crm_parent_account_region_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_area_id') }}          AS dim_crm_parent_account_area_id,

      --dates
      prep_crm_account.crm_account_created_date_id,
      prep_crm_account.abm_tier_1_date_id,
      prep_crm_account.abm_tier_2_date_id,
      prep_crm_account.abm_tier_3_date_id,
      prep_crm_account.gtm_acceleration_date_id,
      prep_crm_account.gtm_account_based_date_id,
      prep_crm_account.gtm_account_centric_date_id,
      prep_crm_account.partners_signed_contract_date_id,
      prep_crm_account.technical_account_manager_date_id,
      prep_crm_account.next_renewal_date_id,
      prep_crm_account.customer_since_date_id,
      prep_crm_account.gs_first_value_date_id,
      prep_crm_account.gs_last_csm_activity_date_id,

      --measures
      prep_crm_account.count_active_subscription_charges,
      prep_crm_account.count_active_subscriptions,
      prep_crm_account.count_billing_accounts,
      prep_crm_account.count_licensed_users,
      prep_crm_account.count_of_new_business_won_opportunities,
      prep_crm_account.count_open_renewal_opportunities,
      prep_crm_account.count_opportunities,
      prep_crm_account.count_products_purchased,
      prep_crm_account.count_won_opportunities,
      prep_crm_account.count_concurrent_ee_subscriptions,
      prep_crm_account.count_ce_instances,
      prep_crm_account.count_active_ce_users,
      prep_crm_account.count_open_opportunities,
      prep_crm_account.count_using_ce,
      prep_crm_account.parent_crm_account_lam,
      prep_crm_account.parent_crm_account_lam_dev_count,
      prep_crm_account.carr_this_account,
      prep_crm_account.carr_account_family,
      prep_crm_account.potential_users,
      prep_crm_account.number_of_licenses_this_account,
      prep_crm_account.crm_account_zoom_info_number_of_developers,
      prep_crm_account.crm_account_zoom_info_total_funding,
      prep_crm_account.decision_maker_count_linkedin,
      prep_crm_account.number_of_employees,

      --metadata
      prep_crm_account.created_by_id,
      prep_crm_account.last_modified_by_id,
      prep_crm_account.last_modified_date_id,
      prep_crm_account.last_activity_date_id,
      prep_crm_account.is_deleted
    FROM prep_crm_account
    LEFT JOIN prep_crm_user_hierarchy
      ON prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk = prep_crm_account.dim_crm_parent_account_hierarchy_sk

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@jpeguero",
    created_date="2022-08-10",
    updated_date="2023-10-19"
) }}