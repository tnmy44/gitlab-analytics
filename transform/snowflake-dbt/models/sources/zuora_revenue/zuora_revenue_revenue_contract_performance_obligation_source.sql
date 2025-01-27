{{ config(
    tags=["mnpi"]
) }}

WITH zuora_revenue_revenue_contract_performance_obligation AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_revenue_contract_performance_obligation')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rc_pob_id ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT 
    
      rc_pob_id::VARCHAR                            AS revenue_contract_performance_obligation_id,
      rc_id::VARCHAR                                AS revenue_contract_id,
      lead_line_id::VARCHAR                         AS lead_line_id,
      pob_version::VARCHAR                          AS performance_obligation_version,
      rc_pob_name::VARCHAR                          AS revenue_contract_performance_obligation_name,
      pob_dependency_flag::VARCHAR                  AS is_performance_obligation_dependency,
      pob_processed_flag::VARCHAR                   AS is_performance_obligation_processed,
      pob_removed_flag::VARCHAR                     AS is_performance_obligation_removed,
      pob_multi_sign_flag::VARCHAR                  AS is_performance_obligation_multiple_sign,
      pob_removal_flag::VARCHAR                     AS is_performance_obligation_removal,
      pob_manual_flag::VARCHAR                      AS is_performance_obligation_manual,
      pob_orphan_flag::VARCHAR                      AS is_performance_obligation_orphan,
      pob_manual_forecast_flag::VARCHAR             AS is_performance_obligation_manual_forecast,
      pob_tmpl_id::VARCHAR                          AS performance_obligation_template_id,
      pob_tmpl_name::VARCHAR                        AS performance_obligation_template_name,
      pob_tmpl_desc::VARCHAR                        AS performance_obligation_template_description,
      pob_tmpl_version::VARCHAR                     AS performance_obligation_template_version,
      rev_rec_type::VARCHAR                         AS revenue_recognition_type,
      start_date::DATETIME                          AS revenue_contract_performance_obligation_start_date,
      end_date::DATETIME                            AS revenue_contract_performance_obligation_end_date,
      rev_timing::VARCHAR                           AS revenue_timing,
      rev_start_dt::DATETIME                        AS revenue_start_date,
      rev_end_dt::DATETIME                          AS revenue_end_date,
      duration::VARCHAR                             AS revenue_amortization_duration,
      rev_segments::VARCHAR                         AS revenue_accounting_segment,
      cv_in_segments::VARCHAR                       AS carve_in_accounting_segment,
      cv_out_segments::VARCHAR                      AS carve_out_accounting_segment,
      cl_segments::VARCHAR                          AS contract_liability_accounting_segment,
      ca_segments::VARCHAR                          AS contract_asset_accounting_segment,
      qty_distinct_flag::VARCHAR                    AS is_quantity_distinct,
      term_distinct_flag::VARCHAR                   AS is_term_distinct,
      apply_manually_flag::VARCHAR                  AS is_apply_manually,
      release_manually_flag::VARCHAR                AS is_release_manually,
      rev_leading_flag::VARCHAR                     AS is_revenue_leading,
      cv_in_leading_flag::VARCHAR                   AS is_carve_in_leading,
      cv_out_leading_flag::VARCHAR                  AS is_carve_out_leading,
      cl_leading_flag::VARCHAR                      AS is_contract_liability_leading,
      ca_leading_flag::VARCHAR                      AS is_contract_asset_leading,
      rel_action_type_flag::VARCHAR                 AS release_action_type,
      pob_tmpl_dependency_flag::VARCHAR             AS is_performance_obligation_template_dependency,
      latest_version_flag::VARCHAR                  AS is_latest_version,
      consumption_flag::VARCHAR                     AS is_consumption,
      pob_satisfied_flag::VARCHAR                   AS is_performance_obligation_satisfied,
      event_id::VARCHAR                             AS event_id,
      event_name::VARCHAR                           AS event_name,
      postable_flag::VARCHAR                        AS is_postable,
      event_type_flag::VARCHAR                      AS event_type,
      book_id::VARCHAR                              AS book_id,
      sec_atr_val::VARCHAR                          AS security_attribute_value,
      client_id::VARCHAR                            AS client_id,
      CONCAT(crtd_prd_id::VARCHAR, '01')            AS revenue_contract_performance_obligation_created_period_id,
      rc_pob_crtd_by::VARCHAR                       AS revenue_contract_performance_obligation_created_by,
      rc_pob_crtd_dt::DATETIME                      AS revenue_contract_performance_obligation_created_date,
      rc_pob_updt_by::VARCHAR                       AS revenue_contract_performance_obligation_updated_by,
      rc_pob_updt_dt::DATETIME                      AS revenue_contract_performance_obligation_updated_date,
      pob_tmpl_crtd_by::VARCHAR                     AS performance_obligation_template_created_by,
      pob_tmpl_crtd_dt::DATETIME                    AS performance_obligation_template_created_date,
      pob_tmpl_updt_by::VARCHAR                     AS performance_obligation_template_updated_by,
      pob_tmpl_updt_dt::DATETIME                    AS performance_obligation_template_updated_date,
      event_crtd_by::VARCHAR                        AS event_created_by,
      event_crtd_dt::DATETIME                       AS event_created_date,
      event_updt_by::VARCHAR                        AS event_updated_by,
      event_updt_dt::DATETIME                       AS event_updated_date,
      incr_updt_dt::VARCHAR                         AS incremental_update_date,
      pob_id::VARCHAR                               AS performance_obligation_id,
      natural_acct::VARCHAR                         AS natural_account,
      distinct_flag::VARCHAR                        AS is_distinct,
      tolerance_pct::FLOAT                          AS tolerance_percent,
      evt_type_id::VARCHAR                          AS event_type_id,
      cmltv_prcs_flag::VARCHAR                      AS is_cumulative_prcs,
      exp_date::DATETIME                            AS expiry_date,
      manual_edit_flag::VARCHAR                     AS is_manual_edit,
      rule_identifier::VARCHAR                      AS rule_identifier,
      exp_fld_name::VARCHAR                         AS expiry_field_name,
      exp_num::VARCHAR                              AS expiry_number,
      exp_num_type::VARCHAR                         AS expiry_number_type,
      rel_immediate_flag::VARCHAR                   AS is_release_immediate,
      so_term_change_flag::VARCHAR                  AS is_sales_order_term_change,
      event_column1::VARCHAR                        AS event_column_1,
      event_column2::VARCHAR                        AS event_column_2,
      event_column3::VARCHAR                        AS event_column_3,
      event_column4::VARCHAR                        AS event_column_4,
      event_column5::VARCHAR                        AS event_column_5,
      source_column1::VARCHAR                       AS source_colum_n1,
      source_column2::VARCHAR                       AS source_column_2,
      source_column3::VARCHAR                       AS source_column_3,
      source_column4::VARCHAR                       AS source_column_4,
      source_column5::VARCHAR                       AS source_column_5,
      order_column1::VARCHAR                        AS order_column_1,
      order_column2::VARCHAR                        AS order_column_2,
      order_column3::VARCHAR                        AS order_column_3,
      order_column4::VARCHAR                        AS order_column_4,
      order_column5::VARCHAR                        AS order_column_5,
      process_type::VARCHAR                         AS process_type,
      rel_base_date::DATETIME                       AS release_base_date,
      retain_method::VARCHAR                        AS retain_method,
      manual_rearranged_flag::VARCHAR               AS is_manual_rearranged,
      manual_release_flag::VARCHAR                  AS is_manual_release,
      fcst_tmpl_id::VARCHAR                         AS forecast_template_id

    FROM zuora_revenue_revenue_contract_performance_obligation

)

SELECT *
FROM renamed