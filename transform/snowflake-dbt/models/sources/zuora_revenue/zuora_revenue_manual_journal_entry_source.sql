{{ config(
    tags=["mnpi"]
) }}

WITH zuora_revenue_manual_journal_entry AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_manual_journal_entry')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY je_line_id ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT 

      je_head_id::VARCHAR                           AS manual_journal_entry_header_id,
      je_head_name::VARCHAR                         AS manual_journal_entry_header_name,
      je_head_desc::VARCHAR                         AS manual_journal_entry_header_description,
      je_head_cat_code::VARCHAR                     AS manual_journal_entry_header_category_code,
      je_head_ex_rate_type::VARCHAR                 AS manual_journal_entry_header_exchange_rate_type,
      hash_total::VARCHAR                           AS hash_total,
      sob_id::VARCHAR                               AS set_of_books_id,
      sob_name::VARCHAR                             AS set_of_books_name,
      fn_cur::VARCHAR                               AS functional_currency,
      -- Data received from Zuora in YYYYMM format, formatted to YYYYMMDD in the below. 
      CONCAT(rvsl_prd_id::VARCHAR,'01')             AS reversal_period_id,
      CONCAT(prd_id::VARCHAR,'01')                  AS period_id,
      je_head_atr1::VARCHAR                         AS manual_journal_entry_header_attribute_1,
      je_head_atr2::VARCHAR                         AS manual_journal_entry_header_attribute_2,
      je_head_atr3::VARCHAR                         AS manual_journal_entry_header_attribute_3,
      je_head_atr4::VARCHAR                         AS manual_journal_entry_header_attribute_4,
      je_head_atr5::VARCHAR                         AS manual_journal_entry_header_attribute_5,
      CONCAT(je_head_crtd_prd_id::VARCHAR,'01')     AS manual_journal_entry_header_created_period_id,
      je_line_id::VARCHAR                           AS manual_journal_entry_line_id,
      activity_type::VARCHAR                        AS activity_type,
      curr::VARCHAR                                 AS currency,
      dr_cc_id::VARCHAR                             AS debit_account_code_combination_id,
      cr_cc_id::VARCHAR                             AS credit_account_code_combination_id,
      ex_rate_date::DATETIME                        AS exchange_rate_date,
      ex_rate::VARCHAR                              AS exchange_rate,
      g_ex_rate::VARCHAR                            AS reporting_currency_exchange_rate,
      amount::FLOAT                                 AS amount,
      func_amount::FLOAT                            AS funcional_currency_amount,
      start_date::DATETIME                          AS manual_journal_entry_line_start_date,
      end_date::DATETIME                            AS manual_journal_entry_line_end_date,
      reason_code::VARCHAR                          AS reason_code,
      description::VARCHAR                          AS manual_journal_entry_line_description,
      comments::VARCHAR                             AS manual_journal_entry_line_comments,
      dr_segment1::VARCHAR                          AS debit_segment_1,
      dr_segment2::VARCHAR                          AS debit_segment_2,
      dr_segment3::VARCHAR                          AS debit_segment_3,
      dr_segment4::VARCHAR                          AS debit_segment_4,
      dr_segment5::VARCHAR                          AS debit_segment_5,
      dr_segment6::VARCHAR                          AS debit_segment_6,
      dr_segment7::VARCHAR                          AS debit_segment_7,
      dr_segment8::VARCHAR                          AS debit_segment_8,
      dr_segment9::VARCHAR                          AS debit_segment_9,
      dr_segment10::VARCHAR                         AS debit_segment_10,
      cr_segment1::VARCHAR                          AS credit_segment_1,
      cr_segment2::VARCHAR                          AS credit_segment_2,
      cr_segment3::VARCHAR                          AS credit_segment_3,
      cr_segment4::VARCHAR                          AS credit_segment_4,
      cr_segment5::VARCHAR                          AS credit_segment_5,
      cr_segment6::VARCHAR                          AS credit_segment_6,
      cr_segment7::VARCHAR                          AS credit_segment_7,
      cr_segment8::VARCHAR                          AS credit_segment_8,
      cr_segment9::VARCHAR                          AS credit_segment_9,
      cr_segment10::VARCHAR                         AS credit_segment_10,
      reference1::VARCHAR                           AS manual_journal_entry_reference_1,
      reference2::VARCHAR                           AS manual_journal_entry_reference_2,
      reference3::VARCHAR                           AS manual_journal_entry_reference_3,
      reference4::VARCHAR                           AS manual_journal_entry_reference_4,
      reference5::VARCHAR                           AS manual_journal_entry_reference_5,
      reference6::VARCHAR                           AS manual_journal_entry_reference_6,
      reference7::VARCHAR                           AS manual_journal_entry_reference_7,
      reference8::VARCHAR                           AS manual_journal_entry_reference_8,
      reference9::VARCHAR                           AS manual_journal_entry_reference_9,
      reference10::VARCHAR                          AS manual_journal_entry_reference_10,
      reference11::VARCHAR                          AS manual_journal_entry_reference_11,
      reference12::VARCHAR                          AS manual_journal_entry_reference_12,
      reference13::VARCHAR                          AS manual_journal_entry_reference_13,
      reference14::VARCHAR                          AS manual_journal_entry_reference_14,
      reference15::VARCHAR                          AS manual_journal_entry_reference_15,
      sec_atr_val::VARCHAR                          AS security_attribute_value,
      book_id::VARCHAR                              AS book_id,
      client_id::VARCHAR                            AS client_id,
      je_head_crtd_by::VARCHAR                      AS manual_journal_entry_header_created_by,
      je_head_crtd_dt::DATETIME                     AS manual_journal_entry_header_created_date,
      je_head_updt_by::VARCHAR                      AS manual_journal_entry_header_updated_by,
      je_head_updt_dt::DATETIME                     AS manual_journal_entry_header_updated_date,
      je_line_crtd_by::VARCHAR                      AS manual_journal_entry_line_created_by,
      je_line_crtd_dt::DATETIME                     AS manual_journal_entry_line_created_date,
      je_line_updt_by::VARCHAR                      AS manual_journal_entry_line_updated_by,
      je_line_updt_dt::DATETIME                     AS manual_journal_entry_line_updated_date,
      incr_updt_dt::DATETIME                        AS incremental_update_date,
      je_status_flag::VARCHAR                       AS manual_journal_entry_header_status,
      rev_rec_type_flag::VARCHAR                    AS is_revenue_recognition_type,
      je_type_flag::VARCHAR                         AS manual_journal_entry_header_type,
      summary_flag::VARCHAR                         AS is_summary,
      manual_reversal_flag::VARCHAR                 AS is_manual_reversal,
      reversal_status_flag::VARCHAR                 AS reversal_status,
      approval_status_flag::VARCHAR                 AS approval_status,
      reversal_approval_status_flag::VARCHAR        AS reversal_approval_status,
      rev_rec_type::VARCHAR                         AS revenue_recognition_type,
      error_msg::VARCHAR                            AS error_message,
      dr_activity_type::VARCHAR                     AS debit_activity_type,
      cr_activity_type::VARCHAR                     AS credit_activity_type,
      active_flag::VARCHAR                          AS is_active,
      appr_name::VARCHAR                            AS approver_name,
      rc_id::VARCHAR                                AS revenue_contract_id,
      doc_line_id::VARCHAR                          AS doc_line_id,
      rc_line_id::VARCHAR                           AS revenue_contract_line_id,
      cst_or_vc_type::VARCHAR                       AS is_cost_or_vairable_consideration,
      type_name::VARCHAR                            AS manual_journal_entry_line_type,
      dt_frmt::VARCHAR                              AS date_format,
      opn_int_flag::VARCHAR                         AS is_open_interface,
      auto_appr_flag::VARCHAR                       AS is_auto_approved,
      unbilled_flag::VARCHAR                        AS is_unbilled

    FROM zuora_revenue_manual_journal_entry


)

SELECT *
FROM renamed