WITH biz_person AS (

    SELECT *
    FROM {{ref('sfdc_bizible_person_source')}}
    WHERE is_deleted = 'FALSE' 

), biz_touchpoints AS (

    SELECT *
    FROM {{ref('sfdc_bizible_touchpoint_source')}}
    WHERE bizible_touchpoint_position LIKE '%FT%'
     AND is_deleted = 'FALSE'

), prep_date AS (

    SELECT *
    FROM {{ ref('prep_date') }}

), prep_location_country AS (

    SELECT *
    FROM {{ ref('prep_location_country') }}

), crm_tasks AS (

    SELECT 
      sfdc_record_id,
      MIN(task_completed_date) AS min_task_completed_date_by_bdr_sdr
    FROM {{ref('prep_crm_task')}}
    WHERE is_deleted = 'FALSE'
    AND task_owner_role LIKE '%BDR%' 
    OR task_owner_role LIKE '%SDR%'
    GROUP BY 1

), crm_events AS (

    SELECT 
      sfdc_record_id,
      MIN(event_date) AS min_task_completed_date_by_bdr_sdr
    FROM {{ref('prep_crm_event')}}
    LEFT JOIN {{ref('dim_crm_user')}} event_user_id 
    ON prep_crm_event.dim_crm_user_id = event_user_id.dim_crm_user_id 
    LEFT JOIN {{ref('dim_crm_user')}}  event_booked_by_id
    ON prep_crm_event.booked_by_employee_number = event_booked_by_id.employee_number
    WHERE 
    event_user_id.user_role_name LIKE '%BDR%' 
    OR event_booked_by_id.user_role_name LIKE '%BDR%' 
    OR event_user_id.user_role_name LIKE '%SDR%' 
    OR event_booked_by_id.user_role_name LIKE '%SDR%' 
    GROUP BY 1

  
), crm_activity_prep AS (
  
    SELECT 
      sfdc_record_id,
      min_task_completed_date_by_bdr_sdr
    FROM crm_tasks
    UNION
    SELECT
      sfdc_record_id,
      min_task_completed_date_by_bdr_sdr
    FROM crm_events
  
),  crm_activity AS (

    SELECT 
      sfdc_record_id,
      MIN(min_task_completed_date_by_bdr_sdr) AS min_task_completed_date_by_bdr_sdr
    FROM crm_activity_prep
    GROUP BY 1 

), biz_person_with_touchpoints AS (

    SELECT

      biz_touchpoints.*,
      biz_person.bizible_contact_id,
      biz_person.bizible_lead_id

    FROM biz_touchpoints
    JOIN biz_person
      ON biz_touchpoints.bizible_person_id = biz_person.person_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY bizible_lead_id,bizible_contact_id ORDER BY bizible_touchpoint_date DESC) = 1

), sfdc_contacts AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_contact_source') }}
    FROM {{ref('sfdc_contact_source')}}
    WHERE is_deleted = 'FALSE'

), sfdc_leads AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_lead_source') }}
    FROM {{ref('sfdc_lead_source')}}
    WHERE is_deleted = 'FALSE'

),  was_converted_lead AS (

    SELECT DISTINCT
      contact_id,
      1 AS was_converted_lead
    FROM {{ ref('sfdc_contact_source') }}
    INNER JOIN {{ ref('sfdc_lead_source') }}
      ON sfdc_contact_source.contact_id = sfdc_lead_source.converted_contact_id

),  marketo_persons AS (

    SELECT
      marketo_lead_id,
      sfdc_type,
      sfdc_lead_id,
      sfdc_contact_id
    FROM {{ ref('marketo_lead_source') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY sfdc_lead_id,sfdc_contact_id  ORDER BY updated_at DESC) = 1

),  crm_person_final AS (

    SELECT
      --id
      {{ dbt_utils.generate_surrogate_key(['sfdc_contacts.contact_id']) }} AS dim_crm_person_id,
      sfdc_contacts.contact_id                      AS sfdc_record_id,
      bizible_person_id                             AS bizible_person_id,
      'contact'                                     AS sfdc_record_type,
      contact_email_hash                            AS email_hash,
      email_domain,
      email_domain_type,
      marketo_lead_id,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      account_id                                    AS dim_crm_account_id,
      reports_to_id,
      owner_id                                      AS dim_crm_user_id,

      --info
      person_score,
      behavior_score,
      contact_title                                 AS title,
      it_job_title_hierarchy,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      contact_status                                AS status,
      lead_source,
      lead_source_type,
      was_converted_lead.was_converted_lead         AS was_converted_lead,
      source_buckets,
      net_new_source_categories,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      marketo_last_interesting_moment,
      marketo_last_interesting_moment_date,
      outreach_step_number,
      NULL                                          AS matched_account_owner_role,
      NULL                                          AS matched_account_account_owner_name,
      NULL                                          AS matched_account_sdr_assigned,
      NULL                                          AS matched_account_type,
      NULL                                          AS matched_account_gtm_strategy,
      NULL                                          AS matched_account_bdr_prospecting_status,
      is_first_order_initial_mql,
      is_first_order_mql,
      is_first_order_person,
      last_utm_content,
      last_utm_campaign,
      sequence_step_type,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      is_actively_being_sequenced,
      high_priority_datetime,
      CASE
        WHEN high_priority_datetime IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS is_high_priority,
      prospect_share_status,
      partner_prospect_status,
      partner_prospect_id,
      partner_prospect_owner_name,
      mailing_country                               AS country,
      mailing_state                                 AS state,
      last_activity_date,
      NULL                                          AS employee_bucket,
      CASE
        WHEN account_demographics_sales_segment IS NULL OR UPPER(account_demographics_sales_segment) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_sales_segment
      END AS account_demographics_sales_segment,
      account_demographics_sales_segment_grouped,
      CASE
        WHEN account_demographics_geo IS NULL OR UPPER(account_demographics_geo) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_geo
      END AS account_demographics_geo,
      CASE
        WHEN account_demographics_region IS NULL OR UPPER(account_demographics_region) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_region
      END AS account_demographics_region,
      CASE
        WHEN account_demographics_area IS NULL OR UPPER(account_demographics_area) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_area
      END AS account_demographics_area,
      account_demographics_segment_region_grouped,
      account_demographics_territory,
      account_demographics_employee_count,
      account_demographics_max_family_employee,
      account_demographics_upa_country,
      account_demographics_upa_state,
      account_demographics_upa_city,
      account_demographics_upa_street,
      account_demographics_upa_postal_code,
      NULL                                          AS crm_partner_id,
      NULL                                          AS ga_client_id,
      NULL                                          AS cognism_company_office_city,
      NULL                                          AS cognism_company_office_state,
      NULL                                          AS cognism_company_office_country,
      NULL                                          AS cognism_city,
      NULL                                          AS cognism_state,
      NULL                                          AS cognism_country,
      cognism_employee_count,
      NULL                                          AS leandata_matched_account_billing_state,
      NULL                                          AS leandata_matched_account_billing_postal_code,
      NULL                                          AS leandata_matched_account_billing_country,
      NULL                                          AS leandata_matched_account_employee_count,
      NULL                                          AS leandata_matched_account_sales_segment,
      zoominfo_contact_city,
      zoominfo_contact_state,
      zoominfo_contact_country,
      zoominfo_company_city,
      zoominfo_company_state,
      zoominfo_company_country,
      zoominfo_phone_number, 
      zoominfo_mobile_phone_number,
      zoominfo_do_not_call_direct_phone,
      zoominfo_do_not_call_mobile_phone,
      traction_first_response_time,
      traction_first_response_time_seconds,
      traction_response_time_in_business_hours,
      last_transfer_date_time,
      time_from_last_transfer_to_sequence,
      time_from_mql_to_last_transfer,
      ptp_score_date                                 AS propensity_to_purchase_score_date,
      ptp_score_group                                AS propensity_to_purchase_score_group,
      pql_namespace_creator_job_description,
      pql_namespace_id,
      pql_namespace_name,
      pql_namespace_users,
      is_product_qualified_lead,
      ptp_days_since_trial_start                     AS propensity_to_purchase_days_since_trial_start,
      ptp_insights                                   AS propensity_to_purchase_insights,
      is_ptp_contact,
      ptp_namespace_id                               AS propensity_to_purchase_namespace_id,
      ptp_past_insights                              AS propensity_to_purchase_past_insights,
      ptp_past_score_group                           AS propensity_to_purchase_past_score_group,
      lead_score_classification,
      NULL                                           AS zoominfo_company_employee_count,
      zoominfo_contact_id,
      NULL                                           AS is_partner_recalled,
      CASE
        WHEN crm_activity.min_task_completed_date_by_bdr_sdr IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS is_bdr_sdr_worked,
      created_date


    FROM sfdc_contacts
    LEFT JOIN biz_person_with_touchpoints
      ON sfdc_contacts.contact_id = biz_person_with_touchpoints.bizible_contact_id
    LEFT JOIN was_converted_lead
      ON was_converted_lead.contact_id = sfdc_contacts.contact_id
    LEFT JOIN marketo_persons
      ON sfdc_contacts.contact_id = marketo_persons.sfdc_contact_id and sfdc_type = 'Contact'
    LEFT JOIN crm_activity
      ON sfdc_contacts.contact_id=crm_activity.sfdc_record_id

    UNION

    SELECT
      --id
      {{ dbt_utils.generate_surrogate_key(['lead_id']) }} AS dim_crm_person_id,
      lead_id                                    AS sfdc_record_id,
      bizible_person_id                          AS bizible_person_id,
      'lead'                                     AS sfdc_record_type,
      lead_email_hash                            AS email_hash,
      email_domain,
      email_domain_type,
      marketo_lead_id,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      lean_data_matched_account                  AS dim_crm_account_id,
      NULL                                       AS reports_to_id,
      owner_id                                   AS dim_crm_user_id,

      --info
      person_score,
      behavior_score,
      title,
      it_job_title_hierarchy,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      lead_status                                AS status,
      lead_source,
      lead_source_type,
      0                                          AS was_converted_lead,
      source_buckets,
      net_new_source_categories,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      marketo_last_interesting_moment,
      marketo_last_interesting_moment_date,
      outreach_step_number,
      matched_account_owner_role,
      matched_account_account_owner_name,
      matched_account_sdr_assigned,
      matched_account_type,
      matched_account_gtm_strategy,
      matched_account_bdr_prospecting_status,
      is_first_order_initial_mql,
      is_first_order_mql,
      is_first_order_person,
      last_utm_content,
      last_utm_campaign,
      sequence_step_type,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      is_actively_being_sequenced,
      high_priority_datetime,
      CASE
        WHEN high_priority_datetime IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS is_high_priority,
      prospect_share_status,
      partner_prospect_status,
      partner_prospect_id,
      partner_prospect_owner_name,
      country,
      state,
      last_activity_date,
      employee_bucket,
      CASE
        WHEN account_demographics_sales_segment IS NULL OR UPPER(account_demographics_sales_segment) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_sales_segment
      END AS account_demographics_sales_segment,
      account_demographics_sales_segment_grouped,
      CASE
        WHEN account_demographics_geo IS NULL OR UPPER(account_demographics_geo) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_geo
      END AS account_demographics_geo,
      CASE
        WHEN account_demographics_region IS NULL OR UPPER(account_demographics_region) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_region
      END AS account_demographics_region,
      CASE
        WHEN account_demographics_area IS NULL OR UPPER(account_demographics_area) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_area
      END AS account_demographics_area,
      account_demographics_segment_region_grouped,
      account_demographics_territory,
      account_demographics_employee_count,
      account_demographics_max_family_employee,
      account_demographics_upa_country,
      account_demographics_upa_state,
      account_demographics_upa_city,
      account_demographics_upa_street,
      account_demographics_upa_postal_code,
      crm_partner_id,
      ga_client_id,
      cognism_company_office_city,
      cognism_company_office_state,
      cognism_company_office_country,
      cognism_city,
      cognism_state,
      cognism_country,
      cognism_employee_count,
      leandata_matched_account_billing_state,
      leandata_matched_account_billing_postal_code,
      leandata_matched_account_billing_country,
      leandata_matched_account_employee_count,
      leandata_matched_account_sales_segment,
      zoominfo_contact_city,
      zoominfo_contact_state,
      zoominfo_contact_country,
      zoominfo_company_city,
      zoominfo_company_state,
      zoominfo_company_country,
      zoominfo_phone_number, 
      zoominfo_mobile_phone_number,
      zoominfo_do_not_call_direct_phone,
      zoominfo_do_not_call_mobile_phone,
      traction_first_response_time,
      traction_first_response_time_seconds,
      traction_response_time_in_business_hours,
      last_transfer_date_time,
      time_from_last_transfer_to_sequence,
      time_from_mql_to_last_transfer,
      ptp_score_date                                 AS propensity_to_purchase_score_date,
      ptp_score_group                                AS propensity_to_purchase_score_group,
      pql_namespace_creator_job_description,
      pql_namespace_id,
      pql_namespace_name,
      pql_namespace_users,
      is_product_qualified_lead,
      ptp_days_since_trial_start                     AS propensity_to_purchase_days_since_trial_start,
      ptp_insights                                   AS propensity_to_purchase_insights,
      is_ptp_contact,
      ptp_namespace_id                               AS propensity_to_purchase_namespace_id,
      ptp_past_insights                              AS propensity_to_purchase_past_insights,
      ptp_past_score_group                           AS propensity_to_purchase_past_score_group,
      lead_score_classification,
      zoominfo_company_employee_count,
      NULL AS zoominfo_contact_id,
      is_partner_recalled,
      CASE
        WHEN crm_tasks.min_task_completed_date_by_bdr_sdr IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS is_bdr_sdr_worked,
      created_date

    FROM sfdc_leads
    LEFT JOIN biz_person_with_touchpoints
      ON sfdc_leads.lead_id = biz_person_with_touchpoints.bizible_lead_id
    LEFT JOIN marketo_persons
      ON sfdc_leads.lead_id = marketo_persons.sfdc_lead_id and sfdc_type = 'Lead'
    LEFT JOIN crm_tasks
      ON sfdc_leads.lead_id=crm_tasks.sfdc_record_id
    WHERE is_converted = 'FALSE'

), final AS (

    SELECT
      crm_person_final.*,
      LOWER(COALESCE(
                     account_demographics_upa_country,
                     zoominfo_company_country,
                     country
                    )
       ) AS two_letter_person_first_country,
      CASE
        -- remove a few case where value is only numbers
        WHEN (TRY_TO_NUMBER(two_letter_person_first_country)) IS NOT NULL THEN
             NULL
        WHEN LEN(two_letter_person_first_country) = 2 AND prep_location_country.country_name IS NOT NULL THEN
            INITCAP(prep_location_country.country_name)
        WHEN LEN(two_letter_person_first_country) = 2 THEN
            -- This condition would be for a country code that isn't on the model
            UPPER(two_letter_person_first_country)
        ELSE
            INITCAP(two_letter_person_first_country)
      END AS person_first_country,
      CASE
        WHEN LEN(two_letter_person_first_country) = 2 THEN UPPER(two_letter_person_first_country)
        WHEN two_letter_person_first_country LIKE '%ivoire%' THEN 'CI'
        WHEN two_letter_person_first_country = 'saint kitts and nevis' THEN 'KN'
        WHEN two_letter_person_first_country = 'democratic repulic of congo' THEN 'CD'
        WHEN two_letter_person_first_country = 'congo' THEN 'CG'
        WHEN two_letter_person_first_country = 'libyan arab jamahiriya' THEN 'LY'
        WHEN two_letter_person_first_country = 'holy see (vatican city state)' THEN 'VA'
        WHEN two_letter_person_first_country = 'viet nam' THEN 'VN'
        WHEN two_letter_person_first_country = 'timor-leste' THEN 'TL'
        WHEN two_letter_person_first_country = 'bonaire saint eustatius and saba' THEN 'BQ'
        WHEN two_letter_person_first_country = 'czech republic' THEN 'CZ'
        WHEN two_letter_person_first_country = 'cape verde' THEN 'CV'
        WHEN two_letter_person_first_country LIKE 'korea, democratic%' THEN 'KP'
        WHEN two_letter_person_first_country = 'cambodia.' THEN 'KH'
        WHEN two_letter_person_first_country LIKE 'lao people%' THEN 'LA'
        WHEN two_letter_person_first_country = 'reunion' THEN 'RE'
        WHEN two_letter_person_first_country = 'macedonia, the former yugoslav republic of' THEN 'MK'
        WHEN two_letter_person_first_country = 'south africa centurion' THEN 'ZA'
        WHEN two_letter_person_first_country = 'taiwan, province of china' THEN 'TW'
        WHEN two_letter_person_first_country = 'brunei darussalam' THEN 'BN'
        WHEN two_letter_person_first_country = 'bosnia and herzegowina' THEN 'BA'
        WHEN two_letter_person_first_country = 'falkland islands (malvinas)' THEN 'FK'
        WHEN two_letter_person_first_country = 'brasil' THEN 'BR'
        WHEN two_letter_person_first_country = 'korea, republic of' THEN 'KR'
        WHEN two_letter_person_first_country = 'aland islands' THEN 'AX'
        WHEN two_letter_person_first_country = 'palestinian territory, occupied' THEN 'PS'
        WHEN two_letter_person_first_country = 'moldova' THEN 'MD'
        WHEN two_letter_person_first_country = 'russian federation' THEN 'RU'
        WHEN two_letter_person_first_country = 'swaziland' THEN 'SZ'
        WHEN two_letter_person_first_country = 'us virgin islands' THEN 'VI'
        WHEN two_letter_person_first_country = 'virgin islands, british' THEN 'VG'
        WHEN two_letter_person_first_country = 'iran, islamic republic of' THEN 'IR'
        WHEN two_letter_person_first_country = 'pitcairn' THEN 'PN'
        WHEN two_letter_person_first_country = 'congo, the democratic republic of the' THEN 'CD'
        WHEN two_letter_person_first_country = 'lithuania' THEN 'LT'
        WHEN two_letter_person_first_country = 'moldova, republic of' THEN 'MD'
        WHEN two_letter_person_first_country = 'saint helena, ascension and tristan da cunha' THEN 'SH'
        WHEN two_letter_person_first_country = 'sao tome and principe' THEN 'ST'
        WHEN two_letter_person_first_country = 'united republic of tanzania' THEN 'TZ'
        WHEN two_letter_person_first_country = 'cocos (keeling) islands' THEN 'CC'
        WHEN two_letter_person_first_country = 'macedonia' THEN 'MK'
        WHEN two_letter_person_first_country = 'the bahamas' THEN 'BS'
        WHEN two_letter_person_first_country = 'costa rica br' THEN 'CR'
        WHEN two_letter_person_first_country = 'fiji islands' THEN 'FJ'
        WHEN two_letter_person_first_country = 'saint martin (french part)' THEN 'MF'
        WHEN two_letter_person_first_country = 'sint maarten (dutch part)' THEN 'SX'
        WHEN two_letter_person_first_country = 't3k' THEN 'null'
        WHEN two_letter_person_first_country = 'tanzania, united republic of' THEN 'TZ'
        WHEN two_letter_person_first_country = 'syrian arab republic' THEN 'SY'
        WHEN two_letter_person_first_country = 'null' THEN 'null'
        WHEN two_letter_person_first_country = 'bonaire, saint eustatius and saba' THEN 'BQ'
        WHEN two_letter_person_first_country = 'jordan' THEN 'JO'
        WHEN two_letter_person_first_country = 'españa' THEN 'ES'
        ELSE two_letter_country.iso_2_country_code
      END                AS iso_2_country_code_final,
      CASE
        WHEN iso_2_country_code_final IN
          (
            'AG', 'AI', 'AR', 'AW', 'BB', 'BM', 'BO', 'BQ', 'BR', 'BS', 'BZ', 'CA', 'CL', 'CO', 'CR', 'DO', 'EC', 'GD', 'BL', 'GP', 'CU', 'GT',
            'GY', 'HN', 'HT', 'CW', 'DM', 'JM', 'FK', 'GF', 'KY', 'LC', 'GS', 'KN', 'MQ', 'MF', 'MX', 'NI', 'PA', 'PE', 'MS', 'PM', 'PY', 'SX',
            'SR', 'SV', 'TC', 'VG', 'TT', 'US', 'UY', 'WF', 'VC', 'VE'
          ) THEN 'AMER'
        WHEN iso_2_country_code_final IN
          (
            'AAQ', 'BN', 'CC', 'TF', 'ID', 'KH', 'LA', 'MM', 'MN', 'MY', 'PH', 'TH', 'TW', 'VN', 'AU', 'CK', 'CX', 'FJ', 'HM', 'IO', 'KI', 'MH',
            'NC', 'NF', 'NR', 'NU', 'PF', 'PG', 'PN', 'SB', 'TK', 'TL', 'TO', 'TV', 'VU', 'WS', 'KP', 'KR', 'SG', 'BD', 'BT', 'IN', 'LK', 'MV',
            'NP', 'NZ'
          ) THEN 'APAC'
        WHEN iso_2_country_code_final IN
          (
            'AE', 'AF', 'AO', 'BF', 'BH', 'BI', 'BJ', 'BW', 'CD', 'CF', 'CG', 'CI', 'CM', 'CV', 'DJ', 'EG', 'ER', 'ET', 'GA', 'GH', 'GM', 'SM',
            'GN', 'GQ', 'GW', 'IQ', 'IR', 'VA', 'JO', 'KE', 'KM', 'KW', 'LB', 'LR', 'LS', 'LY', 'MG', 'ML', 'MR', 'MU', 'MW', 'MZ', 'NA', 'NE',
            'NG', 'OM', 'PK', 'PS', 'QA', 'RW', 'SA', 'SC', 'SD', 'SL', 'SN', 'SO', 'SS', 'ST', 'SY', 'SZ', 'TD', 'TG', 'TM', 'TR', 'TZ', 'UG',
            'YE', 'ZA', 'ZM', 'ZW', 'AL', 'BA', 'CY', 'GR', 'HR', 'BE', 'ME', 'MK', 'MT', 'RS', 'LU', 'SI', 'BG', 'CZ', 'EE', 'HU', 'LT', 'LV',
            'PL', 'RO', 'SK', 'AM', 'AZ', 'BY', 'GE', 'KG', 'KZ', 'MD', 'RU', 'TJ', 'UA', 'UZ', 'AD', 'ES', 'PT', 'DZ', 'IL', 'EH', 'IT', 'MA',
            'TN', 'SH', 'YT', 'AT', 'CH', 'DE', 'LI', 'MC', 'RE', 'FR', 'BV', 'FI', 'GL', 'IS', 'SE', 'GB', 'GG', 'GI', 'SJ', 'IE', 'IM', 'DK',
            'JE', 'NL', 'FO', 'NO', 'AX'
          ) THEN 'EMEA'
        WHEN iso_2_country_code_final = 'JP' THEN 'JAPAN'
        WHEN iso_2_country_code_final IN
          (
            'AG', 'AI', 'AR', 'AW', 'BB', 'BM', 'BO', 'BQ', 'BR', 'BS', 'BZ', 'CA', 'CL', 'CO', 'CR', 'DO', 'EC', 'GD', 'BL', 'GP', 'CU', 'GT',
            'GY', 'HN', 'HT', 'CW', 'DM', 'JM', 'FK', 'GF', 'KY', 'LC', 'GS', 'KN', 'MQ', 'MF', 'MX', 'NI', 'PA', 'PE', 'MS', 'PM', 'PY', 'SX',
            'SR', 'SV', 'TC', 'VG', 'TT', 'US', 'UY', 'WF', 'VC', 'VE'
          ) THEN 'JIHU'
        ELSE 'OTHER'
      END                AS person_first_geo,
      CASE
        WHEN account_demographics_geo IN ('CHANNEL','UNKNOWN','Channel') OR account_demographics_geo IS NULL
          THEN person_first_geo
        ELSE account_demographics_geo 
      END AS person_geo_combined,
      final_iso_country.country_name AS country_name_iso_based,
      prep_date.fiscal_year  AS created_date_fiscal_year,
      CONCAT(
        UPPER(crm_person_final.account_demographics_sales_segment),
        '-',
        UPPER(crm_person_final.account_demographics_geo),
        '-',
        UPPER(crm_person_final.account_demographics_region),
        '-',
        UPPER(crm_person_final.account_demographics_area),
        '-',
        prep_date.fiscal_year
      ) AS dim_account_demographics_hierarchy_sk

    FROM crm_person_final
    LEFT JOIN prep_date
      ON prep_date.date_actual = crm_person_final.created_date::DATE
    LEFT JOIN prep_location_country
      ON two_letter_person_first_country = LOWER(prep_location_country.iso_2_country_code)
      -- Only join when the value is 2 letters
      AND LEN(two_letter_person_first_country) = 2
    LEFT JOIN prep_location_country AS two_letter_country
      ON two_letter_person_first_country = LOWER(two_letter_country.country_name)
    LEFT JOIN prep_location_country AS final_iso_country 
      ON iso_2_country_code_final = final_iso_country.iso_2_country_code
    WHERE sfdc_record_id != '00Q4M00000kDDKuUAO' --DQ issue: https://gitlab.com/gitlab-data/analytics/-/issues/11559

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2020-12-08",
    updated_date="2024-02-06"
) }}
