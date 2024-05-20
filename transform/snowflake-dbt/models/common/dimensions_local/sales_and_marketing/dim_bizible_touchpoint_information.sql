{{ config(
    materialized="table",
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('sfdc_lead_source','sfdc_lead_source'),
    ('sfdc_contact_source','sfdc_contact_source'),
    ('prep_crm_person','prep_crm_person'),
    ('prep_crm_touchpoint','prep_crm_touchpoint')
]) }}

, marketing_qualified_leads AS (
    
    SELECT
        {{ dbt_utils.generate_surrogate_key(['COALESCE(converted_contact_id, lead_id)','marketo_qualified_lead_datetime::timestamp']) }} AS mql_event_id,
        lead_id AS sfdc_record_id,
        MAX(marketo_qualified_lead_datetime)::timestamp AS mql_date_latest
    FROM sfdc_lead_source
    WHERE is_deleted = 'FALSE'
        AND (marketo_qualified_lead_datetime IS NOT NULL
            OR mql_datetime_inferred IS NOT NULL)


), marketing_qualified_contacts AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['contact_id','marketo_qualified_lead_datetime::timestamp']) }} AS mql_event_id,
        contact_id AS sfdc_record_id,
        MAX(marketo_qualified_lead_datetime)::timestamp AS mql_date_latest
    FROM sfdc_contact_source
    WHERE is_deleted = 'FALSE'
        AND (marketo_qualified_lead_datetime IS NOT NULL
            OR mql_datetime_inferred IS NOT NULL )
    HAVING mql_event_id NOT IN (
                         SELECT mql_event_id
                         FROM marketing_qualified_leads
                         )

), mql_person_prep AS (

    SELECT
        sfdc_record_id,
        mql_date_latest
    FROM marketing_qualified_leads
    UNION ALL
    SELECT
        sfdc_record_id,
        mql_date_latest
    FROM marketing_qualified_contacts
    
), bizible_mql_touchpoint_information_base AS (

    SELECT DISTINCT
        prep_crm_person.sfdc_record_id,
        prep_crm_touchpoint.touchpoint_id,
        prep_crm_touchpoint.bizible_touchpoint_date,
        prep_crm_touchpoint.bizible_form_url,
        prep_crm_touchpoint.campaign_id AS sfdc_campaign_id,
        prep_crm_touchpoint.bizible_ad_campaign_name,
        prep_crm_touchpoint.bizible_marketing_channel,
        prep_crm_touchpoint.bizible_marketing_channel_path,
        ROW_NUMBER () OVER (PARTITION BY prep_crm_person.sfdc_record_id ORDER BY prep_crm_touchpoint.bizible_touchpoint_date DESC) AS touchpoint_order_by_person
    FROM prep_crm_person
    LEFT JOIN mql_person_prep
        ON prep_crm_person.sfdc_record_id=mql_person_prep.sfdc_record_id
    LEFT JOIN prep_crm_touchpoint
        ON prep_crm_person.bizible_person_id = prep_crm_touchpoint.bizible_person_id
    WHERE prep_crm_touchpoint.touchpoint_id IS NOT null
        AND mql_person_prep.mql_date_latest IS NOT null
        AND prep_crm_touchpoint.bizible_touchpoint_date <= mql_person_prep.mql_date_latest
    ORDER BY prep_crm_touchpoint.bizible_touchpoint_date DESC

), bizible_mql_touchpoint_information_final AS (

  SELECT
      sfdc_record_id AS biz_mql_person_id,
      touchpoint_id AS bizible_mql_touchpoint_id,
      bizible_touchpoint_date AS bizible_mql_touchpoint_date,
      bizible_form_url AS bizible_mql_form_url,
      sfdc_campaign_id AS bizible_mql_sfdc_campaign_id,
      bizible_ad_campaign_name AS bizible_mql_ad_campaign_name,
      bizible_marketing_channel AS bizible_mql_marketing_channel,
      bizible_marketing_channel_path AS bizible_mql_marketing_channel_path
  FROM bizible_mql_touchpoint_information_base
  WHERE touchpoint_order_by_person = 1

), bizible_most_recent_touchpoint_information_base AS (

    SELECT DISTINCT
        prep_crm_person.sfdc_record_id,
        prep_crm_touchpoint.touchpoint_id,
        prep_crm_touchpoint.bizible_touchpoint_date,
        prep_crm_touchpoint.bizible_form_url,
        prep_crm_touchpoint.campaign_id AS sfdc_campaign_id,
        prep_crm_touchpoint.bizible_ad_campaign_name,
        prep_crm_touchpoint.bizible_marketing_channel,
        prep_crm_touchpoint.bizible_marketing_channel_path,
        ROW_NUMBER () OVER (PARTITION BY prep_crm_person.sfdc_record_id ORDER BY prep_crm_touchpoint.bizible_touchpoint_date DESC) AS touchpoint_order_by_person
    FROM prep_crm_person
    LEFT JOIN prep_crm_touchpoint
        ON prep_crm_person.bizible_person_id = prep_crm_touchpoint.bizible_person_id
    WHERE prep_crm_touchpoint.touchpoint_id IS NOT null

), bizible_most_recent_touchpoint_information_final AS (

  SELECT
      sfdc_record_id AS biz_most_recent_person_id,
      touchpoint_id AS bizible_most_recent_touchpoint_id,
      bizible_touchpoint_date AS bizible_most_recent_touchpoint_date,
      bizible_form_url AS bizible_most_recent_form_url,
      sfdc_campaign_id AS bizible_most_recent_sfdc_campaign_id,
      bizible_ad_campaign_name AS bizible_most_recent_ad_campaign_name,
      bizible_marketing_channel AS bizible_most_recent_marketing_channel,
      bizible_marketing_channel_path AS bizible_most_recent_marketing_channel_path
  FROM bizible_most_recent_touchpoint_information_base
  WHERE touchpoint_order_by_person = 1

), final AS (

    SELECT 
        bizible_most_recent_touchpoint_information_final.biz_most_recent_person_id AS bizible_person_id,
        bizible_mql_touchpoint_information_final.bizible_mql_touchpoint_id,
        bizible_mql_touchpoint_information_final.bizible_mql_touchpoint_date,
        bizible_mql_touchpoint_information_final.bizible_mql_form_url,
        bizible_mql_touchpoint_information_final.bizible_mql_sfdc_campaign_id,
        bizible_mql_touchpoint_information_final.bizible_mql_ad_campaign_name,
        bizible_mql_touchpoint_information_final.bizible_mql_marketing_channel,
        bizible_mql_touchpoint_information_final.bizible_mql_marketing_channel_path,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_touchpoint_id,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_touchpoint_date,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_form_url,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_sfdc_campaign_id,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_ad_campaign_name,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_marketing_channel,
        bizible_most_recent_touchpoint_information_final.bizible_most_recent_marketing_channel_path
    FROM bizible_most_recent_touchpoint_information_final
    LEFT JOIN bizible_mql_touchpoint_information_final
        ON bizible_most_recent_touchpoint_information_final.biz_most_recent_person_id=bizible_mql_touchpoint_information_final.biz_mql_person_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-05-17",
    updated_date="2024-05-20",
  ) }}