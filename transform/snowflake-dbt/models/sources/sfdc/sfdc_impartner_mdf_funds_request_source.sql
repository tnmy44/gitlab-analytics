
WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'impartner_mdf_funds_request') }}

), renamed AS (

    SELECT
      -- ids
      id                                        AS funds_request_id,
      name                                      AS funds_request_name,

      -- info
      impartnermdf__activitylocationcity__c     AS activity_location_city,
      impartnermdf__activitylocationcountry__c  AS activity_location_country,
      impartnermdf__activitylocationstateprovince__c AS activity_location_state_province,
      impartnermdf__activityname__c             AS activity_name,
      impartnermdf__approvalpo__c               AS approval_po,
      impartnermdf__approvedfundrequestamount__c AS approved_fund_request_amount,
      impartnermdf__budgetedcost__c             AS budgeted_cost,
      impartnermdf__cancelmdfeventreason__c     AS cancel_mdf_event_reason,
      impartnermdf__cancelmdfevent__c           AS cancel_mdf_event,
      impartnermdf__description__c              AS description,
      impartnermdf__enddate__c                  AS end_date,
      impartnermdf__expectedresults__c          AS expected_results,
      impartnermdf__notifypartner__c            AS notify_partner,
      impartnermdf__partneraccount__c           AS partner_account,
      impartnermdf__partnercontact__c           AS partner_contact,
      impartnermdf__partnerinvestment__c        AS partner_investment,
      impartnermdf__requestedamount__c          AS requested_amount,
      impartnermdf__resourcedetail__c           AS resource_detail,
      impartnermdf__segmenttargettype__c        AS segment_target_type,
      impartnermdf__startdate__c                AS start_date,
      impartnermdf__status__c                   AS status,
      impartnermdf__targetnumberofattendees__c  AS target_number_of_attendees,
      impartnermdf__typeofmarketingactivity__c  AS type_of_marketing_activity,
      impartnermdf__typeofrequest__c            AS type_of_request,
      impartnermdf__verticalindustrytargettype__c AS vertical_industry_target_type,
      impartnermdf__willresourcesberequired__c  AS will_resources_be_required,
      mdf_notes__c                              AS mdf_notes,
      ownerid                                   AS owner_id,

      -- metadata
      createdbyid                               AS created_by_id,
      createddate                               AS created_date,
      isdeleted                                 AS is_deleted,
      lastmodifiedbyid                          AS last_modified_by_id,
      lastmodifieddate                          AS last_modified_date,
      _sdc_received_at                          AS sfdc_received_at,
      _sdc_extracted_at                         AS sfdc_extracted_at,
      _sdc_table_version                        AS sfdc_table_version,
      _sdc_batched_at                           AS sfdc_batched_at,
      _sdc_sequence                             AS sfdc_sequence,
      systemmodstamp                            AS system_mod_stamp

    FROM source

)

SELECT *
FROM renamed
