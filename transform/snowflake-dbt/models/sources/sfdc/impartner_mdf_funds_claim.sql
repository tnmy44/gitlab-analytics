
WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'impartnermdf__fundsclaim__c') }}

), renamed AS (

    SELECT
      -- ids
      id                                        AS funds_claim_id,
      name                                      AS funds_claim_name,

      -- info
      impartnermdf__claimamount__c              AS claim_amount,
      impartnermdf__fundsrequest__c             AS funds_request,
      impartnermdf__invoicenumber__c            AS invoice_number,
      impartnermdf__notifypartner__c            AS notify_partner,
      impartnermdf__paiddate__c                 AS paid_date,
      impartnermdf__partneraccount__c           AS partner_account,
      impartnermdf__partnercontact__c           AS partner_contact,
      impartnermdf__paymentamount__c            AS payment_amount,
      impartnermdf__paymentmethod__c            AS payment_method,
      impartnermdf__performanceverified__c      AS performance_verified,
      impartnermdf__ponumber__c                 AS po_number,
      impartnermdf__popreceived__c              AS po_received,
      impartnermdf__rejectionreason__c          AS rejection_reason,
      impartnermdf__status__c                   AS status,

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
