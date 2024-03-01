WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'bizible_person') }}

), renamed AS (

    SELECT
      id                              AS person_id,
      bizible2__lead__c               AS bizible_lead_id,
      bizible2__contact__c            AS bizible_contact_id,

      isdeleted::BOOLEAN              AS is_deleted
      
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bizible_contact_id ORDER BY lastmodifieddate DESC) = 1
)

SELECT *
FROM renamed
