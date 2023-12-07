WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'summary_partner_discount') }}

), renamed AS (

    SELECT
      FY AS fiscal_year_name_fy,
      "Opportunity ID (18)"::VARCHAR    AS dim_crm_opportunity_id,
      "Close Date"::DATE                AS close_date,
      "Lessor of Discounts"::NUMBER     AS discount_percent,
      "Partner Type"::VARCHAR           AS partner_type
    FROM source

)

SELECT *
FROM renamed
