-- depends_on: {{ ref('zuora_excluded_accounts') }}

{{ config(
    materialized="view",
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'invoice_aging_detail') }}

), renamed AS (

    SELECT
   -- primary key 
      source.id                          	               AS invoice_aging_detail_id,

   -- keys
      source.invoiceid               	                   AS invoice_id,     
      source.accountingperiodid                  	       AS accounting_period_id,

   -- invoice aging detail dates
      source.accountingperiodenddate                     AS accounting_period_end_date,

   -- additive fields
      source.accountbalanceimpact                      	 AS account_balance_impact,
      source.daysoverdue                     	           AS days_overdue,
    

      -- ext1, ext2, ext3, ... ext9

      -- metadata
      createdbyid                     AS created_by_id,
      createddate                     AS created_date,
      postedby                        AS posted_by,
      source                          AS source,
      source                          AS source_id,
      updatedbyid                     AS updated_by_id,
      updateddate                     AS updated_date,
      deleted                         AS is_deleted

    FROM source

)

SELECT *
FROM renamed
