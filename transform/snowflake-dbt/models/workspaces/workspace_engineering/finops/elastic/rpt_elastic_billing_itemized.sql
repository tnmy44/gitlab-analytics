WITH source_itemized as (

  SELECT *
  FROM {{ ref('elastic_billing_source')}}

)

SELECT * FROM source_itemized