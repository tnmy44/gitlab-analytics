WITH source_itemized as (

  SELECT *
  FROM {{ ref('elastic_billing_itemized')}}

)

SELECT * FROM source_itemized