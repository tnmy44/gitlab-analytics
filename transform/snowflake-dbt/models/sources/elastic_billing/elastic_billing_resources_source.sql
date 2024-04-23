with source as (  
  SELECT *
  FROM  {{ source('elastic_billing', 'itemized_costs') }}
),

parsed as (
  SELECT 
    source.extraction_start_date,
    source.extraction_end_date,
    resources.value['period']['start']::TIMESTAMP AS resource_start_date,
    resources.value['period']['end']::TIMESTAMP AS resource_end_date,
    resources.value['hours']::float AS hours,
    resources.value['instance_count']::NUMBER AS instance_count,
    resources.value['kind']::varchar AS kind,
    resources.value['price']::varchar AS cost,
    resources.value['name']::varchar AS name,
    resources.value['price_per_hour']::float AS price_per_hour,
    resources.value['sku']::varchar AS sku,
    to_timestamp(source._uploaded_at::int) AS _uploaded_at
  FROM source
      INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(payload), outer => TRUE) AS dims
      INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(dims.value), outer => TRUE, mode => 'ARRAY') AS resources
  WHERE dims.path = 'resources'
  QUALIFY 
    ROW_NUMBER () OVER (
        PARTITION BY source.extraction_end_date, resources.value['sku']::VARCHAR
            ORDER BY to_timestamp(source._uploaded_at::int) DESC
    ) = 1
)

SELECT * 
FROM parsed
