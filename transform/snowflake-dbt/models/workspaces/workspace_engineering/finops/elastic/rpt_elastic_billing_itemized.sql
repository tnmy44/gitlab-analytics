WITH data_transfer_source AS (

  SELECT * FROM {{ ref ('elastic_billing_data_transfer_source') }}

),

resource_source AS (

  SELECT * FROM {{ ref ('elastic_billing_resources_source') }}

),

renamed AS (

  SELECT
    extraction_start_date,
    extraction_end_date,
    'data_transfer'                      AS charge_type,
    sku,
    cost,
    resource_quantity_value              AS usage,
    CASE WHEN formated_quantity_value LIKE '%GB%' THEN 'bytes'
      WHEN formated_quantity_value LIKE '%requests%' THEN 'requests'
    END                                  AS usage_unit,
    ARRAY_CONSTRUCT(type, resource_name) AS details
  FROM data_transfer_source
  UNION ALL
  SELECT
    extraction_start_date,
    extraction_end_date,
    'capacity' AS charge_type,
    sku,
    cost,
    hours      AS usage,
    'hours'    AS usage_unit,
    ARRAY_CONSTRUCT(resource_start_date, resource_end_date, instance_count, price_per_hour, name)
  FROM resource_source
),


transformed AS (
  SELECT
    extraction_end_date                        AS day,
    charge_type,
    sku,
    usage_unit,
    LAG(cost, 1) OVER (PARTITION BY
      extraction_start_date,
      charge_type,
      sku,
      usage_unit
    ORDER BY extraction_end_date DESC) - cost  AS daily_cost,
    LAG(usage, 1) OVER (PARTITION BY
      extraction_start_date,
      charge_type,
      sku,
      usage_unit
    ORDER BY extraction_end_date DESC) - usage AS daily_usage,
    cost,
    usage,
    details
  FROM renamed
)

SELECT
  day,
  charge_type,
  sku,
  usage_unit,
  daily_cost,
  cost,
  daily_usage,
  usage,
  details
FROM transformed
WHERE daily_usage IS NOT NULL
  AND daily_cost IS NOT NULL
