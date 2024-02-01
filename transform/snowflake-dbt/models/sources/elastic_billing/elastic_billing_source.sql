WITH source AS (
  SELECT *
  FROM {{ source('elastic_billing', 'itemized_costs') }} INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(payload), outer => TRUE)
),

renamed AS (
    
  WITH cost_dimensions AS (

    SELECT
      extraction_start_date,
      extraction_end_date,
      value:dimensions[0]:cost AS capacity_cost
    FROM source
    WHERE path = 'costs'

  ),


  data_transfer_dimensions AS (

    SELECT
      extraction_start_date,
      extraction_end_date,

      value[0]['cost']                                    AS cost_gcp_data_transfer_in,
      value[0]['quantity']['value']::FLOAT / POW(1024, 3) AS usage_gcp_data_transfer_in_gb,
      value[0]['name']                                    AS cost_gcp_data_transfer_in_formatted_name,

      value[1]['cost']                                    AS cost_gcp_data_transfer_inter_node,
      value[1]['quantity']['value']::FLOAT / POW(1024, 3) AS usage_gcp_data_transfer_inter_node_gb,
      value[1]['name']                                    AS cost_gcp_data_transfer_inter_node_formatted_name,

      value[2]['cost']                                    AS cost_gcp_data_transfer_out,
      value[2]['quantity']['value']::FLOAT / POW(1024, 3) AS usage_gcp_data_transfer_out_gb,
      value[2]['name']                                    AS cost_gcp_data_transfer_out_formatted_name,

      value[3]['cost']                                    AS cost_gcp_snapshot_storage_api,
      value[3]['quantity']['value']::FLOAT                AS usage_gcp_snapshot_storage_api_requests,
      value[3]['name']                                    AS cost_gcp_snapshot_transfer_out_formatted_name,

      value[4]['cost']                                    AS cost_gcp_snapshot_storage,
      value[4]['quantity']['value']::FLOAT / POW(1024, 3) AS usage_gcp_snapshot_storage_gb_month,
      value[4]['name']                                    AS cost_gcp_snapshot_storage_formatted_name

    FROM source
    WHERE path = 'data_transfer_and_storage'

  ),

  resources_dimensions AS (

    SELECT
      extraction_start_date,
      extraction_end_date,
      value::ARRAY AS usage_details
    FROM source
    WHERE path = 'resources'

  ),

  wide AS (

    SELECT
    -- cost dimensions
      data_transfer_dimensions.extraction_start_date,
      data_transfer_dimensions.extraction_end_date,
      cost_dimensions.capacity_cost AS cost_capacity,

      --data_transfer_dimensions
      data_transfer_dimensions.cost_gcp_data_transfer_in,
      data_transfer_dimensions.usage_gcp_data_transfer_in_gb,
      data_transfer_dimensions.cost_gcp_data_transfer_in_formatted_name,

      data_transfer_dimensions.cost_gcp_data_transfer_inter_node,
      data_transfer_dimensions.usage_gcp_data_transfer_inter_node_gb,
      data_transfer_dimensions.cost_gcp_data_transfer_inter_node_formatted_name,

      data_transfer_dimensions.cost_gcp_data_transfer_out,
      data_transfer_dimensions.usage_gcp_data_transfer_out_gb,
      data_transfer_dimensions.cost_gcp_data_transfer_out_formatted_name,

      data_transfer_dimensions.cost_gcp_snapshot_storage_api,
      data_transfer_dimensions.usage_gcp_snapshot_storage_api_requests,
      data_transfer_dimensions.cost_gcp_snapshot_transfer_out_formatted_name,

      data_transfer_dimensions.cost_gcp_snapshot_storage,
      data_transfer_dimensions.usage_gcp_snapshot_storage_gb_month,
      data_transfer_dimensions.cost_gcp_snapshot_storage_formatted_name,

      -- resources dimensions
      resources_dimensions.usage_details
    FROM cost_dimensions
    LEFT JOIN data_transfer_dimensions
      ON cost_dimensions.extraction_start_date = data_transfer_dimensions.extraction_start_date
        AND cost_dimensions.extraction_end_date = data_transfer_dimensions.extraction_end_date
    LEFT JOIN resources_dimensions
      ON data_transfer_dimensions.extraction_start_date = resources_dimensions.extraction_start_date
        AND data_transfer_dimensions.extraction_start_date = resources_dimensions.extraction_start_date

  ),

  long AS (
    SELECT
      extraction_start_date,
      extraction_end_date,
      'capacity'    AS charge_type,
      cost_capacity AS cost,
      NULL          AS usage,
      NULL          AS usage_unit
    FROM wide
    UNION ALL
    SELECT
      extraction_start_date,
      extraction_end_date,
      'cost_gcp_data_transfer_in'              AS charge_type,
      cost_gcp_data_transfer_in                AS cost,
      usage_gcp_data_transfer_in_gb            AS usage,
      cost_gcp_data_transfer_in_formatted_name AS usage_unit
    FROM wide
    UNION ALL
    SELECT
      extraction_start_date,
      extraction_end_date,
      'cost_gcp_data_transfer_inter_node'              AS charge_type,
      cost_gcp_data_transfer_inter_node                AS cost,
      usage_gcp_data_transfer_inter_node_gb            AS usage,
      cost_gcp_data_transfer_inter_node_formatted_name AS usage_unit
    FROM wide
    UNION ALL
    SELECT
      extraction_start_date,
      extraction_end_date,
      'cost_gcp_data_transfer_out'              AS charge_type,
      cost_gcp_data_transfer_out                AS cost,
      usage_gcp_data_transfer_out_gb            AS usage,
      cost_gcp_data_transfer_out_formatted_name AS usage_unit
    FROM wide
    UNION ALL
    SELECT
      extraction_start_date,
      extraction_end_date,
      'cost_gcp_snapshot_storage_api'               AS charge_type,
      cost_gcp_snapshot_storage_api                 AS cost,
      usage_gcp_snapshot_storage_api_requests       AS usage,
      cost_gcp_snapshot_transfer_out_formatted_name AS usage_unit
    FROM wide
    UNION ALL
    SELECT
      extraction_start_date,
      extraction_end_date,
      'cost_gcp_snapshot_storage'              AS charge_type,
      cost_gcp_snapshot_storage                AS cost,
      usage_gcp_snapshot_storage_gb_month      AS usage,
      cost_gcp_snapshot_storage_formatted_name AS usage_unit
    FROM wide
  )

  SELECT * FROM long
)

SELECT * FROM renamed
