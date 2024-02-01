
WITH source as (
SELECT
*
FROM {{ source('elastic_billing', 'itemized_costs') }}, lateral flatten (input => parse_json(payload), outer => true)
),

cost_dimensions as (

    SELECT     
    extraction_start_date,
    extraction_end_date,
    value:dimensions[0]:cost as capacity_cost,
    value:dimensions[1]:cost as data_in_cost,
    value:dimensions[2]:cost as data_internode_cost,
    value:dimensions[3]:cost as data_out_cost,
    value:dimensions[4]:cost as storage_api_cost,
    value:dimensions[5]:cost as storage_bytes_cost FROM source 
    WHERE path = 'costs'

),


data_transfer_dimensions as (

    SELECT 
    extraction_start_date,
    extraction_end_date,

    value[0]:cost as cost_gcp_data_transfer_in,
    value[0]:quantity:value::float/pow(1024,3) as usage_gcp_data_transfer_in_gb,
    value[0]:name as cost_gcp_data_transfer_in_formatted_name,

    value[1]:cost as cost_gcp_data_transfer_inter_node,
    value[1]:quantity:value::float/pow(1024,3) as usage_gcp_data_transfer_inter_node_gb,
    value[1]:name as cost_gcp_data_transfer_inter_node_formatted_name,

    value[2]:cost as cost_gcp_data_transfer_out,
    value[2]:quantity:value::float/pow(1024,3) as usage_gcp_data_transfer_out_gb,
    value[2]:name as cost_gcp_data_transfer_out_formatted_name,

    value[3]:cost as cost_gcp_snapshot_storage_api,
    value[3]:quantity:value::float as usage_gcp_snapshot_storage_api_requests,
    value[3]:name as cost_gcp_snapshot_transfer_out_formatted_name,

    value[4]:cost as cost_gcp_snapshot_storage,
    value[4]:quantity:value::float/pow(1024,3) as usage_gcp_snapshot_storage_gb_month,
    value[4]:name as cost_gcp_snapshot_storage_formatted_name

    FROM source 
    WHERE path = 'data_transfer_and_storage'

),

resources_dimensions as (

    SELECT     
    extraction_start_date,
    extraction_end_date,
    value::ARRAY as usage_details
    FROM source 
    WHERE path = 'resources'

)

SELECT 
-- cost dimensions
cost_dimensions.extraction_start_date, 
cost_dimensions.extraction_end_date,
cost_dimensions.capacity_cost as cost_capacity,
cost_dimensions.data_in_cost as cost_data_in,
cost_dimensions.data_internode_cost as cost_data_internode,
cost_dimensions.data_out_cost as cost_data_out,
cost_dimensions.storage_api_cost as cost_storage_api,
cost_dimensions.storage_bytes_cost as cost_storage_bytes,

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
ON data_transfer_dimensions.extraction_start_date = cost_dimensions.extraction_start_date
AND data_transfer_dimensions.extraction_end_date = cost_dimensions.extraction_end_date
LEFT JOIN resources_dimensions 
ON resources_dimensions.extraction_start_date = cost_dimensions.extraction_start_date
AND resources_dimensions.extraction_start_date = cost_dimensions.extraction_start_date




