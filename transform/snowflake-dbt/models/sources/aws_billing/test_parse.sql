WITH dedicated_legacy_0475 AS (

  SELECT * FROM {{ source('aws_billing', 'dedicated_legacy_0475') }}

),

dedicated_dev_3675 AS (

  SELECT * FROM {{ source('aws_billing', 'dedicated_dev_3675') }}

),

gitlab_marketplace_5127 AS (

  SELECT * FROM {{ source('aws_billing', 'gitlab_marketplace_5127') }}

),

itorg_3027 AS (

  SELECT * FROM {{ source('aws_billing', 'itorg_3027') }}

),

legacy_gitlab_0347 AS (

  SELECT * FROM {{ source('aws_billing', 'legacy_gitlab_0347') }}

),

services_org_6953 AS (

  SELECT * FROM {{ source('aws_billing', 'services_org_6953') }}

),

all_raw AS (

SELECT * FROM dedicated_legacy_0475
UNION ALL
SELECT * FROM dedicated_dev_3675
UNION ALL
SELECT * FROM gitlab_marketplace_5127
UNION ALL
SELECT * FROM itorg_3027
UNION ALL
SELECT * FROM legacy_gitlab_0347
UNION ALL
SELECT * FROM services_org_6953

)

SELECT     
  t.value['bill_bill_type']::VARCHAR as bill_bill_type,
  t.value['bill_billing_entity']::VARCHAR as bill_billing_entity,
  t.value['bill_billing_period_end_date']::TIMESTAMP as bill_billing_period_end_date,
  t.value['bill_billing_period_start_date']::TIMESTAMP as bill_billing_period_start_date,
  t.value['bill_invoice_id']::VARCHAR as bill_invoice_id,
  t.value['bill_invoicing_entity']::VARCHAR as bill_invoicing_entity,
  t.value['bill_payer_account_id']::VARCHAR as bill_payer_account_id,
  t.value['discount_edp_discount']::NUMERIC as discount_edp_discount,
  t.value['discount_total_discount']::NUMERIC as discount_total_discount,
  t.value['identity_line_item_id']::VARCHAR as identity_line_item_id,
  t.value['identity_time_interval']::VARCHAR as identity_time_interval,
  t.value['line_item_availability_zone']::VARCHAR as line_item_availability_zone,
  t.value['line_item_blended_cost']::NUMERIC as line_item_blended_cost,
  t.value['line_item_blended_rate']::VARCHAR as line_item_blended_rate,
  t.value['line_item_currency_code']::VARCHAR as line_item_currency_code,
  t.value['line_item_legal_entity']::VARCHAR as line_item_legal_entity,
  t.value['line_item_line_item_description']::VARCHAR as line_item_line_item_description,
  t.value['line_item_line_item_type']::VARCHAR as line_item_line_item_type,
  t.value['line_item_net_unblended_cost']::NUMERIC as line_item_net_unblended_cost,
  t.value['line_item_net_unblended_rate']::VARCHAR as line_item_net_unblended_rate,
  t.value['line_item_normalization_factor']::NUMERIC as line_item_normalization_factor,
  t.value['line_item_normalized_usage_amount']::NUMERIC as line_item_normalized_usage_amount,
  t.value['line_item_operation']::VARCHAR as line_item_operation,
  t.value['line_item_product_code']::VARCHAR as line_item_product_code,
  t.value['line_item_resource_id']::VARCHAR as line_item_resource_id,
  t.value['line_item_tax_type']::VARCHAR as line_item_tax_type,
  t.value['line_item_unblended_cost']::NUMERIC as line_item_unblended_cost,
  t.value['line_item_unblended_rate']::VARCHAR as line_item_unblended_rate,
  t.value['line_item_usage_account_id']::VARCHAR as line_item_usage_account_id,
  t.value['line_item_usage_amount']::NUMERIC as line_item_usage_amount,
  t.value['line_item_usage_end_date']::TIMESTAMP as line_item_usage_end_date,
  t.value['line_item_usage_start_date']::TIMESTAMP as line_item_usage_start_date,
  t.value['line_item_usage_type']::VARCHAR as line_item_usage_type,
  t.value['pricing_currency']::VARCHAR as pricing_currency,
  t.value['pricing_public_on_demand_cost']::NUMERIC as pricing_public_on_demand_cost,
  t.value['pricing_public_on_demand_rate']::VARCHAR as pricing_public_on_demand_rate,
  t.value['pricing_rate_code']::VARCHAR as pricing_rate_code,  
  t.value['pricing_rate_id']::VARCHAR as pricing_rate_id,
  t.value['pricing_term']::VARCHAR as pricing_term,
  t.value['pricing_unit']::VARCHAR as pricing_unit,
  t.value['product_alarm_type']::VARCHAR as product_alarm_type,
  t.value['product_availability']::VARCHAR as product_availability,
  t.value['product_availability_zone']::VARCHAR as product_availability_zone,
  t.value['product_cache_engine']::VARCHAR as product_cache_engine,
  t.value['product_capacitystatus']::VARCHAR as product_capacitystatus,
  t.value['product_classicnetworkingsupport']::VARCHAR as product_classicnetworkingsupport,
  t.value['product_clock_speed']::VARCHAR as product_clock_speed,
  t.value['product_content_type']::VARCHAR as product_content_type,
  t.value['product_current_generation']::VARCHAR as product_current_generation,
  t.value['product_database_engine']::VARCHAR as product_database_engine,
  t.value['product_dedicated_ebs_throughput']::VARCHAR as product_dedicated_ebs_throughput,
  t.value['product_deployment_option']::VARCHAR as product_deployment_option,
  t.value['product_description']::VARCHAR as product_description,
  t.value['product_durability']::VARCHAR as product_durability,  
  t.value['product_ecu']::VARCHAR as product_ecu,
  t.value['product_engine_code']::VARCHAR as product_engine_code,
  t.value['product_enhanced_networking_supported']::VARCHAR as product_enhanced_networking_supported,
  t.value['product_free_tier']::VARCHAR as product_free_tier,
  t.value['product_from_location']::VARCHAR as product_from_location,
  t.value['product_from_location_type']::VARCHAR as product_from_location_type,
  t.value['product_from_region_code']::VARCHAR as product_from_region_code,
  t.value['product_group']::VARCHAR as product_group,
  t.value['product_group_description']::VARCHAR as product_group_description,
  t.value['product_instance_family']::VARCHAR as product_instance_family,
  t.value['product_instance_type']::VARCHAR as product_instance_type,
  t.value['product_instance_type_family']::VARCHAR as product_instance_type_family,
  t.value['product_intel_avx2_available']::VARCHAR as product_intel_avx2_available,
  t.value['product_intel_avx_available']::VARCHAR as product_intel_avx_available,
  t.value['product_intel_turbo_available']::VARCHAR as product_intel_turbo_available,
  t.value['product_license_model']::VARCHAR as product_license_model,
  t.value['product_location']::VARCHAR as product_location,
  t.value['product_location_type']::VARCHAR as product_location_type,
  t.value['product_logs_destination']::VARCHAR as product_logs_destination,
  t.value['product_mailbox_storage']::VARCHAR as product_mailbox_storage,
  t.value['product_marketoption']::VARCHAR as product_marketoption,
  t.value['product_max_iops_burst_performance']::VARCHAR as product_max_iops_burst_performance,
  t.value['product_max_iopsvolume']::VARCHAR as product_max_iopsvolume,
  t.value['product_max_throughputvolume']::VARCHAR as product_max_throughputvolume,
  t.value['product_max_volume_size']::VARCHAR as product_max_volume_size,
  t.value['product_memory']::VARCHAR as product_memory,
  t.value['product_memory_gib']::VARCHAR as product_memory_gib,
  t.value['product_message_delivery_frequency']::VARCHAR as product_message_delivery_frequency,
  t.value['product_message_delivery_order']::VARCHAR as product_message_delivery_order,
  t.value['product_min_volume_size']::VARCHAR as product_min_volume_size,
  t.value['product_network_performance']::VARCHAR as product_network_performance,
  t.value['product_normalization_size_factor']::VARCHAR as product_normalization_size_factor,
  t.value['product_operating_system']::VARCHAR as product_operating_system,
  t.value['product_operation']::VARCHAR as product_operation,
  t.value['product_origin']::VARCHAR as product_origin,
  t.value['product_physical_processor']::VARCHAR as product_physical_processor,
  t.value['product_platopricingtype']::VARCHAR as product_platopricingtype,
  t.value['product_platousagetype']::VARCHAR as product_platousagetype, 
  t.value['product_platovolumetype']::VARCHAR as product_platovolumetype,
  t.value['product_pre_installed_sw']::VARCHAR as product_pre_installed_sw,
  t.value['product_pricing_unit']::VARCHAR as product_pricing_unit,
  t.value['product_processor_architecture']::VARCHAR as product_processor_architecture,
  t.value['product_processor_features']::VARCHAR as product_processor_features,
  t.value['product_product_family']::VARCHAR as product_product_family,
  t.value['product_product_name']::VARCHAR as product_product_name,
  t.value['product_provisioned']::VARCHAR as product_provisioned,
  t.value['product_queue_type']::VARCHAR as product_queue_type,
  t.value['product_recipient']::VARCHAR as product_recipient,
  t.value['product_region']::VARCHAR as product_region,
  t.value['product_region_code']::VARCHAR as product_region_code,
  t.value['product_routing_target']::VARCHAR as product_routing_target,
  t.value['product_routing_type']::VARCHAR as product_routing_type,
  t.value['product_servicecode']::VARCHAR as product_servicecode,
  t.value['product_servicename']::VARCHAR as product_servicename,
  t.value['product_sku']::VARCHAR as product_sku,
  t.value['product_storage']::VARCHAR as product_storage,
  t.value['product_storage_class']::VARCHAR as product_storage_class,
  t.value['product_storage_media']::VARCHAR as product_storage_media,
  t.value['product_tenancy']::VARCHAR as product_tenancy,
  t.value['product_tickettype']::VARCHAR as product_tickettype,
  t.value['product_tiertype']::VARCHAR as product_tiertype,
  t.value['product_to_location']::VARCHAR as product_to_location,
  t.value['product_to_location_type']::VARCHAR as product_to_location_type,
  t.value['product_to_region_code']::VARCHAR as product_to_region_code,
  t.value['product_transfer_type']::VARCHAR as product_transfer_type,
  t.value['product_usagetype']::VARCHAR as product_usagetype,
  t.value['product_vcpu']::VARCHAR as product_vcpu,
  t.value['product_version']::VARCHAR as product_version,
  t.value['product_volume_api_name']::VARCHAR as product_volume_api_name,
  t.value['product_volume_type']::VARCHAR as product_volume_type,
  t.value['product_vpcnetworkingsupport']::VARCHAR as product_vpcnetworkingsupport,
  t.value['reservation_amortized_upfront_cost_for_usage']::NUMERIC as reservation_amortized_upfront_cost_for_usage,
  t.value['reservation_amortized_upfront_fee_for_billing_period']::NUMERIC as reservation_amortized_upfront_fee_for_billing_period,
  t.value['reservation_effective_cost']::NUMERIC as reservation_effective_cost,
  t.value['reservation_end_time']::TIMESTAMP as reservation_end_time,
  t.value['reservation_modification_status']::VARCHAR as reservation_modification_status,
  t.value['reservation_net_amortized_upfront_cost_for_usage']::NUMERIC as reservation_net_amortized_upfront_cost_for_usage,
  t.value['reservation_net_amortized_upfront_fee_for_billing_period']::NUMERIC as reservation_net_amortized_upfront_fee_for_billing_period,
  t.value['reservation_net_effective_cost']::NUMERIC as reservation_net_effective_cost,
  t.value['reservation_net_recurring_fee_for_usage']::NUMERIC as reservation_net_recurring_fee_for_usage,
  t.value['reservation_net_unused_amortized_upfront_fee_for_billing_period']::NUMERIC as reservation_net_unused_amortized_upfront_fee_for_billing_period,
  t.value['reservation_net_unused_recurring_fee']::NUMERIC as reservation_net_unused_recurring_fee,
  t.value['reservation_net_upfront_value']::NUMERIC as reservation_net_upfront_value,
  t.value['reservation_normalized_units_per_reservation']::VARCHAR as reservation_normalized_units_per_reservation,
  t.value['reservation_number_of_reservations']::VARCHAR as reservation_number_of_reservations,
  t.value['reservation_recurring_fee_for_usage']::NUMERIC as reservation_recurring_fee_for_usage,
  t.value['reservation_start_time']::TIMESTAMP as reservation_start_time,
  t.value['reservation_subscription_id']::VARCHAR as reservation_subscription_id,
  t.value['reservation_total_reserved_normalized_units']::VARCHAR as reservation_total_reserved_normalized_units,
  t.value['reservation_total_reserved_units']::VARCHAR as reservation_total_reserved_units,
  t.value['reservation_units_per_reservation']::VARCHAR as reservation_units_per_reservation,
  t.value['reservation_unused_amortized_upfront_fee_for_billing_period']::NUMERIC as reservation_unused_amortized_upfront_fee_for_billing_period,
  t.value['reservation_unused_normalized_unit_quantity']::NUMERIC as reservation_unused_normalized_unit_quantity,
  t.value['reservation_unused_quantity']::NUMERIC as reservation_unused_quantity,
  t.value['reservation_unused_recurring_fee']::NUMERIC as reservation_unused_recurring_fee,
  t.value['reservation_upfront_value']::NUMERIC as reservation_upfront_value,
  t.value['savings_plan_amortized_upfront_commitment_for_billing_period']::NUMERIC as savings_plan_amortized_upfront_commitment_for_billing_period,
  t.value['savings_plan_net_amortized_upfront_commitment_for_billing_period']::DECIMAL as savings_plan_net_amortized_upfront_commitment_for_billing_period,
  t.value['savings_plan_net_recurring_commitment_for_billing_period']::DECIMAL as savings_plan_net_recurring_commitment_for_billing_period,
  t.value['savings_plan_net_savings_plan_effective_cost']::DECIMAL as savings_plan_net_savings_plan_effective_cost,
  t.value['savings_plan_recurring_commitment_for_billing_period']::DECIMAL as savings_plan_recurring_commitment_for_billing_period,
  t.value['savings_plan_savings_plan_a_r_n']::VARCHAR as savings_plan_savings_plan_a_r_n,
  t.value['savings_plan_savings_plan_effective_cost']::DECIMAL as savings_plan_savings_plan_effective_cost,
  t.value['savings_plan_savings_plan_rate']::DECIMAL as savings_plan_savings_plan_rate,
  t.value['savings_plan_total_commitment_to_date']::DECIMAL as savings_plan_total_commitment_to_date,
  t.value['savings_plan_used_commitment']::DECIMAL as savings_plan_used_commitment
FROM all_raw as a, table(flatten(a.$1, 'root')) t


