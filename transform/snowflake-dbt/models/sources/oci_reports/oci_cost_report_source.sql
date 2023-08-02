{{ config(
  enabled=false
) }}

WITH source as (

    SELECT *

      FROM {{ source('oci_reports', 'oci_cost_report') }}

), renamed as (

    SELECT DISTINCT

        lineitem__referenceno::VARCHAR                           AS line_item_reference_no,
        lineitem__tenantid::VARCHAR                              AS line_item_tenant_id,
        lineitem__intervalusagestart::TIMESTAMP                  AS line_item_interval_usage_start,
        lineitem__intervalusageend::TIMESTAMP                    AS line_item_interval_usage_end,
        product__service::VARCHAR                                AS product_service,
        product__compartmentid::VARCHAR                          AS product_compartment_id,
        product__compartmentname::VARCHAR                        AS product_compartment_name,
        product__region::VARCHAR                                 AS product_region,
        product__availabilitydomain::VARCHAR                     AS product_availability_domain,
        product__resourceid::VARCHAR                             AS product_resource_id,
        usage__billedquantity::FLOAT                             AS usage_billed_quantity,
        usage__billedquantityoverage::VARCHAR                    AS usage_billed_quantity_overage,
        cost__subscriptionid::NUMBER                             AS cost_subscription_id,
        cost__productsku::VARCHAR                                AS cost_product_sku,
        product__description::VARCHAR                            AS product_description,
        cost__unitprice::FLOAT                                   AS cost_unit_price,
        cost__unitpriceoverage::VARCHAR                          AS cost_unit_price_overage,
        cost__mycost::FLOAT                                      AS cost_my_cost,
        cost__mycostoverage::VARCHAR                             AS cost_my_cost_overage,
        cost__currencycode::VARCHAR                              AS cost_currency_code,
        cost__billingunitreadable::VARCHAR                       AS cost_billing_unit_readable,
        cost__skuunitdescription::VARCHAR                        AS cost_sku_unit_description,
        cost__overageflag::VARCHAR                               AS cost_overage_flag,
        lineitem__iscorrection::VARCHAR                          AS line_item_is_correction,
        lineitem__backreferenceno::VARCHAR                       AS line_item_back_reference_no,
        tags__oracle_tags_createdby::VARCHAR                     AS tags_oracle_tags_created_by,
        tags__oracle_tags_createdon::TIMESTAMP                   AS tags_oracle_tags_created_on,
        tags__cluster_name::VARCHAR                              AS tags_cluster_name,
        tags__oci_compute_instanceconfiguration::VARCHAR         AS tags_oci_compute_instance_configuration,
        tags__oci_compute_instancepool::VARCHAR                  AS tags_oci_compute_instance_pool,
        tags__oci_compute_instancepool_opcretrytoken::VARCHAR    AS tags_oci_compute_instance_pool_opc_retry_token,
        tags__orcl_cloud_free_tier_retained::VARCHAR             AS tags_orcl_cloud_free_tier_retained,
        tags__parent_cluster::VARCHAR                            AS tags_parent_cluster

    FROM source

)

SELECT *
FROM renamed