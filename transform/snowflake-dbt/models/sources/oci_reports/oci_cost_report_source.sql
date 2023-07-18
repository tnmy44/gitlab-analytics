WITH source as (

    SELECT *

      FROM {{ source('oci_reports', 'oci_cost_report') }}

), renamed as (

    SELECT DISTINCT

        line_item_reference_no::VARCHAR                           AS lineitem__referenceno,
        line_item_tenant_id::VARCHAR                              AS lineitem__tenantid,
        line_item_interval_usage_start::TIMESTAMP                  AS lineitem__intervalusagestart,
        line_item_interval_usage_end::TIMESTAMP                    AS lineitem__intervalusageend,
        product_service::VARCHAR                                AS product__service,
        product_compartment_id::VARCHAR                          AS product__compartmentid,
        product_compartment_name::VARCHAR                        AS product__compartmentname,
        product_region::VARCHAR                                 AS product__region,
        product_availability_domain::VARCHAR                     AS product__availabilitydomain,
        product_resource_id::VARCHAR                             AS product__resourceid,
        usage_billed_quantity::FLOAT                             AS usage__billedquantity,
        usage_billed_quantity_overage::VARCHAR                    AS usage__billedquantityoverage,
        cost_subscription_id::NUMBER                             AS cost__subscriptionid,
        cost_product_sku::VARCHAR                                AS cost__productsku,
        product_description::VARCHAR                            AS product__description,
        cost_unit_price::FLOAT                                   AS cost__unitprice,
        cost_unit_price_overage::VARCHAR                          AS cost__unitpriceoverage,
        cost_my_cost::FLOAT                                      AS cost__mycost,
        cost_my_cost_overage::VARCHAR                             AS cost__mycostoverage,
        cost_currency_code::VARCHAR                              AS cost__currencycode,
        cost_billing_unit_readable::VARCHAR                       AS cost__billingunitreadable,
        cost_sku_unit_description::VARCHAR                        AS cost__skuunitdescription,
        cost_overage_flag::VARCHAR                               AS cost__overageflag,
        line_item_is_correction::VARCHAR                          AS lineitem__iscorrection,
        line_item_back_reference_no::VARCHAR                       AS lineitem__backreferenceno,
        tags_oracle_tags_created_by::VARCHAR                     AS tags__oracle_tags_createdby,
        tags_oracle_tags_created_on::TIMESTAMP                   AS tags__oracle_tags_createdon,
        tags_cluster_name::VARCHAR                              AS tags__cluster_name,
        tags_oci_compute_instance_configuration::VARCHAR         AS tags__oci_compute_instanceconfiguration,
        tags_oci_compute_instance_pool::VARCHAR                  AS tags__oci_compute_instancepool,
        tags_oci_compute_instance_pool_opc_retry_token::VARCHAR    AS tags__oci_compute_instancepool_opcretrytoken,
        tags_orcl_cloud_free_tier_retained::VARCHAR             AS tags__orcl_cloud_free_tier_retained,
        tags_parent_cluster::VARCHAR                            AS tags__parent_cluster

    FROM source

)

SELECT *
FROM renamed