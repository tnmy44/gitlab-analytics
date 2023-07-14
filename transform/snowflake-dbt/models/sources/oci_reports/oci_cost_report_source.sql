WITH source as (

    SELECT *

      FROM {{ source('oci_reports', 'oci_cost_report') }}

), renamed as (

    SELECT DISTINCT

        lineitem__referenceno::VARCHAR                           AS lineitem__referenceno,
        lineitem__tenantid::VARCHAR                              AS lineitem__tenantid,
        lineitem__intervalusagestart::TIMESTAMP                  AS lineitem__intervalusagestart,
        lineitem__intervalusageend::TIMESTAMP                    AS lineitem__intervalusageend,
        product__service::VARCHAR                                AS product__service,
        product__compartmentid::VARCHAR                          AS product__compartmentid,
        product__compartmentname::VARCHAR                        AS product__compartmentname,
        product__region::VARCHAR                                 AS product__region,
        product__availabilitydomain::VARCHAR                     AS product__availabilitydomain,
        product__resourceid::VARCHAR                             AS product__resourceid,
        usage__billedquantity::FLOAT                             AS usage__billedquantity,
        usage__billedquantityoverage::VARCHAR                    AS usage__billedquantityoverage,
        cost__subscriptionid::NUMBER                             AS cost__subscriptionid,
        cost__productsku::VARCHAR                                AS cost__productsku,
        product__description::VARCHAR                            AS product__description,
        cost__unitprice::FLOAT                                   AS cost__unitprice,
        cost__unitpriceoverage::VARCHAR                          AS cost__unitpriceoverage,
        cost__mycost::FLOAT                                      AS cost__mycost,
        cost__mycostoverage::VARCHAR                             AS cost__mycostoverage,
        cost__currencycode::VARCHAR                              AS cost__currencycode,
        cost__billingunitreadable::VARCHAR                       AS cost__billingunitreadable,
        cost__skuunitdescription::VARCHAR                        AS cost__skuunitdescription,
        cost__overageflag::VARCHAR                               AS cost__overageflag,
        lineitem__iscorrection::VARCHAR                          AS lineitem__iscorrection,
        lineitem__backreferenceno::VARCHAR                       AS lineitem__backreferenceno,
        tags__oracle_tags_createdby::VARCHAR                     AS tags__oracle_tags_createdby,
        tags__oracle_tags_createdon::TIMESTAMP                   AS tags__oracle_tags_createdon,
        tags__cluster_name::VARCHAR                              AS tags__cluster_name,
        tags__oci_compute_instanceconfiguration::VARCHAR         AS tags__oci_compute_instanceconfiguration,
        tags__oci_compute_instancepool::VARCHAR                  AS tags__oci_compute_instancepool,
        tags__oci_compute_instancepool_opcretrytoken::VARCHAR    AS tags__oci_compute_instancepool_opcretrytoken,
        tags__orcl_cloud_free_tier_retained::VARCHAR             AS tags__orcl_cloud_free_tier_retained,
        tags__parent_cluster::VARCHAR                            AS tags__parent_cluster

    FROM source

)

SELECT *
FROM renamed