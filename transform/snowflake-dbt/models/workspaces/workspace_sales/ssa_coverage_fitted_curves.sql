{{ config(materialized='view') }}


WITH base AS (

    SELECT *
    FROM raw.sales_analytics.xray_curves_qtd_fitted
    --FROM {{ ref('driveload_ssa_coverage_fitted_curves_source') }}

)

SELECT *
FROM base
