{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

WITH final AS (

    SELECT
      *
    FROM {{ ref('prep_latest_seat_link_installation') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2024-03-07",
    updated_date="2024-03-07"
) }}
