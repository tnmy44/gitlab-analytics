{{config({
    "schema": "legacy"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_ticket_users_source') }}
)

SELECT *
FROM source
