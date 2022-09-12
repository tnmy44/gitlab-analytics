WITH source AS (

    SELECT {{ nohash_sensitive_columns('marketo_activity_email_bounced_soft_source', 'contact_id') }}
    FROM {{ ref('marketo_activity_email_bounced_soft_source') }}

)

SELECT *
FROM source