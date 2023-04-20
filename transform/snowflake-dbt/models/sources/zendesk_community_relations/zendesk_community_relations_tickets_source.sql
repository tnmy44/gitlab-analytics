WITH source AS (

    SELECT *
    FROM {{ source('zendesk_community_relations', 'tickets') }}
),

flattened AS (

    SELECT
      id                                      AS ticket_id,
      created_at                              AS ticket_created_at,
      --ids
      organization_id,
      assignee_id,
      brand_id,
      group_id,
      requester_id,
      submitter_id,

      --fields
      status                                  AS ticket_status,
      lower(priority)                         AS ticket_priority,
      md5(subject)                            AS ticket_subject,
      md5(recipient)                          AS ticket_recipient,
      url                                     AS ticket_url,
      tags                                    AS ticket_tags,

      flat_via.value::varchar                 AS submission_channel,

      --dates
      updated_at::DATE                        AS date_updated

    FROM source
        INNER JOIN LATERAL FLATTEN(INPUT => parse_json(via), OUTER => false, RECURSIVE => false) flat_via
    WHERE submission_channel in ('api', 'web', 'any_channel', 'twitter', 'facebook', 'email')

)

SELECT *
FROM flattened
