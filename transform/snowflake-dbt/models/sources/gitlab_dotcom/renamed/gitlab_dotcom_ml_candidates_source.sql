WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ml_candidates_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER                                          AS ml_candidate_id,
      created_at::TIMESTAMP                               AS created_at,
      updated_at::TIMESTAMP                               AS updated_at,
      experiment_id::NUMBER                               AS experiment_id,
      user_id::NUMBER                                     AS user_id,
      DATEADD('ms', start_time, '1970-01-01')::TIMESTAMP  AS start_at,
      DATEADD('ms', end_time, '1970-01-01')::TIMESTAMP    AS end_at,
      status::NUMBER                                      AS status
    FROM source

)

SELECT *
FROM renamed
