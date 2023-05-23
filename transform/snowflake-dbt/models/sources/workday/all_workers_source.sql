WITH source AS (

  SELECT *
  FROM {{ source('workday','all_workers') }}

),

renamed AS (

  SELECT
    employee_id::NUMBER               AS employee_id,
    nationality::VARCHAR              AS nationality,
    ethnicity::VARCHAR                AS ethnicity,
    preferred_last_name::VARCHAR      AS preferred_last_name,
    preferred_first_name::VARCHAR     AS preferred_first_name,
    gender::VARCHAR                   AS gender,
    work_email::VARCHAR               AS work_email,
    date_of_birth::DATE               AS date_of_birth,
    _fivetran_deleted::BOOLEAN        AS is_deleted,
    _fivetran_synced::TIMESTAMP       AS uploaded_at
  FROM source

)

SELECT *
FROM renamed
