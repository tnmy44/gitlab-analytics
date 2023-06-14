WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'programs') }}

),

renamed AS (

    SELECT
      code::VARCHAR                                          AS program_code, 
      custom_attributes::VARIANT                             AS custom_attributes,
      description::VARCHAR                                   AS program_description,
      editors::VARIANT                                       AS program_editors,
      id::NUMBER                                             AS program_id, 
      managers::VARIANT                                      AS program_managers,
      mapped['assessments']::VARIANT                         AS mapped_assessments,
      mapped['issues']::VARIANT                              AS mapped_issues,
      mapped['org_groups']::VARIANT                          AS mapped_org_groups,
      mapped['requests']::VARIANT                            AS mapped_requests,
      mapped['risks']::VARIANT                               AS mapped_risks,
      mapped['standards']::VARIANT                           AS mapped_standards,
      mapped['systems']::VARIANT                             AS mapped_systems,
      notes::VARCHAR                                         AS program_notes,
      primary_contact::VARIANT                               AS program_primary_contact,
      readers::VARIANT                                       AS program_readers,
      reference_url::VARCHAR                                 AS program_reference_url,
      secondary_contact::VARIANT                             AS program_secondary_contact,
      status::VARCHAR                                        AS program_status,
      tags::VARCHAR                                          AS program_tags,
      title::VARCHAR                                         AS program_title,
      type::VARCHAR                                          AS program_type,
      url::VARCHAR                                           AS program_url,
      effective_date::DATE                                   AS program_effective_date,
      created_at::TIMESTAMP                                  AS program_created_at,
      updated_at::TIMESTAMP                                  AS program_updated_at,
      stop_date::DATE                                        AS program_stop_date,
      _sdc_extracted_at::TIMESTAMP                           AS program_loaded_at
    FROM source

)

SELECT *
FROM renamed
