WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'project_ci_cd_settings') }}

)

{{ scd_latest_state(source='base', max_column='_task_instance') }}