WITH repository_lang AS (

    SELECT * FROM {{ ref('gitlab_dotcom_repository_languages')}}

),

programming_lang AS (

    SELECT * FROM {{ ref('gitlab_dotcom_programming_languages')}}

),

projects AS (

    SELECT * FROM {{ ref('dim_project')}}

),

namespaces AS (

    SELECT * FROM {{ ref('dim_namespace')}}

),

project_statistics AS (

    SELECT * FROM {{ ref('gitlab_dotcom_project_statistics')}} 

)

    SELECT
      repository_lang.share,
      repository_lang.repository_language_id,
      repository_lang.project_id,
      repository_lang.programming_language_id,
      programming_lang.programming_language_name,
      projects.ultimate_parent_namespace_id,
      project_statistics.repository_size,
      namespaces.gitlab_plan_id,
      namespaces.gitlab_plan_title,
      namespaces.namespace_name,
      namespaces.namespace_is_internal,
      projects.mirror,
      namespaces.dim_namespace_id
    FROM repository_lang
    JOIN programming_lang
      ON repository_lang.programming_language_id = programming_lang.programming_language_id 
    JOIN projects
      ON repository_lang.project_id = projects.dim_project_id 
    JOIN namespaces
      ON projects.ultimate_parent_namespace_id = namespaces.dim_namespace_id 
    JOIN  project_statistics 
      ON projects.dim_project_id = project_statistics.project_id 

