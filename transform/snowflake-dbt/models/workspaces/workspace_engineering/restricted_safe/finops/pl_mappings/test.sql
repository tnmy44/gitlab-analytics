

WITH 

    combined_pl_mapping as (

SELECT * FROM {{ ref('combined_pl_mapping') }}

)




SELECT *,
    case when gcp_project_id is null then 1 else 0 end +
    case when gcp_service_description is null then 1 else 0 end +
    case when gcp_sku_description is null then 1 else 0 end +
    case when infra_label is null then 1 else 0 end
     AS priority_rank
    FROM combined_pl_mapping