WITH source AS (

	SELECT *
	FROM {{ ref('ga360_session_source') }}

), custom_dimensions AS (

	SELECT visit_id, visitor_id, visit_start_time, dimension_value, dimension_name
	FROM {{ ref('ga360_session_custom_dimension_xf') }}
	
), joined AS (

    SELECT 
        source.visit_id, 
        source.visitor_id, 
        source.visit_start_time,
        OBJECT_AGG(dims.dimension_name, dims.dimension_value::VARIANT)
            OVER (PARTITION BY source.visit_id, source.visitor_id, source.visit_start_time) AS custom_dimensions
    FROM source
    LEFT JOIN custom_dimensions dims
    ON dims.visit_id = source.visit_id 
            AND dims.visitor_id = source.visitor_id 
                AND dims.visit_start_time = source.visit_start_time
    GROUP BY source.visit_id, source.visitor_id, source.visit_start_time, dims.dimension_name, dims.dimension_value

), final AS (

    SELECT  
        source.*,
        joined.custom_dimensions
    FROM source
    LEFT JOIN joined
    ON joined.visit_id = source.visit_id 
            AND joined.visitor_id = source.visitor_id 
                AND joined.visit_start_time = source.visit_start_time
    {{ dbt_utils.group_by(n=31)}}

)

SELECT *
FROM final
