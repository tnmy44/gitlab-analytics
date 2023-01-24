WITH source AS (
	SELECT *
	FROM {{ ref('ga360_session_source') }}

), custom_dimensions AS (

	SELECT *
	FROM {{ ref('ga360_session_custom_dimension_xf') }}
	
), joined AS (

	SELECT 
		source.*,
		OBJECT_AGG(dims.dimension_name, dims.dimension_value::VARIANT)
    		OVER (PARTITION BY source.visit_id, source.visitor_id, source.visit_start_time) AS custom_dimensions
	FROM source
	LEFT JOIN custom_dimensions AS dims
	ON dims.visit_id = source.visit_id 
		AND dims.visitor_id = source.visitor_id 
		    AND dims.visit_start_time = source.visit_start_time
	GROUP BY source.visit_id, source.visitor_id, source.visit_start_time,dims.dimension_name, dims.dimension_value
	
)



SELECT *
FROM joined
