WITH source AS (
	SELECT *
	FROM {{ ref('ga360_session_hit_source') }}

), custom_dimensions AS (

	SELECT *
	FROM {{ ref('ga360_session_hit_custom_dimension_xf') }}
	
), joined AS (

	SELECT 
		source.visit_id, 
        source.visitor_id, 
        source.hit_number,
        OBJECT_AGG(dims.dimension_name, dims.dimension_value::VARIANT)
            OVER (PARTITION BY source.visit_id, source.visitor_id, source.hit_number) AS custom_dimensions
	FROM source
	LEFT JOIN custom_dimensions AS dims
	ON dims.visit_id = source.visit_id 
		AND dims.visitor_id = source.visitor_id 
		    AND dims.hit_number = source.hit_number
    GROUP BY source.visit_id, source.visitor_id, source.hit_number, dims.dimension_name, dims.dimension_value

), final AS (

    SELECT  
        source.*,
        joined.custom_dimensions
    FROM source
    LEFT JOIN joined
    ON joined.visit_id = source.visit_id 
            AND joined.visitor_id = source.visitor_id 
                AND joined.hit_number = source.hit_number
    {{ dbt_utils.group_by(n=18)}}

)

SELECT *
FROM final