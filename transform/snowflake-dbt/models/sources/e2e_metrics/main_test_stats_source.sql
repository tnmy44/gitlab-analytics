WITH source AS (

   SELECT *
   FROM {{ source('e2e_metrics','main_test_stats') }}
 
), final AS (

    SELECT
        
       FROM source
)

SELECT *
FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY combined_composite_keys ORDER BY _uploaded_at DESC) = 1
