{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

With date_details AS (

    SELECT distinct(DATE_TRUNC('month',date_actual))    AS month_date,
            'join'                                      AS join_field
    FROM {{ref('date_details')}} 
    WHERE date_actual <= {{max_date_in_bamboo_analyses()}}
    
), overall_headcount_pivoted AS (
   
    SELECT 
        DATE_TRUNC('month',month_date)                  AS month_date,  
        'total'                                         AS diversity_field,   
        'total'                                         AS aggregation_type,  
        {{ dbt_utils.pivot(
            'metric',
            dbt_utils.get_column_values(ref('bamboohr_headcount_aggregation_intermediate'), 'metric'),
            then_value ='total_count',
            quote_identifiers = False
        ) }} 
    FROM {{ ref('bamboohr_headcount_aggregation_intermediate') }}
    GROUP BY 1,2,3

), gender_headcount_pivoted AS (

    SELECT 
        DATE_TRUNC('month',month_date)                  AS month_date,  
        gender,    
        'gender_breakdown'                              AS aggregation_type,                                     
        {{ dbt_utils.pivot(
            'metric',
            dbt_utils.get_column_values(ref('bamboohr_headcount_aggregation_intermediate'), 'metric'),
            then_value ='total_count',
            quote_identifiers = False
        ) }} 
    FROM {{ ref('bamboohr_headcount_aggregation_intermediate') }}
    GROUP BY 1,2,3

), aggregated AS (

    SELECT
        overall_headcount_pivoted.*,
        (headcount_start + headcount_end)/2                                 AS average_headcount
    FROM overall_headcount_pivoted

    UNION ALL

    SELECT
        gender_headcount_pivoted.*,
        (headcount_start + headcount_end)/2                                 AS average_headcount
    FROM gender_headcount_pivoted

), diversity_fields AS (

    SELECT 
        DISTINCT diversity_field, 
        aggregation_type,
        'join'                                                              AS join_field
    FROM aggregated

), base AS (

    SELECT 
        date_details.*,
        diversity_field,
        aggregation_type
    FROM date_details
    LEFT JOIN diversity_fields
      ON date_details.join_field = diversity_fields.join_field

), intermediate AS(

    SELECT 
      base.month_date,
      base.diversity_field,
      base.aggregation_type,
      aggregated.headcount_start,
      aggregated.headcount_end,  
      aggregated.hires,
      AVG(aggregated.average_headcount) 
        OVER (PARTITION BY base.diversity_field, base.aggregation_type
        ORDER BY base.month_date DESC
        ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING)                              AS rolling_12_month_headcount,                            
      SUM(total_separated) 
        OVER (PARTITION BY base.diversity_field, base.aggregation_type 
        ORDER BY base.month_date DESC  
        ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING )                             AS rolling_12_month_separations,
      SUM(voluntary_separations) 
        OVER (PARTITION BY base.diversity_field, base.aggregation_type 
        ORDER BY base.month_date DESC  
        ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING )                             AS rolling_12_month_voluntary_separations,
      SUM(involuntary_separations) 
        OVER (PARTITION BY base.diversity_field, base.aggregation_type 
        ORDER BY base.month_date DESC  
        ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING )                             AS rolling_12_month_involuntary_separations
FROM base
LEFT JOIN aggregated 
  ON base.month_date = aggregated.month_date
  AND base.diversity_field = aggregated.diversity_field
  AND base.aggregation_type = aggregated.aggregation_type

), final AS (

    SELECT 
      intermediate.*,
      1 - (rolling_12_month_separations/rolling_12_month_headcount)             AS retention
    FROM intermediate

)

SELECT * 
FROM final 
