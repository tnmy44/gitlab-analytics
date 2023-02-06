
{{
  config(
    tags=["mnpi"]
  )
}}

SELECT
  {{
      dbt_utils.star(
        from=ref('adaptive_dimension_families_source'),
        except=[]
        )
  }}
FROM {{ ref('adaptive_dimension_families_source') }}
