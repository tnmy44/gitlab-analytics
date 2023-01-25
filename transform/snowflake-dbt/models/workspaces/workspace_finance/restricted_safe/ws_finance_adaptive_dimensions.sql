
{{ config(
tags=["mnpi"]
)
}}

SELECT
{{
      dbt_utils.star(
        from=ref('adaptive_dimensions_source'),
        except=[]
        )
  }}
FROM {{ ref('adaptive_dimensions_source') }}
