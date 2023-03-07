
{{
  config(
    tags=["mnpi"]
  )
}}

SELECT
  {{
      dbt_utils.star(
        from=ref('adaptive_versions_source'),
        except=[]
        )
  }}
FROM {{ ref('adaptive_versions_source') }}
