
{{
  config(
    tags=["mnpi"]
  )
}}

SELECT
  {{
      dbt_utils.star(
        from=ref('adaptive_instances_source'),
        except=[]
        )
  }}
FROM {{ ref('adaptive_instances_source') }}
