{{
  config(
    tags=["mnpi"]
  )
}}

SELECT
  {{
      dbt_utils.star(
        from=ref('adaptive_attributes_source'),
        except=[]
        )
  }}
FROM {{ ref('adaptive_attributes_source') }}
