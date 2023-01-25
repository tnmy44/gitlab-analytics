
{{ config(
tags=["mnpi"]
)
}}

SELECT
  {{
      dbt_utils.star(
        from=ref('adaptive_users_source'),
        except=[]
        )
  }}
FROM {{ ref('adaptive_users_source') }}
