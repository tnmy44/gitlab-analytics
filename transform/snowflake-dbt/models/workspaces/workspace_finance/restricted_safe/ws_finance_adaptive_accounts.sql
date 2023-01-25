{{ config(
tags=["mnpi"]
)
}}

SELECT
  {{
      dbt_utils.star(
        from=ref('adaptive_accounts_source'),
        except=[]
        )
  }}
FROM {{ ref('adaptive_accounts_source') }}
