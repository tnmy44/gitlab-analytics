-- GCP project id to pl mapping

SELECT * FROM {{ ref ('gcp_billing_sandbox_projects') }}
