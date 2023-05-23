-- GCP project id to pl mapping

SELECT * FROM {{ ref ('gcp_billing_single_sku_pl_mapping') }}
UNPIVOT(allocation FOR type IN (free, internal, paid))
