{{ config(
    materialized = 'table',
    tags = ["mnpi_exception", "product"]
) }} 

WITH unify AS (
SELECT
REPLACE(f.value,'"','') AS crm_account_name,
o.gitlab_global_user_id,
o.requested_at AS _datetime,
'gitlab_ide_extension' AS app_id,
o.language,
o.delivery_type,
o.product_deployment_type,
o.is_direct_connection,
o.suggestion_id,
NULL AS behavior_structured_event_pk,
o.suggestion_source,
o.model_engine,
o.model_name,
o.extension_name,
o.extension_version,
o.ide_name,
o.ide_version,
o.has_advanced_context,
o.is_streaming,
o.is_invoked,
o.options_count,
o.display_time_in_ms,
o.load_time_in_ms,
o.suggestion_outcome,
o.was_accepted,
o.was_cancelled,
o.was_error,
o.was_loaded,
o.was_not_provided,
o.was_rejected,
o.was_requested,
o.was_shown,
o.was_stream_completed,
o.was_stream_started
FROM
 {{ ref('rpt_behavior_code_suggestion_outcome') }} o, LATERAL FLATTEN(input => crm_account_names::ARRAY ) AS f
WHERE
o.namespace_is_internal != TRUE
AND
o.suggestion_outcome != 'suggestion_cancelled'
AND
f.value IS NOT NULL

UNION ALL 
SELECT
REPLACE(f.value,'"','') AS crm_account_name,
r.gitlab_global_user_id,
r.behavior_at,
r.app_id,
r.language,
r.delivery_type,
r.is_direct_connection,
NULL,
r.behavior_structured_event_pk,
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL, 
NULL
FROM 
{{ ref('rpt_behavior_code_suggestion_gateway_request') }} r, LATERAL FLATTEN(input => crm_account_names::ARRAY ) AS f
WHERE
r.namespace_is_internal != TRUE
AND 
f.value IS NOT NULL
)

SELECT
*
FROM
unify u
WHERE
u._datetime > '2024-02-15'