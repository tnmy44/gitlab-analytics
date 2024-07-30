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
o.is_direct_connection,
o.suggestion_id,
NULL AS behavior_structured_event_pk,
o.suggestion_source,
o.model_engine,
o.model_name,
o.extension_name,
o.ide_name,
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
_datetime::DATE AS date_day,
u.crm_account_name,
u.gitlab_global_user_id,
u.app_id,
u.language,
u.delivery_type,
u.is_direct_connection,

COUNT(1) AS occs,
COUNT(DISTINCT u.behavior_structured_event_pk) AS event_count,
COALESCE(COUNT(DISTINCT u.suggestion_id),0) AS unique_suggestions,
COALESCE(SUM(u.was_accepted::INT),0) AS was_accepted,
COALESCE(SUM(u.was_cancelled::INT),0) AS was_cancelled,
COALESCE(SUM(u.was_error::INT),0) AS was_error,
COALESCE(SUM(u.was_loaded::INT),0) AS was_loaded,
COALESCE(SUM(u.was_not_provided::INT),0) AS was_not_provided,
COALESCE(SUM(u.was_rejected::INT),0) AS was_rejected,
COALESCE(SUM(u.was_requested::INT),0) AS was_requested,
COALESCE(SUM(u.was_shown::INT),0) AS was_shown,
COALESCE(SUM(u.was_stream_completed::INT),0) AS was_stream_completed,
COALESCE(SUM(u.was_stream_started::INT),0) AS was_stream_started

FROM
unify u
GROUP BY ALL
ORDER BY 1 DESC
