WITH source AS (
  SELECT
    e.behavior_date  AS behavior_date,
    e.event_action   AS event_action,
    e.event_label    AS feature,
    e.event_category AS model,
    e.event_property AS request_id,
    e.event_value    AS token_amount,
    e.contexts
  FROM
    {{ ref ('mart_behavior_structured_event') }} AS e
  WHERE
    (
      e.event_action = 'tokens_per_user_request_prompt'
      OR
      e.event_action = 'tokens_per_user_request_response'
    )
    AND
    e.behavior_date > '2024-01-01'
    AND
    e.app_id = 'gitlab')

    SELECT date_trunc('day', behavior_date),
        CASE WHEN model LIKE '%Anthropic%' OR LOWER(model) LIKE '%aigateway%' THEN 'Anthropic'
      ELSE 'Google'
    END                                     AS provider,
    model as model,
    feature as feature,
    event_action as prompt_answer,
    sum(token_amount) as tokens_tracked,
    sum(token_amount) * 4 as characters_tracked
    FROM source
    group by 1,2,3,4,5