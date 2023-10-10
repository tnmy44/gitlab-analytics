{% docs mart_behavior_structured_event_code_suggestion %}

**Description:** Enriched Snowplow table for the analysis of Code Suggestions-related structured events. This model is limited to events carrying the `code_suggestions_context`, in addition to other filters (listed below). It enhances `fct_behavior_structured_event` and includes fields from the `code_suggestions_context` and `ide_extension_version` contexts.

**Data Grain:** behavior_structured_event_pk

This ID is generated using `event_id` from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) 

**Filters Applied to Model:**
- Include events containing the `code_suggestions_context`
- Include events from the following app_ids: `gitlab_ai_gateway`, `gitlab_ide_extension`
- Exclude events from VS Code extension version 3.76.0
- `Inherited` - This model only includes Structured events (when `event=struct` from `dim_behavior_event`)

**Tips for use:**
- There is a cluster key on `behavior_at::DATE`. Using `behavior_at` in a WHERE clause or INNER JOIN will improve query performance.
- All events will carry the `code_suggestions_context`, but only a subset will contain the `ide_extension_version` context

**Other Comments:**
- Schema for `code_suggestions_context` [here](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/code_suggestions_context)
- Schema for `ide_extension_version` context [here](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/ide_extension_version)
- Structured events are custom events implemented with five parameters: event_category, event_action, event_label, event_property and event_value. Snowplow documentation on [types of events](https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/).
- There is information on some Snowplow structured events in the [Snowplow event dictionary](https://metrics.gitlab.com/snowplow)

{% enddocs %}
