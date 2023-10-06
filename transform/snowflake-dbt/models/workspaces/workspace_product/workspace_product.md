{% docs wk_rpt_event_namespace_plan_monthly %}

**Description:**

This model captures the last plan available from mart_event_valid for a namespace during a 
given calendar month. This is the same logic used to attribute the namespace's usage for PI 
reporting (ex: [`rpt_event_xmau_metric_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_event_xmau_metric_monthly)).

**Data Grain:**
* event_calendar_month
* dim_ultimate_parent_namespace_id

**Intended Usage**

This model is intended to be JOINed to the fct_event lineage in order to determine how 
a namespace's usage will be attributed during monthly reporting. It can also be leveraged 
to track how many plans a namespace has during a calendar month.

**Filters & Business Logic in this Model:**

* This model inherits all filters and business logic from [`mart_event_valid`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.mart_event_valid#description).
* The current month is excluded.

**Important Nuance:**

This model looks at the entire calendar month where the "official" PI reporting models only 
look at the last 28 days of the month. Please apply the filter `WHERE has_event_during_reporting_period = TRUE` 
in order to have this model tie out to the other reporting models. 

Example: Namespace xyz has events on July 1-2, 2022, but nothing for the remainder of the month. 
Namespace xyz will have a record in this model where `has_event_during_reporting_period = FALSE` 
because it did not have any events during the last 28 days of the month.  Namespace xyz's usage 
will _not_ count in the other reporting models (ex: [`rpt_event_xmau_metric_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_event_xmau_metric_monthly)), 
so the `has_event_during_reporting_period` flag needs to be used in order for the models to tie out.

{% enddocs %}

{% docs wk_rpt_namespace_onboarding %}

**Description:**

This model is aggregated at the ultimate parent namespaces level and contains a wide variety of namesapace onboarding behaviors and attributes. This model contains one row per ultimate_parent_namespace_id and is designed to meet most of the primary analytics use cases for the Growth team. 

**Data Grain:**
* ultimate_parent_namespace_id
* namespace_created_at

**Intended Usage**

This model is intended to be used as a reporting model for the Growth Section and any other teams at GitLab that are interested in ultimate parent namespace level onboarding behaviors and attributes.

**Filters & Business Logic in this Model:**

* This model filters out internal ultimate parent namespaces, ultimate parent namespaces whose creator is blocked, and is aggregated at the ultimate parent namespace level meaning that sub-groups and projects are not included in this model.


{% enddocs %}

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