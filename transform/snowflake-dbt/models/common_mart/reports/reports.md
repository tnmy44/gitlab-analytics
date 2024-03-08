{% docs rpt_behavior_code_suggestion_outcome %}

**Description:** Reporting model to enable Code Suggestion analysis and reporting at the grain of one record per suggestion. This model uses Snowplow events and can be used to calculate metrics like Acceptance Rate. Read more about how the Code Suggestions events work [here](https://gitlab.com/gitlab-org/editor-extensions/gitlab-lsp/-/blob/main/docs/telemetry.md).

**Data Grain:** suggestion_id

This is an alias of `event_label` from the Snowplow data

**Filters Applied to Model:**

- Include events from the app_id `gitlab_ide_extension`
- Exclude events without an `event_label` (aka `suggestion_id`)
- Exclude suggestions that do not have a `suggestion_requested` event
- Exclude suggestions that have more than one event for a given `event_action`
- Exclude `suggestion_rejected` events if the suggestion also has a `suggestion_accepted` event (see "Other Comments" below)
- `Inherited` - Include events containing the `code_suggestions_context`
- `Inherited` - Exclude IDE events from VS Code extension version 3.76.0. These are excluded by using both `ide_name` and `extension_version` values.

**Intended Usage**

This model is intended to enable reporting and analysis on the "outcome" of a suggestion. It 
can be used to calculate Acceptance Rate, Load Time, etc.

**Other Comments:**

- A suggestion cannot be both accepted and rejected, but it can have both `suggestion_accepted` 
and `suggestion_rejected` events. The explanation is in [this issue comment](https://gitlab.com/gitlab-data/product-analytics/-/issues/1410#note_1581747408)
- A visual representation of the different Snowplow events associated with the single suggestion 
can be found [here](https://gitlab.com/gitlab-org/editor-extensions/gitlab-lsp/-/blob/main/docs/telemetry.md)

{% enddocs %}
