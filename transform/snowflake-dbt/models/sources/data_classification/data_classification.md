{% docs sensitive_queries_tags_doc %}

Set of `MNPI` and `PII` tags we want to track.

{% enddocs %}

{% docs sensitive_queries_base_doc %}

VIEW created to help and avoid code duplication.

{% enddocs %}


{% docs sensitive_queries_source_doc %}

Source for sensitive queries for:
* `PII`
* `MNPI`

data. Combining tags with views:

1. [QUERY_HISTORY](https://docs.snowflake.com/en/sql-reference/functions/query_history) 
1. [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history)

to get the data can be treated as not allowed or suspicious action. 

More details about the technical implementation can be found in the [**/analytics**](https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/data_classification/README.md) repo.

{% enddocs %}

{% docs sensitive_queries_details_doc %}

Details about the tagging.

{% enddocs %}

{% docs classification_type_doc %}

This column can have values:


{% enddocs %}


{% docs start_time_doc %}

When query execution was started.

{% enddocs %}

{% docs end_time_doc %}

When query execution was ended.

{% enddocs %}