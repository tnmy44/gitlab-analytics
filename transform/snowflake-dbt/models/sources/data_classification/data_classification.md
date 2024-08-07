{% docs sensitive_queries_tags %}

Set of `MNPI` and `PII` tags we want to track.

{% enddocs %}

{% docs sensitive_queries_source %}

Source for sensitive queries for:
* `PII`
* `MNPI`

data. Combining tags with views:

1. [QUERY_HISTORY](https://docs.snowflake.com/en/sql-reference/functions/query_history) 
1. [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history)

to get the data can be treated as not allowed or suspicious action. 

More details about the technical implementation can be found in the [**/analytics**](https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/data_classification/README.md) repo.

{% enddocs %}

{% docs sensitive_queries_details %}

Details about the tagging.

{% enddocs %}