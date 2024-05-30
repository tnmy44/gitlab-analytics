{% docs dedupe_aws_source %}

## Deduplication Macro for AWS Sources

**Macro Name:** `dedupe_aws_source`

**Purpose:**

Streamlines the process of deduplicating data from AWS sources while seamlessly integrating with dbt's incremental model functionality.  It's optimized for AWS data that includes a `metadata$file_last_modified` column, which is often used to track data freshness.

**Parameters:**

* `source_table` (**required**): The name of your AWS source table.
* `source_schema` (optional, default: 'gitlab_dotcom'): The schema where the source table resides.
* `condition_column` (optional, default: 'metadata$file_last_modified'): The column used to determine the most recent record (usually a timestamp).
* `unique_key` (optional, default: 'id'): A comma-separated string specifying the columns that uniquely identify a record (e.g., "value['field1']::VARCHAR, value['field2']::VARCHAR").

**Usage:**

{% raw %}
```jinja2
{{ dedupe_aws_source(source_table='your_aws_table', source_schema='aws_schema', unique_key="value['field1']::VARCHAR, value['field2']::VARCHAR") }}
```
{% endraw %}

Will compile to:

{% raw %}
```
SELECT *
FROM (
  SELECT *,
      metadata$file_last_modified AS modified_at_,
      ROW_NUMBER() OVER (
          PARTITION BY value['field1']::VARCHAR, value['field2']::VARCHAR
          ORDER BY metadata$file_last_modified DESC
      ) AS rn
  FROM "aws_schema"."your_aws_table"
  WHERE metadata$file_last_modified > (
      SELECT MAX(modified_at) 
      FROM "your_target_schema"."my_aws_model"  -- Reference to your model
  )
) subquery
WHERE rn = 1
```
{% endraw %}

**How it Works:**

* Configuration: Sets the model to incremental and defines the unique_key for efficient dbt updates.
* Full Refresh/Initial Run: Selects all data, ranks rows by unique_key and condition_column, keeping only the latest (rank 1) per group.
* Incremental Run: Fetches the max condition_column from the target table, then selects only newer source rows and deduplicates them as above.
{% enddocs %}

{% docs dedupe_and_union_aws_source %}

## Deduplicating and Unioning Multiple AWS Sources

**Macro Name:** `dedupe_and_union_aws_source`

**Purpose:**

Simplifies the process of deduplicating multiple AWS source tables and combining them into a single result set. This macro is optimized for AWS data that includes a `metadata$file_last_modified` column and allows for efficient incremental updates.

**Parameters:**

* `source_tables` (**required**): A list of the names of your AWS source tables (e.g., `['table1', 'table2', 'table3']`).
* `schema` (optional, default: 'aws_billing'): The schema where your source tables are located.
* `condition_column` (optional, default: 'metadata$file_last_modified'): The column used to determine the most recent record (usually a timestamp).
* `unique_key` (optional, default: 'id'): A comma-separated string specifying the columns that uniquely identify a record (e.g., "value['field1']::VARCHAR, value['field2']::VARCHAR").

**Usage:**
{% raw %}
```jinja2
{% set source_tables = ['dedicated_legacy_0475', 'dedicated_dev_3675', 'gitlab_marketplace_5127', 'itorg_3027', 'legacy_gitlab_0347', 'services_org_6953'] %}
{% set unique_key = "value['bill_payer_account_id']::VARCHAR, value['bill_invoice_id']::VARCHAR, value['identity_line_item_id']::VARCHAR, value['identity_time_interval']::VARCHAR" %}


{{ dedupe_and_union_aws_source('dedicated_legacy_0475', 'aws_billing', 'metadata$file_last_modified', unique_key) }}
```

{% endraw %}
{% enddocs %}