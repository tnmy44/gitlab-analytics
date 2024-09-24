# Data classification

Support the following data:
* `MNPI` data - identified using `MNPI` tag from [GitLab dbt](https://dbt.gitlabdata.com/) project
* `PII` data - identify using [SYSTEM$CLASSIFY](https://docs.snowflake.com/en/sql-reference/stored-procedures/system_classify) Snowflake (built in) procedure 

The data classification process will be triggered manually, once per quarter to tag data and check which queries are potential security threats from the perspective of accessing `PII` or `MNPI` data.
