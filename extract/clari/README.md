Please see [handbook](https://about.gitlab.com/handbook/business-technology/data-team/platform/pipelines/#clari) for details on this extraction.

Below are instructions to set-up the proper Snowflake environment.

### One-time Setup of Database Environment
#### Create Stage Command
```sql
use raw.clari;

CREATE STAGE clari_load
FILE_FORMAT = (TYPE = 'JSON');
```

#### Create Table Command
Execute following command for creating new table in RAW database
```sql
CREATE OR REPLACE TABLE raw.clari.net_arr (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);
```

