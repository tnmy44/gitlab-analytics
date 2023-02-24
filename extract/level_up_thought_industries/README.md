More information on this extraction is in the [handbook](https://gitlab.com/gitlab-com/www-gitlab-com/-/blob/91a63ee99016d986904dc73bb6d29aa1bb67c8b8/sites/handbook/source/handbook/business-technology/data-team/platform/pipelines/index.html.md#level-up-thought-industries-extract)

#TODO: update the above link once the 'Level Up' section is finalized in the 'master' branch.

### One-time Setup of Database Environment
```sql
use role loader;
use database <raw_db>;

-- set-up stage
create schema level_up;
use schema level_up;

CREATE STAGE level_up_load_stage
FILE_FORMAT = (TYPE = 'JSON');


-- create tables
CREATE OR REPLACE TABLE course_completions (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);


CREATE OR REPLACE TABLE logins (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE visits (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE TABLE course_views (
  jsontext variant,
  uploaded_at timestamp_ntz(9) default CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);
```
