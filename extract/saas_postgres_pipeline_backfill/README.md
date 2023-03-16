### Purpose
This is a copy of `extract/postgres_pipeline`, but with some backfill logic being adjusted.

Backfill will be triggered when:
1. New table
1. Schema addition to **source only**
1. In the middle of a backfill (but only if last upload was within 24 HR)

Changes:
1. Saved in GCS as parquet, does not upload to Snowflake
1. Use backfill metadata database to preserve backfill status
1. Chunked in 5 million records to match [Snowflake General File Sizing Recommendations](https://docs.snowflake.com/en/user-guide/data-load-considerations-prepare#general-file-sizing-recommendations)

### Note: Half finished state
There are sections of code, i.e within the DAG that are commented out, and it seems a bit messy. 

The reason for this is that this backfill code may be integrated in one of two ways:
1. Current pgp pipeline
2. New Snowplow pipeline

If integrated with current pgp pipeline, we don't want to delete existing code, as we will need to re-integrate it. 
As such, we are in a 'limbo' state.
