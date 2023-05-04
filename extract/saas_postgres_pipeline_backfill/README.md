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

The files are saved in [saas-pipeline-backfills](https://console.cloud.google.com/storage/browser/saas-pipeline-backfills;tab=objects?forceOnBucketsSortingFiltering=true&project=gitlab-analysis&prefix=&forceOnObjectsSortingFiltering=false) bucket, specifically in  `staging/` folder.

It is the responsibility of the consuming service to move the files to the `processed/` folder once the files are processed.

Note: If the files are not moved to `processed/`, the backfill mechanism will continually trigger because the 'processed' folder is used as the source of truth to determine schema changes.

### Half finished state of code
As of 2023-05-04, there are sections of code, i.e within the DAG that are commented out, and it seems a bit messy. 

The reason for this is that this backfill code may be integrated in one of two ways:
1. Current pgp pipeline
2. New Snowplow pipeline

If integrated with current pgp pipeline, we don't want to delete existing code, as we will need to re-integrate it. 
As such, we are in a 'limbo' state.


## Detailed flow - Backfill
This [LucidChart](https://gitlab.com/gitlab-data/analytics/uploads/1007c5d0ab1d30e57ff1b4a780d8a940/image.png) shows the flow visually.

When a backfill is started, the following conditions are checked:

1. Check if in the middle of the backfill
    - If last file written was less than 24 hrs ago, resume from last written id
    - Else if more than 24 hr, backfill from beginning
1. Check if new table, if so, backfill from beginning
1. Check if schema changed. If so, backfill from beginning
1. Else, don't backfill 
    

#### Mid-backfill

During the backfill, for every n chunks, the metadata is written to a postgres table including the following metadata:
1. `table_name`
1. `initial_load_start_date`: save all the files associated to the load in the same folder based off the initial_load_start_date
1. `upload_date`: the upload date of the file
1. `last_extracted_id`: this is where the backfill will start from if mid-backfill
1. `is_export_completed`: Determines if the table is in the middle of a backfill

To determine if in the middle of a backfill, simply take the `is_export_completed` flag of the most recent record associated with that table.

#### New table
To determine if the table is a 'new' table, check the metadata table to see if the new table exists.

#### schema change
A schema change is determined by checking the `processed/` folder's latest parquet file and schema. The last processed schema is then compared against what is read-in from postgres (via manifest file) to see if there were any schema **additions**- schema subtractions are ignored.

## Detailed Flow - Deletes
This [LucidChart](https://gitlab.com/gitlab-data/analytics/uploads/92cfe3652da293a9ca776ba98047a826/image.png) shows the delete flow.

It is a simpler process:
1. If in the middle of a delete export, start from last extracted id
1. Else start the delete process from the beginning

The delete process only exports the PK or composite key of the table.
