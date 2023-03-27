Closes

* List the tables added/changed below
* If tables are non-SCD add them to reconcile process.
* Run the `clone_raw_postgres_pipeline` CI job
* Run the `pgp_test` or `gitlab_ops_pgp_test` CI job by right clicking on the job name and opening in a new tab
  * For `pgp_test`, include the `MANIFEST_NAME` variable and input the name of the changed yml file except postfix `_db_manifest.yaml`.
  * If this is a SCD table be sure to include the next 2 variables:
    * `advanced_metadata` with value `true`
    * `TASK_INSTANCE` variable in job trigger with any value (i.e. `mr-####`)

#### Tables Changed/Added

* [ ] List

* [ ] If tables are non-SCD add them to reconcile process.

#### PGP Test CI job passed?

* [ ] List

#### Red data

* [ ] Confirm no [red data](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/new-data-source/#red-data) is loaded into Snowflake or the data is masked.

#### Check: is the manual full refresh needed

- [ ] **Check required**: If the MR adds/renames columns to a table in the `RAW.TAP_POSTGRES` schema and the following model exists in the `PREP.GITLAB_DOTCOM` schema, a `dbt run --full-refresh` will be needed after merging the `MR`. Please, add it to the Reviewer Checklist to warn them that this step is required.
    - [ ] DBT full refresh required post merge?[
        - [ ] `Yes`. Create an issue with the model name which needs to be refreshed and assign it to the Reviewer to run it post merge. Use the page [**DBT model manual full refresh**](https://about.gitlab.com/handbook/business-technology/data-team/platform/infrastructure/#dbt-model-manual-full-refresh) for the detailed explanation about the full refresh 
        - [ ] `No`.