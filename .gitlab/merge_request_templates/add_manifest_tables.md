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

#### Check downstream impact

Determine if there is downstream impact. I.e. additional changes to downstream models or the need for a manual full refresh in dbt. 

- [ ] Downstream impact has been determined and;
   - [ ] changes to the manifest do not have downstream impact.
   - [ ] changes to the manifest have downstream impact and
      - [ ] is handled in this MR.
      - [ ] new MR and or issue is opened.
