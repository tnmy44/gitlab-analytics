## Request for Data Import in Snowflake/Tableau Checklist

<!--
Please complete all items. Ask questions in the #data slack channel
--->

**Original Issue Link**:
<!--
If none, please include a description
--->

**Editor Slack Handle**: @`handle`

**Business Use Case** (Please explain what this data will be used for):


### DRI/Prioritization Owner Checklist
* [ ]  Provide link to CSV/GSheet data. Link: ____
* [ ]  Does this data live in the Data team's data warehouse? (https://dbt.gitlabdata.com)
  - [ ] Yes
  - [ ] No
* [ ]  Does this data need to be linked to other data in the Data team's data warehouse?
  - [ ] Yes
  - [ ] No
* [ ] Does this data contain anything that is sensitive (Classified as Red or Orange in our [Data Classification Policy](https://about.gitlab.com/handbook/engineering/security/data-classification-standard.html#data-classification-levels))?
  - [ ] Yes
  - [ ] No
  - [ ] I don't know
* [ ]  How long will this data need to reside in the Data team's data warehouse? Expiration Date: ______
* [ ]  How do you want to name the table? Table Name: ______
* [ ]  Update the due date on this issue for when you want this data in Tableau. (Note: The airflow job for sheetload runs every night and is immediately followed by a sheetload-specific dbt run)
* [ ]  Provide who will be the owner of the file to fix potential data issues. @____

### If you need data in Tableau but...

- [ ] Do **NOT** need to link it to other data
    * [ ]  Open CSV directly in Tableau -  Submitter needs to be a Tableau Creator. If the submitter is not a Tableau Creator, please reach out to #data-tableau Slack Channel for support. As a Tableau Creator, Submitter please follow steps in Tableau Cloud or Desktop:
        
       - Tableau Cloud
          1. In Tableau Cloud, navigate to the "Explore" page.
          1. Click on the "New" button and select "Workbook" to start a new project.
          1. In the "Connect to Data" section, click "More" under the "Files" category and select the CSV file.
          1. Click "Open" to load the file into Tableau Cloud.
          1. Click on "Sheet 1" at the bottom of the screen to begin working on your Tableau project.

      - Tableau Desktop
          1. In Tableau Desktop select Connect to Data from the start page
          1. In the To a File section, click More and select the CSV file
          1. Click Open to load the file into Tableau
          1. Click Sheet 1 at the bottom of the screen to start working on your Tableau project
    * [ ]  Submitter to close issue if this data will not be used again.

---

- [ ] **DO** need to link it to other data AND this is a one-off analysis (Boneyard)
    * [ ] Submitter to put spreadsheet data into a new file into the [Sheetload > Boneyard GDrive Folder](https://drive.google.com/open?id=1NdA5CDy2kT653qUdqtCiq_RkmRa-LKqs).
    * [ ] Submitter to share it with the required service account - [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)
    * [ ] Submitter to make an MR to [this file](https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/sheetload/boneyard/sheets.yml) adding the new sheet in alphabetical order. Sheet and tab names are *case-sensitive*.
    * [ ]  Submitter to run the following CI Jobs on the MR:
         - [ ] `clone_prod_real`
         - [ ] `boneyard_sheetload` (wait until `clone_prod_real` has finished)
    * [ ] Submitter to assign MR to member of the Data Team
    * [ ] Data Team member to check file name and sheet names to match: The file will be located and loaded based on its name `boneyard.<table_name>`. The names of the sheets shared with the runner must be unique and in the `<file_name>.<tab_name>` format.
    * [ ] Data Team member to merge update after validation of data and MR
    * [ ] Submitter to wait 6 to 8 hours for data to become available (runs 4x per day), or if urgent to ask Data Engineer to trigger job
    * [ ] Submitter to create a new data source in Tableau using the table: `boneyard.[new-dbt-model-name]`. The submitter needs to be a Tableau Creator. If the submitter is not a Tableau Creator, please reach out to #data-tableau Slack Channel for support.

---

- [ ] **DO** need to link it to other data AND want this to be repeatable (Analytics)
    * [ ]  Data Team member to put spreadsheet data into a new file into [Sheetload > Sheetload GDrive Folder](https://drive.google.com/drive/folders/1F5jKClNEsQstngbrh3UYVzoHAqPTf-l0).
    * [ ]  Data Team member to share it with the required service account - [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal)
    * [ ]  Data Team member to check file name and sheet names to match: The file will be located and loaded based on its name `sheetload.<table_name>`. The names of the sheets shared with the runner must be unique and in the `<file_name>.<tab_name>` format
    * [ ]  Data Team member to create MR to add this sheet to be pulled in by Sheetload that combines the steps taken in the following MR examples:
      - [ ] [Edit the sheets.yml file](https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/sheetload/sheets.yml)
      - [ ] [Edit the schema.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/sources/sheetload/schema.yml)
      - [ ] [Edit the sources.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/sources/sheetload/sources.yml)
      - [ ] Add a new base model under [sources-->sheetload repo](https://gitlab.com/gitlab-data/analytics/-/tree/master/transform/snowflake-dbt/models/sources/sheetload)
      - **If the the data is required in Tableau**
         - [ ] Add a new staging model under [staging-->sheetload repo](https://gitlab.com/gitlab-data/analytics/-/tree/master/transform/snowflake-dbt/models/staging/sheetload)
         - [ ] [Edit the schema.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/staging/sheetload/schema.yml)
    * [ ]  Data Team member to run the following CI Jobs on the MR:
         - [ ] `clone_raw`
         - [ ] `sheetload`
         - [ ] `specify_raw_model` (here you need to specify which dbt-models you want to run/test, just as if it was specify_model)
    * [ ]  Data Analyst to assign MR to project maintainer for review (iterate until model is complete).
    * [ ]  Data Team project maintainers/owners to merge in dbt models
    * [ ]  If not urgent, data will be availble within 24 hours. If urgent, Data Engineer to run full refresh and inform when available.
    * [ ]  Submitter to create a new data source in Tableau using the table: `boneyard.[new-dbt-model-name]`. The submitter needs to be a Tableau Creator. If the submitter is not a Tableau Creator, please reach out to #data-tableau Slack Channel for support.

