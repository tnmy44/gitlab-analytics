# Quarterly Data Health and Security Audit

Quarterly audit is performed to validate security like right people with right access in environments (Example: Sisense, Snowflake.etc) and data feeds that are running are healthy (Example: Salesforce, GitLab.com..etc).

Please see the [handbook page](https://about.gitlab.com/handbook/business-technology/data-team/data-management/#quarterly-data-health-and-security-audit) for more information. 

Below checklist of activities would be run once for quarter to validate security and system health. Recommend to convert all steps to seperate tasks. 

## SNOWFLAKE
1. [ ] Validate terminated employees have been removed from Snowflake access. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/snowflake.md#validate-terminated-employees-have-been-removed-from-snowflake-access)
2. [ ] De-activate any account that has not logged-in within the past 60 days from the moment of performing audit from Snowflake. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/snowflake.md#de-activate-any-account-that-has-not-logged-in-within-the-past-60-days-from-the-moment-of-performing-audit-from-snowflake)
3. [ ] Validate all user accounts do not have password set. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/snowflake.md#validate-all-user-accounts-do-not-have-password-set)
4. [ ] Drop orphaned tables. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/snowflake.md#drop-orphaned-tables)

## DBT Execution
1. [ ] Generate report on top 25 long running dbt models. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/dbt.md)
 
## AIRFLOW
1. [ ] Validate off-boarded employees have been removed from Airflow access. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/airflow.md#validate-off-boarded-employees-have-been-removed-from-airflow-access)
1. [ ] Clean up log files [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/airflow.md#clean-up-old-log-files)

## Monte Carlo
1. [ ] Validate off-boarded employees have been removed from Monte Carlo access. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/montecarlo.md#validate-off-boarded-employees-have-been-removed-from-monte-carlo-access)
1. [ ] Deprovision access if an account has not logged-in within the past 90 days from the moment of performing audit. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/montecarlo.md#deprovision-access-if-an-account-has-not-logged-in-within-the-past-90-days-from-the-moment-of-performing-audit)

## Tableau
1. [ ] Validate offboarded employess have been removed from Tableau Cloud and purge from systems:
    
    1. [ ] Remove offboarded users from [Google Groups: okta-tableau-users](https://groups.google.com/a/gitlab.com/g/okta-tableau-users/members)
    1. [ ] Remove offboarded (Unlicensed) users full entry from [Tableau's spec.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/tableau_con_man_config/src/specification.yaml)

1. [ ] Downgrade users - Report: [User Deprovision Check](https://10az.online.tableau.com/#/site/gitlab/workbooks/2447768/views)
    1. [ ] Creators - Downgrade users who haven't published a data source or used Tableau Desktop in the last 90 days.
    1. [ ] Explorers - Downgrade users who haven't published a workbook or used Tableau web authoring the last 90 days.
    1. [ ] Viewers - Deprovision access if a user has had access for >=90 days, but have not logged in during the past 90 days from the moment of performing audit. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/tableau.md#deprovision-access-if-a-user-has-had-access-for-90-days-but-have-not-logged-in-during-the-past-90-days-from-the-moment-of-performing-audit)
1. [ ] Role Credentials for Snowflake Service Account: TABLEAU
    1. [ ] Collect new credentials from Platform and have the records updated in 1Pass for `Snowflake_tableau`.
    1. [ ] Batch update all data sources using the credentials.

## Package version inventory

1. [ ] Python and other tools/libraries inventory. [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/quarterly_data_health_and_security_audit/package_inventory.md#package-version-inventory)


<!-- DO NOT EDIT BELOW THIS LINE -->
/label ~"Team::Data Platform" ~Snowflake ~TDF ~"Data Team" ~"Priority::1-Ops" ~"workflow::4 - scheduled" ~"Quarterly Data Health and Security Audit" ~"Periscope / Sisense"
/confidential 
