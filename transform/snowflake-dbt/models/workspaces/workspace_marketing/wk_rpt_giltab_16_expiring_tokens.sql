/*



 */

 {{ config(
    tags=["mnpi_exception"],
    materialized='table'
) }}

{{ simple_cte([
    ('mart_ping_instance','mart_ping_instance'),
    ('mart_arr', 'mart_arr'),
    ('sfdc_contact_source', 'sfdc_contact_source')
]) }}

, latest_ping_sent AS (
    SELECT DISTINCT
    dim_installation_id,
    host_name,
    is_paid_subscription,
    major_minor_version_id
        AS latest_major_minor_version,
    latest_subscription_id,
    crm_account_name,
    parent_crm_account_name,
    dim_billing_account_id,
    dim_parent_crm_account_id,
    ping_deployment_type,
    ping_product_tier,
    ping_edition,
    ping_created_at::DATE
        AS date_of_latest_ping_sent
    FROM mart_ping_instance
    WHERE major_minor_version_id != 10000
    QUALIFY RANK() OVER (PARTITION BY dim_installation_id ORDER BY ping_created_at DESC) = 1 --latest ping record
), all_installations_on_16 AS (
-- all results from previous CTE, filtering to installations with latest ping sent on 16.0 and max(latest_major_minor_version) to ensure results are at installation grain
    SELECT
    dim_installation_id,
    host_name,
    is_paid_subscription,
    ping_product_tier,
    ping_edition,
    MAX(latest_major_minor_version)
        AS latest_major_minor_version, -- there is one installation ddea7372abd38377ba5bebe8f2e1a88e that sent two major minor version values in the same ping - both over 1600
    latest_subscription_id,
    crm_account_name,
    parent_crm_account_name,
    dim_billing_account_id,
    dim_parent_crm_account_id,
    ping_deployment_type,
    date_of_latest_ping_sent
    FROM latest_ping_sent
    WHERE latest_major_minor_version >= 1600
    GROUP BY ALL
), days_since_first_ping_on_16 AS (
--Not all installations send a ping on every major minor version. It is possible to jump versions or to simply not send a ping while they are on a certain version.
--This methodology allows us to see the first ping sent on a major minor version at 16.0 or above even if we don't have a ping for all installations that are on 16.0 or higher
    SELECT DISTINCT
    dim_installation_id,
    major_minor_version_id,
    ping_created_at::DATE
        AS date_of_first_ping_sent_on_1600_or_higher,
    DATEDIFF(day, date_of_first_ping_sent_on_1600_or_higher, current_date)
        AS days_since_first_ping_sent_on_1600_or_higher,
    DATEDIFF(week, date_of_first_ping_sent_on_1600_or_higher, current_date)
        AS weeks_since_first_ping_sent_on_1600_or_higher,
    DATEDIFF(month, date_of_first_ping_sent_on_1600_or_higher, current_date)
        AS months_since_first_ping_sent_on_1600_or_higher
    FROM mart_ping_instance
    WHERE major_minor_version_id >= 1600
    QUALIFY RANK() OVER (PARTITION BY dim_installation_id ORDER BY ping_created_at ASC) = 1 --first ping record for installations on 1600 or higher
), final AS (
    SELECT DISTINCT
    all_installations_on_16.*,
    days_since_first_ping_on_16.major_minor_version_id AS first_major_minor_version_sent_on_1600_or_higher,
    days_since_first_ping_on_16.date_of_first_ping_sent_on_1600_or_higher,
    days_since_first_ping_on_16.days_since_first_ping_sent_on_1600_or_higher,
    days_since_first_ping_on_16.weeks_since_first_ping_sent_on_1600_or_higher,
    days_since_first_ping_on_16.months_since_first_ping_sent_on_1600_or_higher
    FROM all_installations_on_16
    INNER JOIN days_since_first_ping_on_16
    ON days_since_first_ping_on_16.dim_installation_id = days_since_first_ping_on_16.dim_installation_id
    WHERE 
    all_installations_on_16.ping_deployment_type != 'GitLab.com' -- Our organization manages version upgrades for the Gitlab.com deployment
    and days_since_first_ping_sent_on_1600_or_higher BETWEEN 335 AND 365

),staging AS (
    SELECT DISTINCT 
        final.*, 
        mart_arr.DIM_CRM_ACCOUNT_ID
    FROM final
    LEFT JOIN mart_arr
        ON mart_arr.DIM_SUBSCRIPTION_ID = final.LATEST_SUBSCRIPTION_ID
        and ARR_MONTH = date_trunc('month', current_date)
    WHERE PING_PRODUCT_TIER <> 'Free'
        AND IS_PAID_SUBSCRIPTION
)
SELECT DISTINCT
    staging.DIM_CRM_ACCOUNT_ID,
    -- DIM_CRM_ACCOUNT.CRM_ACCOUNT_NAME,
    inactive_contact,
    sfdc_contact_source.contact_first_name,
    sfdc_contact_source.contact_last_name,
    sfdc_contact_source.contact_email,
    sfdc_contact_source.contact_role,
    sfdc_contact_source.mobile_phone
FROM staging
LEFT join sfdc_contact_source
        on sfdc_contact_source.account_id = staging.DIM_CRM_ACCOUNT_ID 
        AND contains(contact_role,'Gitlab Admin')
left join raw.driveload.MARKETING_DNC_LIST 
        on lower(sfdc_contact_source.contact_email) = lower(MARKETING_DNC_LIST.address)
where lower(MARKETING_DNC_LIST.address) is null
and contact_email is not null
ORDER BY 2