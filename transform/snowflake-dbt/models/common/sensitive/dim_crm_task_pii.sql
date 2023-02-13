WITH prep_crm_task_pii AS (

  SELECT *
  FROM {{ ref('sfdc_task_pii') }}

), final AS (


  SELECT

    -- Surrogate key
    {{ dbt_utils.surrogate_key(['prep_crm_task_pii.task_id']) }}             AS dim_crm_task_sk,

    -- Natural key
    prep_crm_task_pii.task_id                                               AS dim_crm_task_id,
    prep_crm_task_pii.task_id_hash,

    -- Task infomation
    prep_crm_task_pii.full_comments,
    prep_crm_task_pii.task_subject

    FROM prep_crm_task_pii


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-12-13",
    updated_date="2023-02-13"
) }}
