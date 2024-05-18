{{ union_tables_with_filters(
    relations=[
        ref('mart_crm_opportunity_7th_day_weekly_snapshot'), 
        ref('mart_crm_opportunity_7th_day_weekly_snapshot_aggregate'), 
        ref('mart_targets_actuals_7th_day_weekly_snapshot')
    ],
    filters={
        'mart_crm_opportunity_7th_day_weekly_snapshot': "is_current_snapshot_quarter = true",
        'mart_crm_opportunity_7th_day_weekly_snapshot_aggregate': "is_current_snapshot_quarter = false"
    }
) }}