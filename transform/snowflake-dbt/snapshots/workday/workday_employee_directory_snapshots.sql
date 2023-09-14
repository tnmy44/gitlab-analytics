{% snapshot workday_employee_directory_snapshots %}

    {{
        config(
          unique_key='employee_id',
          strategy='timestamp',
          updated_at='_fivetran_synced',
          invalidate_hard_deletes=True
        )
    }}
    
    SELECT * 
    FROM {{ source('workday','directory') }}
    
{% endsnapshot %}