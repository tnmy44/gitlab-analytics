{% snapshot bizible_touchpoint_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp',
          updated_at='_modified_date,
        )
    }}
    
    SELECT * 
    FROM {{ source('bizible', 'biz_touchpoints') }}
    
{% endsnapshot %}
