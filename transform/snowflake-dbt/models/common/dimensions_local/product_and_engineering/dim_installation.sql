{{ simple_cte([('prep_host', 'prep_host'),
('prep_ping_instance', 'prep_ping_instance')])}}

, joined AS (

    SELECT
    prep_ping_instance.dim_installation_id, 
    prep_ping_instance.dim_instance_id,
    prep_ping_instance.dim_host_id,    
    prep_host.host_name
    FROM prep_ping_instance
    INNER JOIN prep_host ON prep_ping_instance.dim_host_id = prep_host.dim_host_id
    {{ dbt_utils.group_by(n=4) }}
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@tpoole",
    created_date="2021-05-20",
    updated_date="2023-01-20"
) }}


