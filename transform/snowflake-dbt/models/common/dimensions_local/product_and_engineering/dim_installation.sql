{{ simple_cte([('prep_host', 'prep_host'),
('prep_ping_instance', 'prep_ping_instance'),
('prep_installation_creation', 'prep_installation_creation')])}},

joined AS (

  SELECT DISTINCT
    -- Primary Key
    prep_ping_instance.dim_installation_id,

    -- Natural Keys 
    prep_ping_instance.dim_instance_id,
    prep_ping_instance.dim_host_id,

    -- Dimensional contexts  
    prep_host.host_name,
    prep_installation_creation.installation_creation_date
  FROM prep_ping_instance
  INNER JOIN prep_host ON prep_ping_instance.dim_host_id = prep_host.dim_host_id
  LEFT JOIN prep_installation_creation ON prep_installation_creation.dim_installation_id = prep_ping_instance.dim_installation_id

  UNION ALL

  SELECT
    MD5('-1')                 AS dim_installation_id,
    'Missing dim_instance_id' AS dim_instance_id,
    NULL                      AS dim_host_id,
    'Missing host_name'       AS host_name,
    NULL                      AS installation_creation_date
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mdrussell",
    created_date="2021-05-20",
    updated_date="2023-03-31"
) }}
