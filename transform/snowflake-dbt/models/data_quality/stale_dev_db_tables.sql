{{ simple_cte([
    ('snowflake_databases', 'snowflake_databases_source'),
    ('snowflake_tables', 'snowflake_tables_source'),
    ('snowflake_users', 'snowflake_show_users_source'),
    ('dim_team_member', 'dim_team_member'),
]) }}

, dev_databases as (
  
  select
    database_id, 
    database_name,
    database_owner,
  from snowflake_databases
  where database_owner not in (
    'TRANSFORMER',
    'ACCOUNTADMIN',
    'SYSADMIN',
    'LOADER',
    'STATIC_ADMIN'
  )
  and database_name like '%' || snowflake_databases.database_owner || '_%' 

), stale_dev_db_tables as (

  select 
    snowflake_tables.table_name,
    snowflake_tables.table_schema,
    snowflake_tables.table_catalog,
    snowflake_tables.table_type,
    snowflake_tables.created,
    snowflake_tables.last_altered,
    snowflake_tables.table_owner,
    dev_databases.database_owner,
  from snowflake_tables
  inner join dev_databases on snowflake_tables.table_catalog_id = dev_databases.database_id
  where table_schema !='INFORMATION_SCHEMA'
    and snowflake_tables.table_type = 'BASE TABLE'
    and snowflake_tables.last_altered <= dateadd(d, {{ var('dev_db_object_expiration') }}, current_timestamp())
  order by dev_databases.database_owner

), snowflake_dev_users as (

  select 
    user_name,
    email,
  from snowflake_users 
  where is_disabled = false
    and user_name in (
      select distinct
        database_owner
      from stale_dev_db_tables
    )

), gitlab_team_members as (

  select
    work_email,
    gitlab_username,
  from prod.common.dim_team_member
  where is_current = true
  and is_current_team_member = true  

), final as (

select
  stale_dev_db_tables.table_name,
  stale_dev_db_tables.table_schema,
  stale_dev_db_tables.table_catalog,
  stale_dev_db_tables.created,
  stale_dev_db_tables.last_altered,
  stale_dev_db_tables.database_owner,
  gitlab_team_members.gitlab_username,
from stale_dev_db_tables
left join snowflake_dev_users on lower(stale_dev_db_tables.database_owner) = lower(snowflake_dev_users.user_name)
left join gitlab_team_members on lower(gitlab_team_members.work_email) = lower(snowflake_dev_users.email)
  
)


select *
from final