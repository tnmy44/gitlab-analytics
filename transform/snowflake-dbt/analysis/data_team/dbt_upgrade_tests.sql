{% if execute and target.name != 'prod'%}

  {%- set test_db = "16576-UPDATE-DBT-1-4_" -%}
  {%- set test_prod = test_db + "PROD" -%}
  {%- set test_prep = test_db + "PREP" -%}

  {#
Example
  samples:
    - name: date_details_source
      clause: "{{ sample_table(3) }}"
    - name: dim_date
      clause: "WHERE date_actual >= DATEADD('day', -30, CURRENT_DATE())"

#}

  {% set tests_yml -%}


target_prefix: "16576-UPDATE-DBT-1-4_"

tests:
  - name: compare_dim_date
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_date
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - current_date_actual

  - name: compare_mart_product_usage_paid_user_metrics_monthly
    test_type: compare_relations
    source_db: prod
    source_schema: common_mart_product
    source_name: mart_product_usage_paid_user_metrics_monthly
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_fct_event
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: fct_event
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_mart_crm_opportunity_stamped_hierarchy_hist
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_sales
    source_name: mart_crm_opportunity_stamped_hierarchy_hist
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - days_since_last_activity

  - name: compare_mart_arr
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_sales
    source_name: mart_arr
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_employee_directory_analysis
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: employee_directory_analysis
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_mart_charge
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_sales
    source_name: mart_charge
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_mart_retention_parent_account
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_sales
    source_name: mart_retention_parent_account
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_fct_ping_instance_metric_monthly
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: fct_ping_instance_metric_monthly
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_mart_available_to_renew
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common_mart_finance
    source_name: mart_available_to_renew
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_rpt_event_xmau_metric_monthly
    test_type: compare_relations
    source_db: prod
    source_schema: common_mart_product
    source_name: rpt_event_xmau_metric_monthly
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_dim_ping_instance
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_ping_instance
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - IP_ADDRESS_HASH
      - IS_LAST_PING_OF_MONTH
      - IS_LAST_PING_OF_WEEK

  - name: compare_mart_ping_instance_metric
    test_type: compare_relations
    source_db: prod
    source_schema: common_mart
    source_name: mart_ping_instance_metric
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - IS_LAST_PING_OF_MONTH
      - IS_LAST_PING_OF_WEEK

  - name: compare_dim_namespace
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_namespace
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_dim_user
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_user
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_customers_db_charges_xf
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_legacy
    source_name: customers_db_charges_xf
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_customers_db_trial_histories
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: customers_db_trial_histories
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_gitlab_dotcom_members
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: gitlab_dotcom_members
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_snowplow_unnested_events_all
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: snowplow_unnested_events_all
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_dim_crm_account
    test_type: compare_relations
    source_db: prod
    source_schema: restricted_safe_common
    source_name: dim_crm_account
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at
      - LAST_MODIFIED_DATE
      - LAST_MODIFIED_BY_NAME

  - name: compare_dim_crm_user
    test_type: compare_relations
    source_db: prod
    source_schema: common
    source_name: dim_crm_user
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at

  - name: compare_gitlab_dotcom_projects_xf
    test_type: compare_relations
    source_db: prod
    source_schema: legacy
    source_name: gitlab_dotcom_projects_xf
    exclude_columns:
      - dbt_updated_at
      - dbt_created_at


  {%- endset %}

  {%- set tests_dict = fromyaml(tests_yml) -%}

  {%- set tests = tests_dict.tests | list -%}

  {%- if tests | length < 1 %}
    {% do exceptions.raise_compiler_error('There are no tests configured.')%}
  {% endif -%}



  {%- for test in tests -%}

    {% set table_db = test.source_db | upper %}
    {% set target_db = tests_dict.target_prefix + table_db | upper %}
    {% set table_schema = test.source_schema | upper %}
    {% set table_name = test.source_name | upper %}
    {%- set old_etl_relation=adapter.get_relation(table_db, table_schema, table_name ) -%}
    {%- set new_etl_relation=adapter.get_relation(target_db, table_schema, table_name ) -%}

{{"-- "}}{{ old_etl_relation }}
{{ audit_helper.compare_relations(
    old_etl_relation,
    new_etl_relation,
    exclude_columns= test.exclude_columns
) | trim | replace(' 
    
      , 
     
   
    ', ',')  }}
;
    
  {%- endfor -%}



  

{# 
  -- PROD.common.dim_date
  {% set table_db = 'PROD' %}
  {% set table_schema = 'common' %}
  {% set table_name = 'dim_date' %}
  {%- set old_etl_relation=adapter.get_relation(
        table_db,
        table_schema,
        table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}

  {{ audit_helper.compare_relations(
      old_etl_relation,
      new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at","current_date_actual"]
  ) }}
  where in_a and in_b
  ;

  -- PROD.common_mart_product.mart_product_usage_paid_user_metrics_monthly
  {% set table_db = "PROD" %}
  {% set table_schema = "common_mart_product" %}
  {% set table_name = "mart_product_usage_paid_user_metrics_monthly" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
     
  -- PROD.common.fct_event
  {% set table_db = "PROD" %}
  {% set table_schema = "common" %}
  {% set table_name = "fct_event" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;


 {#  
  -- PROD.restricted_safe_common_mart_sales.mart_crm_opportunity_stamped_hierarchy_hist
  {% set table_db = "PROD" %}
  {% set table_schema = "restricted_safe_common_mart_sales" %}
  {% set table_name = "mart_crm_opportunity_stamped_hierarchy_hist" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["days_since_last_activity","dbt_updated_at","dbt_created_at"]
  ) }}
  ;
  #}
  {#  
  -- PROD.restricted_safe_common_mart_sales.mart_arr
  {% set table_db = "PROD" %}
  {% set table_schema = "restricted_safe_common_mart_sales" %}
  {% set table_name = "mart_arr" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
#}
        
  -- PROD.legacy.employee_directory_analysis
  {% set table_db = "PROD" %}
  {% set table_schema = "legacy" %}
  {% set table_name = "employee_directory_analysis" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
   {#   
  -- PROD.restricted_safe_common_mart_sales.mart_charge
  {% set table_db = "PROD" %}
  {% set table_schema = "restricted_safe_common_mart_sales" %}
  {% set table_name = "mart_charge" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
  #}
    {#
  -- PROD.restricted_safe_common_mart_sales.mart_retention_parent_account
  {% set table_db = "PROD" %}
  {% set table_schema = "restricted_safe_common_mart_sales" %}
  {% set table_name = "mart_retention_parent_account" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
#}
    
  -- PROD.common.fct_ping_instance_metric_monthly
  {% set table_db = "PROD" %}
  {% set table_schema = "common" %}
  {% set table_name = "fct_ping_instance_metric_monthly" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
    {#
  -- PROD.restricted_safe_common_mart_finance.mart_available_to_renew
  {% set table_db = "PROD" %}
  {% set table_schema = "restricted_safe_common_mart_finance" %}
  {% set table_name = "mart_available_to_renew" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
    #}
  -- PROD.common_mart_product.rpt_event_xmau_metric_monthly
  {% set table_db = "PROD" %}
  {% set table_schema = "common_mart_product" %}
  {% set table_name = "rpt_event_xmau_metric_monthly" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
    
  -- PROD.common.dim_ping_instance
  {% set table_db = "PROD" %}
  {% set table_schema = "common" %}
  {% set table_name = "dim_ping_instance" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at","IP_ADDRESS_HASH", "IS_LAST_PING_OF_MONTH", "IS_LAST_PING_OF_WEEK"]
  ) }}
  ;
    
  -- PROD.common_mart.mart_ping_instance_metric
  {% set table_db = "PROD" %}
  {% set table_schema = "common_mart" %}
  {% set table_name = "mart_ping_instance_metric" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at","IS_LAST_PING_OF_MONTH", "IS_LAST_PING_OF_WEEK"]
  ) }}
  ;
    
  -- PROD.common.dim_namespace
  {% set table_db = "PROD" %}
  {% set table_schema = "common" %}
  {% set table_name = "dim_namespace" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
    
  -- PROD.common.dim_user
  {% set table_db = "PROD" %}
  {% set table_schema = "common" %}
  {% set table_name = "dim_user" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
    {#
  -- PROD.restricted_safe_legacy.customers_db_charges_xf
  {% set table_db = "PROD" %}
  {% set table_schema = "restricted_safe_legacy" %}
  {% set table_name = "customers_db_charges_xf" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
#}
  -- PROD.legacy.customers_db_trial_histories
  {% set table_db = "PROD" %}
  {% set table_schema = "legacy" %}
  {% set table_name = "customers_db_trial_histories" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
    
  -- PROD.legacy.gitlab_dotcom_members
  {% set table_db = "PROD" %}
  {% set table_schema = "legacy" %}
  {% set table_name = "gitlab_dotcom_members" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
    
  -- PROD.legacy.snowplow_unnested_events_all
  {% set table_db = "PROD" %}
  {% set table_schema = "legacy" %}
  {% set table_name = "snowplow_unnested_events_all" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;
    {#
  -- PROD.restricted_safe_common.dim_crm_account
  {% set table_db = "PROD" %}
  {% set table_schema = "restricted_safe_common" %}
  {% set table_name = "dim_crm_account" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at","LAST_MODIFIED_DATE", "LAST_MODIFIED_BY_NAME"]
  ) }}
  ;
    #}
  -- PROD.common.dim_crm_user
  {% set table_db = "PROD" %}
  {% set table_schema = "common" %}
  {% set table_name = "dim_crm_user" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
  ;

    
  -- PROD.legacy.gitlab_dotcom_projects_xf
  {% set table_db = "PROD" %}
  {% set table_schema = "legacy" %}
  {% set table_name = "gitlab_dotcom_projects_xf" %}
  {%- set old_etl_relation=adapter.get_relation(
        database=table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {%- set new_etl_relation=adapter.get_relation(
        database=test_db + table_db,
        schema=table_schema,
        identifier=table_name
  ) -%}
  {{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=new_etl_relation,
      exclude_columns=["dbt_updated_at","dbt_created_at"]
  ) }}
;

#}
{% endif %}