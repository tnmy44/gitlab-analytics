{% set source_tables = ['dedicated_legacy_0475',
    'dedicated_dev_3675',
    'gitlab_marketplace_5127',
    'itorg_3027',
    'legacy_gitlab_0347',
    'services_org_6953'] %}

{{ union_aws_source(source_tables, 'aws_billing' )}}
