{{ datasiren.search_sample_columns_samples_for_pattern(this.identifier, env_var('SNOWFLAKE_PROD_DATABASE'), 'TEXT', '10', '11', '^\w+@{1}[a-zA-Z_]+?\.{1}[a-zA-Z]{2,3}$', 'common') }}
