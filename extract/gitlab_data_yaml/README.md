# YML Extractor

This pipeline pulls an Airflow-generated `JSON` files into tables in Snowflake within the `RAW.GITLAB_DATA_YAML` schema. It uses [remarshal](https://pypi.org/project/remarshal/) to serialize `YAML` to `JSON`. The pipeline is executed in the `gitlab_data_yaml.py` DAG in the `dags/extract` directory.

## API Access

We are using a `project level token` created in the `compensation calculator` project to access the API. In order to create one if the existing token has expired, you need to login as the `Analytics API` user into GitLab.com and create a new token for the `compensation calculator` project [here](https://gitlab.com/gitlab-com/people-group/peopleops-eng/compensation-calculator/-/settings/access_tokens). 

The token is stored in our vault as part of the `Analytics API GitLab Login`. The environment variables:

1. `GITLAB_API_PRIVATE_TOKEN`
1. `GITLAB_INTERNAL_HANDBOOK_TOKEN`
 
needed by the upload script is stored as part of the `airflow` k8s secret and needs to be updated accordingly.


## New file setup 
Current files are defined in the `specificaiton.yml` file. If you want to add new section to the file and include it into the pipeline, here is the setup example:

```yml
pi_internal_hb_file:                            # [Mandatory] replace it with your section name, can be any string. 
  files:                                        # [Mandatory] This section should stay the same as it is. 
    table_name_for_uploading: file_name         # [Mandatory] can be multiple files, key part is the table name you want to upload the content of the tile, and the value is actually a file name. 
  URL: https://...                              # [Mandatory] the main part of the URL. This item is mandatory.
  streaming: True                               # [Optional] True: will stream files, False: will do a batch processing (cUrl + upload to Snowflake).  
  private_token: GITLAB_INTERNAL_HANDBOOK_TOKEN # [Optional] Either you skip this part, or put the token you want to use. 
```

If you introduce a new file, you should create a table in the `RAW.GITLAB_DATA_YAML` schema:

```sql
CREATE OR REPLACE TABLE NEW_TABLE_NAME ( -- replace NEW_TABLE_NAME with real table  name
	JSONTEXT VARIANT,
	UPLOADED_AT TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
);
```

