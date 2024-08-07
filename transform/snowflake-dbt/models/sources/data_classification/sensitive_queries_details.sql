{{ config({
    "materialized": "incremental",
    "tags": ["data_classification", "mnpi_exception"]
    })
}}
SELECT 1