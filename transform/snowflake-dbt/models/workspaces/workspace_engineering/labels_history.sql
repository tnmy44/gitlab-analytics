WITH labels AS (

    SELECT *
    FROM {{ ref('prep_labels') }} 

), label_links AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_label_links_source') }} 

), label_type AS (

    SELECT
      dim_label_id,
      label_title,
      CASE
        WHEN LOWER(label_title) IN ('severity::1', 'severity::2', 'severity::3', 'severity::4') THEN 'severity'
        WHEN LOWER(label_title) LIKE 'team%' THEN 'team'
        WHEN LOWER(label_title) LIKE 'group%' THEN 'team'
        ELSE 'other'
      END AS label_type
    FROM labels

),  base_labels AS (

    SELECT
      label_links.target_id                                                                                           AS dim_issue_id,
      label_type.label_title,
      label_type.label_type,
      IFF(label_type.label_type='severity','S' || RIGHT(label_title, 1),NULL) AS severity,
      IFF(label_type.label_type='team',SPLIT(label_title, '::')[ARRAY_SIZE(SPLIT(label_title, '::')) - 1]::VARCHAR, NULL) AS assigned_team,
      label_links.label_link_created_at                                                                                   AS label_added_at,
      label_links.label_link_created_at                                                                                   AS label_valid_from,
      LEAD(label_links.label_link_created_at, 1, CURRENT_DATE())
           OVER (PARTITION BY label_links.target_id,label_type.label_type ORDER BY label_links.label_link_created_at) AS label_valid_to
    FROM label_type
    LEFT JOIN label_links
      ON label_type.dim_label_id = label_links.label_id
      AND label_links.target_type = 'Issue'
    WHERE label_type.label_type != 'other'
      AND label_links.target_id IS NOT NULL

)

SELECT *
FROM base_labels