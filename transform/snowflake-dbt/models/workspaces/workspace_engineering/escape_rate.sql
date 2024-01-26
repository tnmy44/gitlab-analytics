WITH bugs AS
  (SELECT DISTINCT date_trunc('month',date_actual) AS MONTH,
                   issue_id,
                   stage_label,
                   group_label,
                   section_label
   FROM {{ref('issues_history')}} --using this instead of internal_issues_enhanced because of severity labels are added later
   WHERE is_part_of_product
     AND type_label='bug'
     AND severity!='No Severity'
     AND issue_is_moved=FALSE
     AND MONTH=date_trunc('month', issue_created_at)),

     product_mrs AS
  (SELECT date_trunc('month',merged_at) AS MONTH,
          merge_request_id,
          stage_label,
          group_label,
          section_label
   FROM {{ref('engineering_merge_requests')}}
   WHERE merged_at IS NOT NULL
     AND MERGED_AT BETWEEN DATEADD('month', -24, CURRENT_DATE) AND CURRENT_DATE
     AND is_created_by_bot=FALSE),

     bug_regroup AS
  (SELECT MONTH,
          'group' AS breakout,
          group_label,
          '' AS stage_label,
          '' AS section_label,
          count(issue_id) AS issues
   FROM bugs
   {{ dbt_utils.group_by(n=5) }}
   UNION ALL SELECT MONTH,
                    'stage' AS breakout,
                    '' AS group_label,
                    stage_label,
                    '' AS section_label,
                    count(issue_id) AS issues
   FROM bugs
   {{ dbt_utils.group_by(n=5) }}
   UNION ALL SELECT MONTH,
                    'section' AS breakout,
                    '' AS group_label,
                    '' AS stage_label,
                    section_label,
                    count(issue_id) AS issues
   FROM bugs
   {{ dbt_utils.group_by(n=5) }}
   ),

     mr_regroup AS
  (SELECT MONTH,
          'group' AS breakout,
          group_label,
          '' AS stage_label,
          '' AS section_label,
          count(merge_request_id) AS mr
   FROM product_mrs
   {{ dbt_utils.group_by(n=5) }}
   UNION ALL SELECT MONTH,
                    'stage' AS breakout,
                    '' AS group_label,
                    stage_label,
                    '' AS section_label,
                    count(merge_request_id) AS mr
   FROM product_mrs
   {{ dbt_utils.group_by(n=5) }}
   UNION ALL SELECT MONTH,
                    'section' AS breakout,
                    '' AS group_label,
                    '' AS stage_label,
                    section_label,
                    count(merge_request_id) AS mr
   FROM product_mrs
   {{ dbt_utils.group_by(n=5) }}
   )
   
SELECT coalesce(a.MONTH,b.MONTH) AS MONTH,
       coalesce(a.breakout,b.breakout) AS breakout,
       coalesce(a.group_label,b.group_label) AS group_label,
       coalesce(a.stage_label,b.stage_label) AS stage_label,
       coalesce(a.section_label,b.section_label) AS section_label,
       coalesce(issues,0) as issues,
       coalesce(mr,0) as mr
FROM bug_regroup AS a
FULL OUTER JOIN mr_regroup AS b ON a.MONTH = b.MONTH
AND a.breakout = b.breakout
AND a.group_label = b.group_label
AND a.stage_label = b.stage_label
AND a.section_label = b.section_label
UNION ALL
SELECT a.MONTH,
         'all' AS breakout,
         '' AS group_label,
         '' AS stage_label,
         '' AS section_label,
         count(DISTINCT issue_id) AS issues,
         count(DISTINCT merge_request_id) AS mr
FROM bugs AS a
JOIN product_mrs AS b ON a.MONTH = b.MONTH
GROUP BY 1