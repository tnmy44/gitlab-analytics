with source as
(select * FROM {{ source('level_up', 'course_views') }} ),

  intermediate AS (
    SELECT
      d.value,
      source.uploaded_at
    FROM
      source,
      LATERAL FLATTEN(input => source.jsontext['data']) AS d
  ),

parsed as (
select
value['companyId']::varchar as company_id,
value['courseId']::varchar as course_id,
value['courseSku']::varchar as course_sku,
value['courseTitle']::varchar as course_title,
value['lessonId']::varchar as lesson_id,
value['lessonSlug']::varchar as lesson_slug,
value['lessonTitle']::varchar as lesson_title,
value['sectionId']::varchar as section_id,
value['sectionSlug']::varchar as section_slug,
value['sectionTitle']::varchar as section_title,
value['timestamp']::varchar as timestamp,
value['topicId']::varchar as topic_id,
value['topicTitle']::varchar as topic_title,
  value['userDetail']['id']::varchar as user_id,
  value['userDetail']['ref1']::varchar as user_category,
  value['userDetail']['ref2']::varchar as user_job,
  value['userDetail']['ref3']::varchar as ref3,
  value['userDetail']['ref4']::varchar as user_company,
  value['userDetail']['ref6']::varchar as user_role_type,
  value['userDetail']['ref7']::varchar as user_continent,
  value['userDetail']['ref8']::varchar as user_country,
  value['userDetail']['ref9']::varchar as user_sub_dept,
  value['userDetail']['ref10']::varchar as user_dept,
  value['userDetail']['sfAccountId']::varchar as sf_account_id,
  value['userDetail']['sfContactId']::varchar as sf_contact_id,
  uploaded_at
from intermediate

-- remove dups in case 'raw' is reloaded
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
    user_id,
    timestamp,
    topic_title
    ORDER BY
      uploaded_at DESC
  ) = 1
)
select * from parsed

