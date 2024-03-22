WITH greenhouse_candidates_source AS (
  SELECT *
  FROM {{ ref('greenhouse_candidates_source') }}
),
greenhouse_candidate_custom_fields_source AS (
  SELECT *
  FROM {{ ref('greenhouse_candidate_custom_fields_source') }}
),
candidates_raw AS (
  SELECT *
  FROM {{ source('greenhouse', 'candidates') }}
),
candidate
AS (
	SELECT c1.CANDIDATE_ID
		,c1.CANDIDATE_RECRUITER_ID
		,c1.CANDIDATE_COORDINATOR_ID
		,c1.CANDIDATE_RECRUITER
		,c2.recruiter_id
		,c2.coordinator_id
		,c1.CANDIDATE_COORDINATOR
		,c1.CANDIDATE_COMPANY
		,c1.CANDIDATE_TITLE
		,c1.CANDIDATE_CREATED_AT
		,c1.CANDIDATE_UPDATED_AT
		,c1.IS_CANDIDATE_MIGRATED
		,c1.IS_CANDIDATE_PRIVATE
		,c2.First_name
		,c2.Last_name
	FROM greenhouse_candidates_source c1
	LEFT JOIN candidates_raw c2 ON c1.candidate_id = c2.id
	)
	,candidate_custom
AS (
	SELECT CANDIDATE_ID
		,max(CASE CANDIDATE_CUSTOM_FIELD
				WHEN 'Country'
					THEN CANDIDATE_CUSTOM_FIELD_DISPLAY_VALUE
				ELSE NULL
				END) AS Country
		,max(CASE CANDIDATE_CUSTOM_FIELD
				WHEN 'Locality'
					THEN CANDIDATE_CUSTOM_FIELD_DISPLAY_VALUE
				ELSE NULL
				END) AS Locality
		,max(CASE CANDIDATE_CUSTOM_FIELD
				WHEN 'Relationship'
					THEN CANDIDATE_CUSTOM_FIELD_DISPLAY_VALUE
				ELSE NULL
				END) AS Relationship
		,max(CASE CANDIDATE_CUSTOM_FIELD
				WHEN 'Family Relationship'
					THEN CANDIDATE_CUSTOM_FIELD_DISPLAY_VALUE
				ELSE NULL
				END) AS Family_Relationship
		,max(CASE CANDIDATE_CUSTOM_FIELD
				WHEN 'Referral'
					THEN CANDIDATE_CUSTOM_FIELD_DISPLAY_VALUE
				ELSE NULL
				END) AS referral
		,max(CASE CANDIDATE_CUSTOM_FIELD
				WHEN 'Preferred First Name/Nickname'
					THEN CANDIDATE_CUSTOM_FIELD_DISPLAY_VALUE
				ELSE NULL
				END) AS preferred_first
		,max(CASE CANDIDATE_CUSTOM_FIELD
				WHEN 'Referral Notes'
					THEN CANDIDATE_CUSTOM_FIELD_DISPLAY_VALUE
				ELSE NULL
				END) AS referral_notes
	FROM greenhouse_candidate_custom_fields_source
	GROUP BY 1
	)
    
    select candidate.*
    ,cust.country
    ,cust.locality
    ,cust.relationship
    ,cust.family_relationship
    ,cust.referral
    ,cust.preferred_first
    ,cust.referral_notes
    from candidate
    left join candidate_custom cust on candidate.candidate_id = cust.candidate_id

