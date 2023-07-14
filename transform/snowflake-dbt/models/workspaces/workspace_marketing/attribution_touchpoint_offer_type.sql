{{ config(
    tags=["mnpi_exception"],
    materialized="table"
) }}

WITH attribution_touchpoint AS (

  SELECT DISTINCT 
    dim_crm_touchpoint_id,
    bizible_ad_campaign_name,
    bizible_marketing_channel,
    bizible_marketing_channel_path,
    bizible_medium,
    type,
    bizible_touchpoint_type,
    LOWER(bizible_form_url) AS bizible_form_url,
    REPLACE(bizible_form_url,'.html','') AS bizible_form_url_clean,
    pathfactory_content_type
  FROM {{ ref('mart_crm_attribution_touchpoint') }}
  LEFT JOIN {{ ref('sheetload_bizible_to_pathfactory_mapping') }}  ON bizible_form_url_clean=bizible_url
),

base AS (

  SELECT 
  attribution_touchpoint.*,

  CASE   
  WHEN 
  bizible_touchpoint_type = 'Web Chat' OR
  LOWER(bizible_ad_campaign_name) LIKE '%webchat%' 
  THEN 'Web Chat'
  
  WHEN bizible_touchpoint_type = 'Web Form' 
  AND bizible_form_url IN ('gitlab.com/-/trial_registrations/new',
                           'about.gitlab.com/free-trial',
                           'gitlab.com/-/trial_registrations',
                           'gitlab.com/-/trials/new',
                           'about.gitlab.com/free-trial/index.html',
                           'gitlab.com/-/trials/new')
  THEN 'Trial'      

  WHEN  bizible_form_url LIKE '%/sign_up%' 
  THEN 'Sign Up Form'
   
  WHEN bizible_form_url = 'about.gitlab.com/sales' 
  THEN 'Contact Sales Form' 

  WHEN bizible_form_url LIKE '%/resources/%' 
  THEN 'Resources'
  
  WHEN bizible_touchpoint_type = 'Web Form' AND (bizible_form_url LIKE '%page.gitlab.com%') 
  THEN 'Online Content' 
  
  WHEN bizible_form_url LIKE 'learn.gitlab.com%' 
  THEN 'Online Content'
  
  WHEN bizible_marketing_channel = 'Event' 
  THEN TYPE 
  
  WHEN LOWER(bizible_ad_campaign_name) like '%lead%' AND LOWER(bizible_ad_campaign_name) LIKE '%linkedin%' 
  THEN 'Lead Gen Form'
  
  WHEN bizible_marketing_channel = 'IQM' 
  THEN 'IQM'
  
  WHEN bizible_form_url LIKE '%/education/%' OR LOWER(bizible_ad_campaign_name) LIKE '%education%' 
  THEN 'Education'
  
  WHEN bizible_form_url LIKE '%/company/%' 
  THEN 'Company Pages'

  WHEN bizible_touchpoint_type = 'CRM' AND (LOWER(bizible_ad_campaign_name) LIKE '%partner%' OR LOWER(bizible_medium) LIKE '%partner%')
  THEN 'Partner Sourced'  

  WHEN bizible_form_url ='about.gitlab.com/company/contact/' or bizible_form_url LIKE '%/releases/%' or bizible_form_url LIKE '%/blog/%' 
  THEN 'Newsletter/Release/Blog Sign-Up'

  WHEN type = 'Survey' OR bizible_form_url LIKE '%survey%' OR LOWER(bizible_ad_campaign_name) LIKE '%survey%' 
  THEN 'Survey'

  WHEN bizible_form_url LIKE '%/solutions/%' OR bizible_form_url IN ('https://about.gitlab.com/enterprise/','https://about.gitlab.com/small-business/') 
  THEN 'Solutions' 
  
  WHEN bizible_form_url LIKE '%/blog%' 
  THEN 'Blog'
  
  WHEN bizible_form_url LIKE '%/webcast/%' 
  THEN 'Webcast'
  
  WHEN bizible_form_url LIKE '%/services%'  
  THEN 'GitLab Professional Services' 
  
  WHEN bizible_form_url LIKE '%/fifteen%'  
  THEN  'Event Registration' 
  
  
  WHEN bizible_ad_campaign_name LIKE '%PQL%' 
  THEN 'Product Qualified Lead'
  
  WHEN bizible_marketing_channel_path LIKE '%Content Syndication%' 
  THEN 'Content Syndication'

  ELSE 'Other'

  END 
  AS touchpoint_offer_type_wip,
  
  
  CASE 
  WHEN 
  pathfactory_content_type IS NOT null 
  THEN pathfactory_content_type

  WHEN
  touchpoint_offer_type_wip = 'Online Content' 
  AND bizible_form_url LIKE '%registration-page%' or bizible_form_url like '%registrationpage%' or bizible_form_url like '%inperson%' or bizible_form_url like '%in-person%' or bizible_form_url like '%landing-page%'
  THEN 'Event Registration'

  WHEN
  touchpoint_offer_type_wip = 'Online Content' 
  AND bizible_form_url LIKE '%ebook%'
  THEN 'eBook'
    
  WHEN
  touchpoint_offer_type_wip = 'Online Content' 
  AND bizible_form_url LIKE '%webcast%'
  THEN 'Webcast'

  WHEN 
  touchpoint_offer_type_wip = 'Online Content' 
  AND bizible_form_url LIKE '%demo%' 
  THEN 'Demo'

  WHEN touchpoint_offer_type_wip = 'Online Content'
  THEN 'Other Online Content'

  ELSE touchpoint_offer_type_wip 

  END 
  AS touchpoint_offer_type,
  
  CASE
  WHEN bizible_marketing_channel = 'Event' OR touchpoint_offer_type = 'Event Registration' 
  OR touchpoint_offer_type = 'Webcast' OR touchpoint_offer_type = 'Workshop' 
  THEN 'Events'

  WHEN touchpoint_offer_type_wip = 'Online Content' 
  THEN 'Online Content'

  WHEN touchpoint_offer_type IN ('Content Syndication','Newsletter/Release/Blog Sign-Up','Blog','Resources')
  THEN 'Online Content'

  WHEN touchpoint_offer_type = 'Sign Up Form' 
  THEN 'Sign Up Form'

  WHEN touchpoint_offer_type = 'Trial' 
  THEN 'Trials'

  WHEN touchpoint_offer_type = 'Web Chat' 
  THEN 'Web Chat'

  WHEN touchpoint_offer_type = 'Contact Sales Form' 
  THEN 'Contact Sales Form'

  WHEN touchpoint_offer_type = 'Partner Sourced' 
  THEN 'Partner Sourced'

  WHEN touchpoint_offer_type = 'Lead Gen Form' 
  THEN 'Lead Gen Form'

  ELSE 'Other'
  END AS touchpoint_offer_type_grouped

  FROM attribution_touchpoint

), final AS (

  SELECT 
    dim_crm_touchpoint_id,
    touchpoint_offer_type,
    touchpoint_offer_type_grouped
  FROM base 

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@dmicovic",
    updated_by="@rkohnke",
    created_date="2023-05-15",
    updated_date="2023-05-30",
  ) }}
