WITH bizible_touchpoints AS (
 
    SELECT 
      touchpoint_id,
      campaign_id,
      bizible_touchpoint_type,
      bizible_touchpoint_source,
      bizible_landing_page,
      bizible_landing_page_raw,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_ad_campaign_name,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_ad_content
    FROM {{ ref('sfdc_bizible_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), bizible_attribution_touchpoints AS (

    SELECT 
      touchpoint_id,
      campaign_id,
      bizible_touchpoint_type,
      bizible_touchpoint_source,
      bizible_landing_page,
      bizible_landing_page_raw,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_ad_campaign_name,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_ad_content
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), bizible AS (

    SELECT *
    FROM bizible_touchpoints

    UNION ALL

    SELECT *
    FROM bizible_attribution_touchpoints

), campaign AS (

    SELECT *
    FROM {{ ref('prep_campaign') }}

), touchpoints_with_campaign AS (
    
    SELECT 
      {{ dbt_utils.surrogate_key(['campaign.dim_campaign_id','campaign.dim_parent_campaign_id',
      								'bizible.bizible_touchpoint_type','bizible.bizible_landing_page',
      								'bizible.bizible_referrer_page','bizible.bizible_form_url',
      								'bizible.bizible_ad_campaign_name','bizible.bizible_marketing_channel_path'
      							]) 
      }}																										AS bizible_campaign_grouping_id,
      bizible.touchpoint_id                                 AS dim_crm_touchpoint_id,
      campaign.dim_campaign_id,
      campaign.dim_parent_campaign_id,
      bizible.bizible_touchpoint_type,
      bizible.bizible_touchpoint_source,
      bizible.bizible_landing_page,
      bizible.bizible_landing_page_raw,
      bizible.bizible_referrer_page,
      bizible.bizible_referrer_page_raw,
      bizible.bizible_form_url,
      bizible.bizible_ad_campaign_name,
      bizible.bizible_marketing_channel_path,
      bizible.bizible_ad_content,
      bizible.bizible_medium,
      bizible.bizible_form_url_raw,
      CASE
   WHEN campaign.campaign_parent_id = '7014M000001dowZQAQ' -- based on issue https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/246
    OR (bizible_medium = 'sponsorship'
      AND bizible_touchpoint_source IN ('issa','stackoverflow','securityweekly-appsec','unix&linux','stackexchange'))
    THEN 'Publishers/Sponsorships' 
    WHEN  (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%smbnurture%' 
      OR bizible_form_url LIKE '%smbnurture%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%smbnurture%'
      OR bizible_ad_campaign_name LIKE '%smbnurture%'
      OR bizible_landing_page LIKE '%smbagnostic%' 
      OR bizible_form_url LIKE '%smbagnostic%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%smbagnostic%'
      OR bizible_ad_campaign_name LIKE '%smbagnostic%'))
      OR bizible_ad_campaign_name = 'Nurture - SMB Mixed Use Case'
      THEN 'SMB Nurture' 
    WHEN  (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%cicdseeingisbelieving%' 
      OR bizible_form_url LIKE '%cicdseeingisbelieving%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%cicdseeingisbelieving%'
      OR bizible_ad_campaign_name LIKE '%cicdseeingisbelieving%'))
      OR bizible_ad_campaign_name = '20201215_HowCiDifferent' --added 2022-04-06 Agnes O Demand Gen issue 2330
      THEN 'CI/CD Seeing is Believing' 
    WHEN  (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%simplifydevops%' 
      OR bizible_form_url LIKE '%simplifydevops%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%simplifydevops%'
      OR bizible_ad_campaign_name LIKE '%simplifydevops%'))
      OR campaign.campaign_parent_id = '7014M000001doAGQAY'
      OR campaign.campaign_id LIKE '7014M000001dn6z%'
      THEN 'Simplify DevOps' 
    WHEN  (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%21q4-jp%' 
      OR bizible_form_url LIKE '%21q4-jp%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%21q4-jp%'
      OR bizible_ad_campaign_name LIKE '%21q4-jp%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND bizible_ad_campaign_name ='2021_Social_Japan_LinkedIn Lead Gen')
      THEN 'Japan-Digital Readiness' 
    WHEN  (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%lower-tco%' 
      OR bizible_form_url LIKE '%lower-tco%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%lower-tco%'
      OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'
      OR bizible_ad_campaign_name LIKE '%operationalefficiences%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND (bizible_ad_campaign_name LIKE '%_Operational Efficiencies%'
          OR bizible_ad_campaign_name LIKE '%operationalefficiencies%'))
      THEN 'Increase Operational Efficiencies' 
    WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%reduce-cycle-time%' 
      OR bizible_form_url LIKE '%reduce-cycle-time%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%reduce-cycle-time%'
      OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND (bizible_ad_campaign_name LIKE '%_Better Products Faster%'
          OR bizible_ad_campaign_name LIKE '%betterproductsfaster%'))
      THEN 'Deliver Better Products Faster'
    WHEN (bizible_touchpoint_type = 'Web Form'
      AND (bizible_landing_page LIKE '%secure-apps%' 
      OR bizible_form_url LIKE '%secure-apps%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%secure-apps%'
      OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND (bizible_ad_campaign_name LIKE '%_Reduce Security Risk%'
          OR bizible_ad_campaign_name LIKE '%reducesecurityrisk%'))
      THEN 'Reduce Security and Compliance Risk'
    WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%jenkins-alternative%' 
      OR bizible_form_url LIKE '%jenkins-alternative%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%jenkins-alternative%'
      OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ' 
      AND (bizible_ad_campaign_name LIKE '%_Jenkins%'
          OR bizible_ad_campaign_name LIKE '%cicdcmp2%'))
      THEN 'Jenkins Take Out'
    WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%single-application-ci%' 
      OR bizible_form_url LIKE '%single-application-ci%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%single-application-ci%'
      OR bizible_ad_campaign_name LIKE '%cicdcmp3%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ' 
      AND bizible_ad_campaign_name LIKE '%cicdcmp3%')
      THEN 'CI Build & Test Auto'
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%github-actions-alternative%' 
      OR bizible_form_url LIKE '%github-actions-alternative%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%github-actions-alternative%'
      OR bizible_ad_campaign_name LIKE '%octocat%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND bizible_ad_campaign_name ILIKE '%_OctoCat%')
      OR  bizible_ad_campaign_name = '20200122_MakingCaseCICD'
      THEN 'OctoCat'
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%' 
      OR bizible_form_url LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%integration-continue-pour-construire-et-tester-plus-rapidement%'
      OR (bizible_ad_campaign_name LIKE '%singleappci%' and BIZIBLE_AD_CONTENT LIKE '%french%')))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND bizible_ad_campaign_name ILIKE '%Singleappci_French%')
      THEN 'CI Use Case - FR'
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%' 
      OR bizible_form_url LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
      OR (bizible_ad_campaign_name LIKE '%singleappci%' and BIZIBLE_AD_CONTENT LIKE '%paesslergerman%')))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND bizible_ad_campaign_name ILIKE '%Singleappci_German%')
      THEN 'CI Use Case - DE'
  WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-11-22 MSandP: 585
      AND ( bizible_form_url_raw LIKE '%whygitlabdevopsplatform-apac%'
      OR bizible_landing_page_raw LIKE '%whygitlabdevopsplatform-apac%'
      OR bizible_referrer_page_raw LIKE '%whygitlabdevopsplatform-apac%'))
      OR bizible_ad_campaign_name = '20211208_GitHubCompetitive_APAC'
  THEN 'FY22 GitHub Competitive Campaign - APAC' --added 2022-04-06 Agnes O Demand Gen issue 2330
  WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-11-22 MSandP: 585
      AND ( bizible_form_url_raw LIKE '%whygitlabdevopsplatform%'
      OR bizible_landing_page_raw LIKE '%whygitlabdevopsplatform%'
      OR bizible_referrer_page_raw LIKE '%whygitlabdevopsplatform%'
      OR bizible_form_url_raw LIKE '%githubcompete%'
      OR bizible_landing_page_raw LIKE '%githubcompete%'
      OR bizible_referrer_page_raw LIKE '%githubcompete%'
      OR bizible_form_url_raw LIKE '%gitlabcicdwithgithub%' --added 2022-09-28 Agnes O MS & P issue 897
      OR bizible_landing_page_raw LIKE '%gitlabcicdwithgithub%'--added 2022-09-28 Agnes O MS & P issue 897
      OR bizible_referrer_page_raw LIKE '%gitlabcicdwithgithub%'--added 2022-09-28 Agnes O MS & P issue 897
      OR bizible_form_url_raw LIKE '%conversica-move-to-gitlab%'
      OR bizible_landing_page_raw LIKE '%conversica-move-to-gitlab%'
      OR bizible_referrer_page_raw LIKE '%conversica-move-to-gitlab%' -- added 2022-04-06 Agnes O MS & P issue 874
        ))
    OR bizible_ad_campaign_name = '20211202_GitHubCompetitive'
    OR bizible_ad_campaign_name LIKE '%competegh%' --added 2022-04-06 Agnes O Demand Gen issue 2330
    OR bizible_ad_campaign_name = '20220713_GitHubtoGitLabMigration_AMER_EMEA' -- added 2022-04-06 Agnes O MS & P issue 874
    OR (campaign.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name ILIKE '%github%') --added 2022-09-28 Agnes O MS & P issue 897
  THEN 'FY22 GitHub Competitive Campaign'
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%use-continuous-integration-to-build-and-test-faster%' 
      OR bizible_form_url LIKE '%use-continuous-integration-to-build-and-test-faster%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%use-continuous-integration-to-build-and-test-faster%'
      OR bizible_ad_campaign_name LIKE '%singleappci%'))
      OR bizible_ad_campaign_name ='20201013_ActualTechMedia_DeepMonitoringCI'
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND (bizible_ad_campaign_name LIKE '%_CI%'
          OR bizible_ad_campaign_name ILIKE '%singleappci%'))
    OR (campaign.campaign_parent_id = '7014M000001vm9KQAQ' AND (bizible_ad_campaign_name LIKE '%_CI%'
      OR bizible_ad_campaign_name ILIKE '%singleappci%'))
      THEN 'CI Use Case' --- Added by AO demand gen issue 2262
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%shift-your-security-scanning-left%' 
      OR bizible_form_url LIKE '%shift-your-security-scanning-left%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%shift-your-security-scanning-left%'
      OR bizible_form_url LIKE '%developer-survey%' -- Added by AO 2023-01-23
      OR bizible_landing_page LIKE '%developer-survey%' -- Added by AO 2023-01-23
      OR BIZIBLE_REFERRER_PAGE LIKE '%developer-survey%' -- Added by AO 2023-01-23
      OR bizible_form_url LIKE '%2022-devsecops-report%' -- Added by AO 2023-01-23
      OR bizible_landing_page LIKE '%2022-devsecops-report%' -- Added by AO 2023-01-23
      OR BIZIBLE_REFERRER_PAGE LIKE '%2022-devsecops-report%' -- Added by AO 2023-01-23
      OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
      OR campaign.campaign_parent_id = '7014M000001dnVOQAY' -- GCP Partner campaign
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND (bizible_ad_campaign_name ILIKE '%_DevSecOps%'
          OR bizible_ad_campaign_name LIKE '%devsecopsusecase%'))
      OR (campaign.campaign_parent_id = '7014M000001vm9KQAQ'
      AND (bizible_ad_campaign_name ILIKE '%DevSecOps%'
      OR bizible_ad_campaign_name LIKE '%Fuzzing%'))-- Added by AO demand gen issue 2262
      THEN 'DevSecOps Use Case'
  WHEN (bizible_touchpoint_type = 'Web Form'
      AND (BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=autosd%'
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=autosd%'
      OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=autosd%'))
      OR bizible_ad_campaign_name = 'autosd'
      OR bizible_ad_campaign_name ILIKE '%AutomatedSoftwareDelivery%' -- added by AO MS & P issue 896
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ' AND bizible_ad_campaign_name LIKE '%autosd%') --- Added by AO MS&P issue 825
    THEN 'Automated Software Delivery' 
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%aws-gitlab-serverless%' 
      OR bizible_landing_page LIKE '%trek10-aws-cicd%'
      OR bizible_form_url LIKE '%aws-gitlab-serverless%' 
      OR bizible_form_url LIKE '%trek10-aws-cicd%'
      OR BIZIBLE_REFERRER_PAGE LIKE '%aws-gitlab-serverless%'
      OR bizible_ad_campaign_name LIKE '%awspartner%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND bizible_ad_campaign_name ILIKE '%_AWS%')
      THEN 'AWS'
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%simplify-collaboration-with-version-control%' 
      OR bizible_form_url LIKE '%simplify-collaboration-with-version-control%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%simplify-collaboration-with-version-control%'
      OR bizible_ad_campaign_name LIKE '%vccusecase%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND (bizible_ad_campaign_name LIKE '%_VCC%'
          OR bizible_ad_campaign_name LIKE '%vccusecase%'))
      THEN 'VCC Use Case'
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_landing_page LIKE '%gitops-infrastructure-automation%' 
      OR bizible_form_url LIKE '%gitops-infrastructure-automation%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%gitops-infrastructure-automation%'
      OR bizible_ad_campaign_name LIKE '%iacgitops%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND (bizible_ad_campaign_name LIKE '%_GitOps%'
          OR bizible_ad_campaign_name LIKE '%iacgitops%'))
      OR (campaign.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name ILIKE '%GitOps_%') --- Added by AO demand gen issue 2327
      THEN 'GitOps Use Case'
  WHEN  (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_ad_campaign_name LIKE '%evergreen%'
      OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=evergreen%'
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=evergreen%'
      OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=evergreen%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND bizible_ad_campaign_name ILIKE '%_Evergreen%')
    Then 'Evergreen'
  WHEN (bizible_touchpoint_type = 'Web Form' 
      AND (bizible_ad_campaign_name LIKE 'brand%'
      OR bizible_ad_campaign_name LIKE 'Brand%'
      OR BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=brand%'
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=brand%'
      OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=brand%'))
      OR (campaign.campaign_parent_id = '7014M000001dn8MQAQ'
      AND bizible_ad_campaign_name ILIKE '%_Brand%')
    Then 'Brand'
  WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 332
      AND (bizible_landing_page LIKE '%contact-us-ultimate%' 
      OR bizible_form_url LIKE '%contact-us-ultimate%' 
      OR BIZIBLE_REFERRER_PAGE LIKE '%contact-us-ultimate%'
      OR bizible_ad_campaign_name LIKE '%premtoultimatesp%'))
      THEN 'Premium to Ultimate'
  WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
      AND ( BIZIBLE_FORM_URL_RAW LIKE '%webcast-gitops-multicloudapp%'
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%webcast-gitops-multicloudapp%'
      OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%webcast-gitops-multicloudapp%'))
      OR (campaign.campaign_parent_id LIKE '%7014M000001dpmf%')
    Then 'GitOps GTM webcast'
  WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
      AND ( BIZIBLE_FORM_URL_RAW LIKE '%devopsgtm%'
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%devopsgtm%'
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%events-amer-enterprise%'---Added by AO MSandP issue:811
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%events-pd-emea%'----Added by AO MSandP issue:811
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%events-pd-technical-apac%'---Added by AO MSandP issue:811
      OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%devopsgtm%'))
      OR campaign.campaign_parent_id LIKE '%7014M000001dpT9%'
        -- OR campaign.campaign_parent_id LIKE '%7014M000001dn8M%')
      OR campaign.campaign_id LIKE '%7014M000001vbtw%'
      OR campaign.campaign_id LIKE '%7018X000001lmtN%' -- Added by AO MSandP issue 880
      OR (campaign.campaign_parent_id = '7014M000001vm9KQAQ'
      AND (bizible_ad_campaign_name ILIKE '%MLOps%'
      OR bizible_ad_campaign_name ILIKE '%Dora%'
      OR bizible_ad_campaign_name ILIKE '%DevOps%'
      OR bizible_ad_campaign_name ILIKE '%DOP%'))-- Added by AO demand gen issue 2262
    Then 'DevOps GTM'
  WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
      AND (( bizible_form_url_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_form_url_raw LIKE '%utm_content=partnercredit%' 
      OR bizible_landing_page_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_landing_page_raw LIKE '%utm_content=partnercredit%'
      OR bizible_referrer_page_raw LIKE '%utm_campaign=devopsgtm%' AND bizible_referrer_page_raw LIKE '%utm_content=partnercredit%')
          OR(
        bizible_form_url_raw LIKE '%cloud-credits-promo%'
        OR bizible_landing_page_raw LIKE '%cloud-credits-promo%'
        OR bizible_referrer_page_raw LIKE '%cloud-credits-promo%'
        )))
      OR campaign.campaign_parent_id LIKE '%7014M000001vcDr%'
        -- OR campaign.campaign_parent_id LIKE '%7014M000001dn8M%')
      OR campaign.campaign_id LIKE '%7014M000001vcDr%'
    Then 'Cloud Partner Campaign'
  WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-06-04 MSandP: 346
      AND (( bizible_form_url_raw LIKE '%utm_campaign=gitlab14%'
      OR bizible_landing_page_raw LIKE '%utm_campaign=gitlab14%'
      OR bizible_referrer_page_raw LIKE '%utm_campaign=gitlab14%')
        OR(
        bizible_form_url_raw LIKE '%the-shift-to-modern-devops%'
        OR bizible_landing_page_raw LIKE '%the-shift-to-modern-devops%'
        OR bizible_referrer_page_raw LIKE '%the-shift-to-modern-devops%'
        )))
    Then 'GitLab 14 webcast'
  WHEN
    campaign.campaign_id LIKE '%7014M000001drcQ%'
    Then '20210512_ISSAWebcast'
  WHEN (bizible_touchpoint_type = 'Web Form' --added 2021-0830 MSandP: 325
      AND (( bizible_form_url_raw LIKE '%psdigitaltransformation%'
      OR bizible_landing_page_raw LIKE '%psdigitaltransformation%'
      OR bizible_referrer_page_raw LIKE '%psdigitaltransformation%')
        OR(
        bizible_form_url_raw LIKE '%psglobal%'
        OR bizible_landing_page_raw LIKE '%psglobal%'
        OR bizible_referrer_page_raw LIKE '%psglobal%'
        )))
  Then 'PubSec Nurture'
  WHEN (bizible_touchpoint_type = 'Web Form'
      AND (BIZIBLE_FORM_URL_RAW LIKE '%utm_campaign=cdusecase%'
      OR BIZIBLE_LANDING_PAGE_RAW LIKE '%utm_campaign=cdusecase%'
      OR BIZIBLE_REFERRER_PAGE_RAW LIKE '%utm_campaign=cdusecase%'))
      OR (campaign.campaign_parent_id = '7014M000001vm9KQAQ' AND bizible_ad_campaign_name LIKE '%CD_%') --- Added by AO demand gen issue 2262
    THEN 'CD Use Case' 
  ELSE 'None' END AS bizible_integrated_campaign_grouping,
  CASE
    WHEN bizible_integrated_campaign_grouping IN ('CI Build & Test Auto','CI Use Case','CI Use Case - FR','CI Use Case - DE','CI/CD Seeing is Believing','Jenkins Take Out','OctoCat','Premium to Ultimate','20210512_ISSAWebcast') 
      THEN 'CI/CD'
    WHEN dim_parent_campaign_id = '7014M000001vm9KQAQ' AND campaign.gtm_motion = 'CI (CI/CD)' -- override for TechDemo Series
      THEN 'CI/CD'
    WHEN bizible_integrated_campaign_grouping IN ('Deliver Better Products Faster','DevSecOps Use Case','Reduce Security and Compliance Risk','Simplify DevOps', 'DevOps GTM', 'Cloud Partner Campaign', 'GitLab 14 webcast','DOI Webcast','FY22 GitHub Competitive Campaign') 
      THEN 'DevOps'
    WHEN dim_parent_campaign_id = '7014M000001vm9KQAQ' AND campaign.gtm_motion = 'DevOps Platform' -- override for TechDemo Series
      THEN 'DevOps'
    WHEN bizible_integrated_campaign_grouping IN ('GitOps Use Case','GitOps GTM webcast')  
      THEN 'GitOps'
    WHEN dim_parent_campaign_id = '7014M000001vm9KQAQ' AND campaign.gtm_motion = 'GITOPS' -- override for TechDemo Series
      THEN 'GitOps'
    ELSE NULL
  END                                                                                               AS gtm_motion,
  IFF(bizible_integrated_campaign_grouping <> 'None' or dim_parent_campaign_id = '7014M000001vm9KQAQ','Demand Gen','Other') -- override for TechDemo Series
                                                                                                        AS touchpoint_segment,
  CASE
    WHEN touchpoint_id ILIKE 'a6061000000CeS0%' -- Specific touchpoint overrides
      THEN 'Field Event'
    WHEN bizible_marketing_channel_path = 'CPC.AdWords'
      THEN 'Google AdWords'
    WHEN bizible_marketing_channel_path IN ('Email.Other', 'Email.Newsletter','Email.Outreach')
      THEN 'Email'
    WHEN bizible_marketing_channel_path IN ('Field Event','Partners.Google','Brand.Corporate Event','Conference','Speaking Session')
      OR (bizible_medium = 'Field Event (old)' AND bizible_marketing_channel_path = 'Other')
      THEN 'Field Event'
    WHEN bizible_marketing_channel_path IN ('Paid Social.Facebook','Paid Social.LinkedIn','Paid Social.Twitter','Paid Social.YouTube')
      THEN 'Paid Social'
    WHEN bizible_marketing_channel_path IN ('Social.Facebook','Social.LinkedIn','Social.Twitter','Social.YouTube')
      THEN 'Social'
    WHEN bizible_marketing_channel_path IN ('Marketing Site.Web Referral','Web Referral')
      THEN 'Web Referral'
    WHEN bizible_marketing_channel_path IN ('Marketing Site.Web Direct', 'Web Direct') -- Added to Web Direct
      OR dim_campaign_id IN (
                              '701610000008ciRAAQ', -- Trial - GitLab.com
                              '70161000000VwZbAAK', -- Trial - Self-Managed
                              '70161000000VwZgAAK', -- Trial - SaaS
                              '70161000000CnSLAA0', -- 20181218_DevOpsVirtual
                              '701610000008cDYAAY'  -- 2018_MovingToGitLab
                            )
      THEN 'Web Direct'
    WHEN bizible_marketing_channel_path LIKE 'Organic Search.%'
      OR bizible_marketing_channel_path = 'Marketing Site.Organic'
      THEN 'Organic Search'
    WHEN bizible_marketing_channel_path IN ('Sponsorship')
      THEN 'Paid Sponsorship'
    ELSE 'Unknown'
  END                                                                                                 AS integrated_campaign_grouping

  FROM bizible 
  LEFT JOIN campaign
    ON bizible.campaign_id = campaign.dim_campaign_id

 )

{{ dbt_audit(
    cte_ref="touchpoints_with_campaign",
    created_by="@mcooperDD",
    updated_by="@degan",
    created_date="2021-03-02",
    updated_date="2023-09-27"
) }}
