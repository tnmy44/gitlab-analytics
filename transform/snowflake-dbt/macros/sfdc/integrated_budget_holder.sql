{%- macro integrated_budget_holder(budget_holder,utm_budget,bizible_ad_campaign_name,utm_medium,campaign_rep_user_role_name) -%}


    CASE 
      WHEN LOWER({{ budget_holder }}) = 'fmm' 
        THEN 'Field Marketing'
      WHEN LOWER({{ budget_holder }}) = 'dmp' 
        THEN 'Digital Marketing'
      WHEN LOWER({{ budget_holder }}) = 'corp' 
        THEN 'Corporate Events'
      WHEN LOWER({{ budget_holder }})  = 'abm' 
        THEN 'Account Based Marketing'
      WHEN LOWER({{ budget_holder }}) LIKE '%cmp%' 
        THEN 'Campaigns Team'
      WHEN LOWER({{ budget_holder }}) = 'devrel'
        THEN 'Developer Relations Team'
      WHEN LOWER({{ budget_holder }})  LIKE '%ptnr%' OR LOWER({{ budget_holder }})  LIKE '%chnl%'  
        THEN 'Partner Marketing'
      WHEN LOWER({{ utm_budget }})  = 'fmm' 
        THEN 'Field Marketing'
      WHEN LOWER({{ utm_budget }}) = 'dmp' 
        THEN 'Digital Marketing'
      WHEN LOWER({{ utm_budget }}) = 'corp' 
        THEN 'Corporate Events'
      WHEN LOWER({{ utm_budget }}) = 'abm' 
        THEN 'Account Based Marketing'
      WHEN LOWER({{ utm_budget }}) LIKE '%cmp%' 
        THEN 'Campaigns Team'
      WHEN LOWER({{ utm_budget }}) = 'devrel' 
        THEN 'Developer Relations Team'
      WHEN LOWER({{ utm_budget }})LIKE '%ptnr%' OR LOWER({{ utm_budget }}) LIKE '%chnl%' 
        THEN 'Partner Marketing'
      WHEN LOWER({{ bizible_ad_campaign_name }}) LIKE '%abm%' 
        THEN 'Account Based Marketing'
      WHEN LOWER({{ bizible_ad_campaign_name }})  LIKE '%pmg%' 
        THEN 'Digital Marketing'
      WHEN LOWER({{ bizible_ad_campaign_name }})  LIKE '%fmm%' 
        THEN 'Field Marketing'
      WHEN LOWER({{ bizible_ad_campaign_name }})  LIKE '%dmp%' 
        THEN 'Digital Marketing'
      WHEN LOWER({{ bizible_ad_campaign_name }})  LIKE '%partner%' 
        THEN 'Partner Marketing'
      WHEN LOWER({{ bizible_ad_campaign_name }})  LIKE '%mdf%' 
        THEN 'Partner Marketing'
      WHEN LOWER({{ utm_medium }})  IN ('paidsocial','cpc') 
        THEN 'Digital Marketing'
      WHEN LOWER({{ bizible_ad_campaign_name }}) LIKE '%devopsgtm_' 
        THEN 'Digital Marketing'
      WHEN LOWER({{ campaign_rep_user_role_name }}) LIKE '%field marketing%' 
        THEN 'Field Marketing'
      WHEN LOWER({{ campaign_rep_user_role_name }}) LIKE '%abm%' 
        THEN 'Account Based Marketing'
    END AS integrated_budget_holder

{%- endmacro -%}
