{{config({
    "materialized": "table",
    "transient": false
  })
}}

{% set renewal_fiscal_years = dbt_utils.get_column_values(
        table=ref('prep_renewal_fiscal_years'),
        where="fiscal_year >= 2019",
        column='fiscal_year',
        order_by='fiscal_year' )%}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user','dim_crm_user'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('fct_crm_opportunity', 'fct_crm_opportunity'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('dim_charge', 'dim_charge'),
    ('fct_charge', 'fct_charge'),
    ('mart_charge','mart_charge'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_amendment', 'dim_amendment'),
    ('zuora_ramps_source', 'zuora_query_api_ramps_source')

]) }}


--Historical Ramp Deals for data <= October 2021
, sheetload_map_ramp_deals AS (

    SELECT  * 
    FROM 
    {{ ref('sheetload_map_ramp_deals') }} 
    WHERE "Overwrite_SSP_ID" IS NOT NULL 


--Identifying opportunities associated with Ramp deals by using SSP_ID(introduced in 2021) from Salesforce
), ramp_deals AS (

    SELECT 
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.ssp_id, 
      dim_crm_opportunity.opportunity_term	
    FROM mart_crm_opportunity			
    INNER JOIN dim_crm_opportunity				
      ON LEFT(dim_crm_opportunity.dim_crm_opportunity_id,15) = LEFT(mart_crm_opportunity.ssp_id,15)				
    WHERE ssp_id IS NOT NULL 
      AND mart_crm_opportunity.opportunity_category LIKE '%Ramp Deal%'	


--Identifying Ramps from Zuora Module(implemented in 2023)
), zuora_ramps AS (

    SELECT  
      fct_charge.*,
      IFF(zuora_ramps_source.order_id IS NOT NULL, TRUE, FALSE) AS is_ramp_deal
    FROM fct_charge    
    LEFT JOIN zuora_ramps_source
      ON fct_charge.dim_order_id = zuora_ramps_source.order_id
    WHERE --fct_charge.is_included_in_arr_calc = 'TRUE'
    -- AND fct_charge.term_end_month = fct_charge.effective_end_month
      fct_charge.arr != 0		
   -- INNER JOIN dim_subscription_last_term
   --   ON mart_charge_base.dim_subscription_id = dim_subscription_last_term.dim_subscription_id
	

), ramp_deals_ssp_id_multiyear_linkage AS (

    SELECT 
      dim_crm_opportunity.dim_crm_opportunity_id,	
      CASE WHEN sheetload_map_ramp_deals.dim_crm_opportunity_id IS NOT NULL THEN sheetload_map_ramp_deals."Overwrite_SSP_ID"				
           WHEN dim_crm_opportunity.dim_crm_opportunity_id IS NOT NULL THEN ramp_deals.ssp_id		
      END  AS ramp_ssp_id,
    FROM dim_crm_opportunity				
    LEFT JOIN sheetload_map_ramp_deals				
     ON sheetload_map_ramp_deals.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id 
    LEFT JOIN ramp_deals					
     ON ramp_deals.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id 	
 --   LEFT JOIN zuora_ramps
--    ON zuora_ramps.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    WHERE ramp_ssp_id IS NOT NULL	


--Getting Subscriptions for ramp deals
), subscriptions_with_ssp_id AS (

    SELECT 
      ramp_deals_ssp_id_multiyear_linkage.ramp_ssp_id, 
      dim_subscription.*				
    FROM dim_subscription			
    LEFT JOIN ramp_deals_ssp_id_multiyear_linkage				
    ON dim_subscription.dim_crm_opportunity_id = ramp_deals_ssp_id_multiyear_linkage.dim_crm_opportunity_id		
    

--Getting Last term version of the subscription				  
), dim_subscription_latest_version AS (

    SELECT 
      ROW_NUMBER() OVER (PARTITION BY subscription_name, term_end_date ORDER BY ramp_ssp_id, subscription_version DESC) AS last_term_version,
      subscriptions_with_ssp_id.*				
    FROM subscriptions_with_ssp_id				
    Where subscription_status != 'Cancelled'				
    QUALIFY last_term_version = 1		


), dim_subscription_cancelled AS (	

    SELECT DISTINCT 
      subscription_name, 
      term_start_date 
    FROM dim_subscription			
    Where subscription_status = 'Cancelled'	


), dim_subscription_base AS (			

    SELECT 
      dim_subscription_latest_version.*				
    FROM dim_subscription_latest_version				
    LEFT JOIN dim_subscription_cancelled				
      ON dim_subscription_latest_version.subscription_name = dim_subscription_cancelled.subscription_name				
      AND dim_subscription_latest_version.term_start_date = dim_subscription_cancelled.term_start_date				
    WHERE dim_subscription_cancelled.subscription_name IS NULL	
	

), ramp_ssp_id_min_max_dates AS (

    SELECT 
      ramp_ssp_id, 
      MIN(term_start_date) AS min_term_start_date,  
      MAX(term_end_date) AS max_term_end_date  
    FROM dim_subscription_base			
    WHERE ramp_ssp_id IS NOT NULL				
    GROUP BY 1 HAVING COUNT(*) > 1				
		

   --For the ramp deals, we should show min_start_date and max_end_date. Also, ATR should show the amount on last year line.	
   -- Subscriptions after adjusting dates for Ramp deals  
), subscriptions_for_ramp_deals AS (		

    SELECT 
      dim_subscription_base.*, 
      CASE WHEN min_term_start_date IS NOT NULL THEN min_term_start_date 
      ELSE term_start_date 
      END AS ATR_term_start_date,				
      CASE WHEN max_term_end_date IS NOT NULL THEN max_term_end_date 
      ELSE term_end_date END AS ATR_term_end_date				
    FROM dim_subscription_base				
    LEFT JOIN ramp_ssp_id_min_max_dates				
    ON dim_subscription_base.ramp_ssp_id = ramp_ssp_id_min_max_dates.ramp_ssp_id				
    WHERE dim_subscription_base.ramp_ssp_id IS NULL
     OR (dim_subscription_base.ramp_ssp_id IS NOT NULL 
     AND max_term_end_date != term_end_date)	

  --ARR from charges		
), subscription_charges AS (

    SELECT 
      subscriptions_for_ramp_deals.dim_subscription_id, 
      subscriptions_for_ramp_deals.ATR_term_end_date,
      subscriptions_for_ramp_deals.dim_crm_account_id, 
      subscriptions_for_ramp_deals.subscription_name,
      quantity, 
      ARR				
    FROM subscriptions_for_ramp_deals			
    LEFT JOIN fct_charge			
      on subscriptions_for_ramp_deals.dim_subscription_id = fct_charge.dim_subscription_id				
      and subscriptions_for_ramp_deals.term_end_date = TO_VARCHAR(TO_DATE(TO_CHAR(effective_end_date_id),'yyyymmdd'), 'YYYY-MM-DD')				
      and fct_charge.effective_start_date_id != fct_charge.effective_end_date_id				
    LEFT JOIN dim_charge			
      ON dim_charge.dim_charge_id = fct_charge.dim_charge_id				
    WHERE fct_charge.dim_product_detail_id IS NOT NULL  --Null for "Other Non-Recurring Amount"		

--Final ATR 
), final AS (		
				
    SELECT 
      dim_date.fiscal_quarter_name_fy,
      dim_crm_account_id, 
      dim_subscription_id,
      subscription_name,
      --charge_id
      --product_id
      SUM(ARR) as ARR,
      Quantity			
    FROM subscription_charges			
    LEFT JOIN dim_date			
     ON subscription_charges.ATR_term_end_date = dim_date.date_day										
    GROUP BY 1,2,3,4,6

)					

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2024-03-20",
    updated_date="2024-03-20"
) }}