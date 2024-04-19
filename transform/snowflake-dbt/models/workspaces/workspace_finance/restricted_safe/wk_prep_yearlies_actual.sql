{{ config(materialized='view') }}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_crm_opportunity', 'mart_crm_opportunity')
]) }},

churn_contraction_first as (

   SELECT
      mart_crm_opportunity.CLOSE_FISCAL_QUARTER_NAME,
      mart_crm_opportunity.ARR_BASIS_FOR_CLARI,
      mart_crm_opportunity.order_type,
      mart_crm_opportunity.WON_ARR_BASIS_FOR_CLARI - mart_crm_opportunity.ARR_BASIS_FOR_CLARI AS renewal_net_arr 
   FROM
      mart_crm_opportunity 
      JOIN
         dim_crm_account 
         ON dim_crm_account.dim_crm_account_id = mart_crm_opportunity.dim_crm_account_id
      left JOIN
         dim_subscription 
         on mart_crm_opportunity.dim_crm_opportunity_id = dim_subscription.dim_crm_opportunity_id 
   WHERE
      mart_crm_opportunity.SALES_TYPE = 'Renewal' 
      AND mart_crm_opportunity.STAGE_NAME IN 
      (
         'Closed Won',
         '8-Closed Lost'
      )
      AND mart_crm_opportunity.IS_JIHU_ACCOUNT = FALSE 
      AND mart_crm_opportunity.ORDER_TYPE IN 
      (
         '3. Growth',
         '4. Contraction',
         '5. Churn - Partial',
         '6. Churn - Final'
      )
      AND mart_crm_opportunity.CLOSE_MONTH BETWEEN '2022-02-01' AND date_trunc('month', current_date) 
),

churn_contraction_second as (

   SELECT
      CLOSE_FISCAL_QUARTER_NAME,
      sum(case when order_type in ('4. Contraction','5. Churn - Partial','6. Churn - Final') then renewal_net_arr end) as renewal_net_arr_loss,     
      sum(arr_basis_for_clari) as atr 
   FROM
      base 
   group by
      1 
),

churn_contraction_final as (

select
   '3.4 Churn and Contraction' as yearly_name,
   'Chris Weber, Sherrod Patching' as yearly_dri,
   'Churn and contraction under % of ATR' as yearly_description,
    true as is_mnpi,
   'PROD.RESTRICTED_SAFE_COMMON_SALES.MART_CRM_OPPORTUNITY' as source_table,
   CLOSE_FISCAL_QUARTER_NAME as quarter,
   renewal_net_arr_loss / atr* - 1 as actuals_raw
from
   final 
where quarter like 'FY25%'
 
order by
   1 desc

),