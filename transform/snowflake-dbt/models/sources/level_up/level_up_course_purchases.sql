{{ config(
    materialized='incremental',
    unique_key='id'
) }}

{{ level_up_incremental('course_purchases') }}

parsed AS (
  SELECT
  value['id']::varchar as id,
  value['transactionId']::varchar as transaction_id,
  {{ level_up_filter_gitlab_email("value['giftRecipientEmail']") }} as gift_recipient_email,
  value['timestamp']::timestamp as event_timestamp,
  value['companyId']::varchar as company_id,
  value['coupon']::variant as coupon,
  value['couponCode']::varchar as coupon_code,
  value['purchasableId']::varchar as purchasable_id,
  value['purchasableType']::varchar as purchasable_type,
  value['chargeId']::varchar as charge_id,
  value['groupId']::varchar as group_id,
  value['orderId']::varchar as order_id,
  value['revenueType']::varchar as revenue_type,
  value['quantity']::int as quantity,
  value['variation']::varchar as variation,
  value['shippingMethod']::varchar as shipping_method,
  value['type']::varchar as type,
  value['sku']::varchar as course_sku,
  value['slug']::varchar as course_slug,
  value['title']::varchar as course_title,
  value['courseStartDate']::timestamp as course_start_date,
  value['success']::boolean as is_success,
  value['failureCode']::varchar as failure_code,
  value['failureMessage']::varchar as failure_message,
  {{ level_up_filter_gitlab_email("value['user']") }} as username,
  value['userDetail']['id']::varchar as user_id,
  value['userDetail']['stripeCustomerId']::varchar as stripe_customer_id,
  value['userDetail']['sfContactId']::varchar as sf_contact_id,
  value['userDetail']['sfAccountId']::varchar as sf_account_id,
  value['referral']['referrer']::varchar as referral_referrer,
  value['referral']['referrer_type']::varchar as referral_referrer_type,
  value['referral']['source']::varchar as referral_source,

  uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
