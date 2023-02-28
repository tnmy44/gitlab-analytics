-- namespace classification snippet

  WITH date_spine AS (

    SELECT DISTINCT DATE_TRUNC(DAY, date_day) AS date_actual
    FROM prod.legacy.date_details AS {{ ref('date_details') }}
    WHERE date_day >= '2022-01-01'
      AND date_day <= CURRENT_DATE
  ),

  ns_daily AS (

    WITH
    dn AS (
      SELECT *
      FROM
        {{ ref('dim_namespace') }}
    ),

    u AS (
      SELECT *
      FROM
        {{ ref('dim_user') }}
    ),

    hist AS (
      SELECT *
      FROM
        {{ ref('dim_order_hist') }}
    ),

    pd AS (
      SELECT *
      FROM
        {{ ref('dim_product_detail') }}
    ),

    ch AS (
      SELECT *
      FROM
        {{ ref('customers_db_charges_xf') }}
    ),

    namespaces AS (
      SELECT *
      FROM
        {{ ref('gitlab_dotcom_namespaces_xf') }}
    ),

    m AS (
      SELECT *
      FROM
        {{ ref('mart_arr') }}
    ),

    ossedu_1 AS (
      SELECT
        hist.*,
        dn.namespace_is_internal,
        dn.visibility_level,
        dn.namespace_creator_is_blocked,
        dn.dim_namespace_id AS dim_namespace_id_,
        dn.gitlab_plan_title,
        dn.gitlab_plan_is_paid,
        u.email_domain--, pd.PRODUCT_RATE_PLAN_CHARGE_NAME
        ,
        MAX(IFF(pd.product_rate_plan_id IS NOT NULL,
          1,
          0))               AS record_present,
        ARRAY_TO_STRING(
          ARRAYAGG(DISTINCT pd.product_rate_plan_charge_name), ','
        )                   AS product_rate_plan,
        MAX(IFF(pd.is_oss_or_edu_rate_plan = TRUE,
          1,
          0))               AS is_ossedu,
        MAX(IFF(LOWER(pd.product_rate_plan_charge_name) LIKE '%edu%'
          AND LOWER(
            pd.product_rate_plan_charge_name
          ) LIKE '%oss%',
          1,
          0))               AS is_oss_or_edu,
        MAX(IFF(LOWER(pd.product_rate_plan_charge_name) LIKE '%oss%',
          1,
          0))               AS is_oss,
        MAX(IFF(LOWER(pd.product_rate_plan_charge_name) LIKE '%edu%',
          1,
          0))               AS is_edu,
        MAX(IFF(LOWER(pd.product_rate_plan_charge_name) LIKE '%storage%',
          1,
          0))               AS is_storage,
        MAX(IFF(LOWER(pd.product_rate_plan_charge_name) LIKE '%minutes%',
          1,
          0))               AS is_ci,
        MAX(IFF(u.email_domain = 'gitlab.com',
          1,
          0))               AS is_gitlab_employee,
        MAX(IFF(LOWER(pd.product_rate_plan_charge_name) LIKE '%combinator%'
          OR LOWER(
            pd.product_rate_plan_charge_name
          ) LIKE '%startup%',
          1,
          0))               AS is_startup
      FROM
        dn
      LEFT JOIN
        u
        ON
          u.dim_user_id = dn.creator_id
      LEFT JOIN
        hist
        ON
          hist.dim_namespace_id = dn.dim_namespace_id
          AND valid_from::DATE <= GETDATE()::DATE
          AND COALESCE(valid_to, GETDATE())::DATE >= GETDATE()::DATE -- there is 1 to many rows for this even with valid from to constraints, but only ~5% duplicate.
      LEFT JOIN
        pd
        ON
          hist.product_rate_plan_id = pd.product_rate_plan_id
      WHERE
        dn.namespace_is_ultimate_parent = TRUE
      GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28
    ),

    -- understand which namespace paid in the last two months and for what.
    mrr_ns AS (
      SELECT
        arr_month,
        c.namespace_id,
        MAX(
          CASE
            WHEN product_tier_name IN ('Storage') THEN 1
            ELSE
              0
          END
        ) AS paid_for_storage,
        MAX(
          CASE
            WHEN
              product_tier_name IN (
                'SaaS - Bronze', 'SaaS - Premium', 'SaaS - Ultimate'
              ) THEN 1
            ELSE
              0
          END
        ) AS paid_for_plan
      FROM (
        SELECT
          namespace_ultimate_parent_id AS namespace_id,
          subscription_name_slugify,
          MIN(subscription_start_date) AS sd
        FROM
          ch
        INNER JOIN
          namespaces
          ON
            ch.current_gitlab_namespace_id::INT = namespaces.namespace_id
        WHERE
          ch.current_gitlab_namespace_id IS NOT NULL
        --and ch.product_category IN ('SaaS - Ultimate','SaaS - Premium','SaaS - Bronze')
        GROUP BY
          1,
          2) AS c
      INNER JOIN
        m
        ON
          c.subscription_name_slugify = m.subscription_name_slugify
          AND m.product_delivery_type = 'SaaS'
      -- and arr_month between dateadd(month,-2,getdate()::date )  and getdate()::date
      -- and product_tier_name != 'Storage'
      GROUP BY
        1,
        2
    )

    SELECT
      mrr_ns.arr_month AS month,
      ossedu_1.*,
      CASE
        WHEN namespace_is_internal = TRUE THEN 'internal'
        WHEN gitlab_plan_is_paid = TRUE
          AND COALESCE(paid_for_plan, 0) = 0
          AND is_gitlab_employee = 1 THEN 'internal employee'
        WHEN
          gitlab_plan_is_paid = TRUE AND paid_for_plan = 1 THEN 'Paid for plan'
        WHEN paid_for_storage = 1 THEN 'Paid for storage'
        WHEN
          gitlab_plan_title = 'Open Source Program' THEN 'Open Source Program'
        WHEN is_ossedu = 1
          OR is_startup = 1 THEN 'OSS EDU or Startup'
        WHEN
          gitlab_plan_is_paid = FALSE AND namespace_creator_is_blocked = TRUE THEN 'Free blocked user'
        WHEN gitlab_plan_is_paid = FALSE THEN 'Free'
        WHEN gitlab_plan_is_paid = TRUE THEN 'Paid for plan'
        WHEN gitlab_plan_is_paid = FALSE THEN 'free others'
        ELSE
          'others'
      END
      AS namespace_type_details,
      CASE
        WHEN namespace_is_internal = TRUE THEN 'internal'
        WHEN gitlab_plan_is_paid = TRUE
          AND COALESCE(paid_for_plan, 0) = 0
          AND is_gitlab_employee = 1 THEN 'internal'
        WHEN gitlab_plan_is_paid = TRUE AND paid_for_plan = 1 THEN 'Paid'
        WHEN paid_for_storage = 1 THEN 'Paid'
        WHEN gitlab_plan_title = 'Open Source Program' THEN 'Free'
        WHEN is_ossedu = 1
          OR is_startup = 1 THEN 'Free'
        WHEN
          gitlab_plan_is_paid = FALSE AND namespace_creator_is_blocked = TRUE THEN 'Free'
        WHEN gitlab_plan_is_paid = FALSE THEN 'Free'
        WHEN gitlab_plan_is_paid = TRUE THEN 'Paid'
        WHEN gitlab_plan_is_paid = FALSE THEN 'free others'
        ELSE
          'others'
      END
      AS namespace_type
    FROM
      ossedu_1
    LEFT JOIN
      mrr_ns
      ON
        mrr_ns.namespace_id = ossedu_1.dim_namespace_id
    ORDER BY 1 ASC
  )

  SELECT
    date_spine.date_actual AS day,
    ns_daily.dim_namespace_id,
    ns_daily.namespace_type
  FROM
    date_spine
  LEFT JOIN ns_daily
    ON
      date_spine.date_actual BETWEEN valid_from AND COALESCE(
        valid_to, GETDATE()
      )
  GROUP BY 1, 2, 3
  ORDER BY 1, 2 DESC
