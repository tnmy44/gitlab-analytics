{{ config(
    tags=["mnpi_exception"]
) }}

 {{ config(alias='report_opportunity_stages_dates') }}

 --TODO
 -- Check out for deals created in a stage that is not 0, use the creation date

WITH sfdc_opportunity_field_history AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_field_history_source')}}

), date_details AS (

    SELECT * 
    FROM {{ ref('wk_sales_date_details') }} 

), sfdc_opportunity_xf AS (

  SELECT *
  FROM {{ ref('wk_sales_sfdc_opportunity_xf')}}

)
-- after every stage change, as it is a tracked field
-- a record would be created in the field history table
, history_base AS (

  SELECT
          opportunity_id,
          replace(replace(replace(replace(replace(new_value_string,'2-Developing','2-Scoping'),'7 - Closing','7-Closing'),'Developing','2-Scoping'),'Closed Lost','8-Closed Lost'),'8-8-Closed Lost','8-Closed Lost') AS new_value_string,
          MAX(field_modified_at::date) AS max_stage_date

  FROM sfdc_opportunity_field_history
  WHERE opportunity_field = 'stagename'
  GROUP BY 1,2

-- just created opportunities won't have any historical record
-- next CTE accounts for them
), opty_base AS (

 SELECT
    o.opportunity_id,
    o.stage_name,
    o.created_date AS max_stage_date
 FROM sfdc_opportunity_xf o
 LEFT JOIN (SELECT DISTINCT opportunity_id FROM history_base) h
    ON h.opportunity_id = o.opportunity_id
 WHERE h.opportunity_id is null

), combined AS (

  SELECT opportunity_id,
      new_value_string AS stage_name,
      max_stage_date
  FROM history_base
  UNION
  SELECT opportunity_id,
      stage_name,
      max_stage_date
  FROM opty_base

), pivoted_combined AS (

  SELECT opportunity_id,
       MAX(CASE WHEN stage_name = '0-Pending Acceptance'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_0_date,
       MAX(CASE WHEN stage_name = '1-Discovery'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_1_date,
       MAX(CASE WHEN stage_name = '2-Scoping'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_2_date,
       MAX(CASE WHEN stage_name = '3-Technical Evaluation'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_3_date,
       MAX(CASE WHEN stage_name = '4-Proposal'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_4_date,
       MAX(CASE WHEN stage_name = '5-Negotiating'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_5_date,
       MAX(CASE WHEN stage_name = '6-Awaiting Signature'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_6_date,
       MAX(CASE WHEN stage_name = '7-Closing'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_7_date,
       MAX(CASE WHEN stage_name = '8-Closed Lost'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_8_lost_date,
       MAX(CASE WHEN stage_name = 'Closed Won'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_8_won_date,
       MAX(CASE WHEN stage_name = '9-Unqualified'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_9_date,
       MAX(CASE WHEN stage_name = '10-Duplicate'
          THEN max_stage_date
       ELSE NULL END)                  AS max_stage_10_date,
       MAX(CASE WHEN stage_name IN ('8-Closed Lost','Closed Won','10-Duplicate','9-Unqualified')
          THEN max_stage_date
       ELSE NULL END)                  AS max_closed_stage_date,
       MAX(CASE WHEN stage_name IN ('8-Closed Lost','10-Duplicate','9-Unqualified')
          THEN max_stage_date
       ELSE NULL END)                  AS max_closed_lost_unqualified_duplicate_date

  FROM combined
  GROUP BY 1

)
, pre_final AS (

SELECT
        base.opportunity_id,
        opty.stage_name,
        opty.close_date,
        opty.created_date,
        opty.cycle_time_in_days,

        -- adjusted dates for throughput analysis
        -- missing stage dates are completed using the next available stage date, up to a closed date
        -- do not populate date of stages that are not reach yet
        IFF(stage_name_rank >= 0, coalesce(base.max_stage_0_date,opty.created_date),Null) as stage_0_date,
        IFF(stage_name_rank >= 1, coalesce(base.max_stage_1_date,base.max_stage_2_date,base.max_stage_3_date,base.max_stage_4_date,base.max_stage_5_date,base.max_stage_6_date,base.max_stage_7_date,base.max_stage_8_won_date),Null)  AS stage_1_date,
        IFF(stage_name_rank >= 2, coalesce(base.max_stage_2_date,base.max_stage_3_date,base.max_stage_4_date,base.max_stage_5_date,base.max_stage_6_date,base.max_stage_7_date,base.max_stage_8_won_date),Null)                     AS stage_2_date,
        IFF(stage_name_rank >= 3, coalesce(base.max_stage_3_date,base.max_stage_4_date,base.max_stage_5_date,base.max_stage_6_date,base.max_stage_7_date,base.max_stage_8_won_date),Null)                                        AS stage_3_date,
        IFF(stage_name_rank >= 4, coalesce(base.max_stage_4_date,base.max_stage_5_date,base.max_stage_6_date,base.max_stage_7_date,base.max_stage_8_won_date),Null)                                                           AS stage_4_date,
        IFF(stage_name_rank >= 5, coalesce(base.max_stage_5_date,base.max_stage_6_date,base.max_stage_7_date,base.max_stage_8_won_date) ,Null)                                              AS stage_5_date,
        IFF(stage_name_rank >= 6, coalesce(base.max_stage_6_date,base.max_stage_7_date,max_stage_8_won_date) ,Null)                                                                                                  AS stage_6_date,
        IFF(stage_name_rank >= 7, coalesce(base.max_stage_7_date,base.max_stage_8_won_date),Null)                                                                                                                    AS stage_7_date,
        IFF(stage_name_rank = 8, base.max_stage_8_lost_date,Null)                      AS stage_8_lost_date,
        IFF(stage_name_rank = 9, base.max_stage_8_won_date,Null)                       AS stage_8_won_date,
        IFF(stage_name_rank = 11, base.max_stage_9_date,Null)                          AS stage_9_date,
        IFF(stage_name_rank = 10, base.max_stage_10_date,Null)                         AS stage_10_date,
        IFF(stage_name_rank IN (8,9),base.max_closed_stage_date,Null)                  AS stage_closed_date,
        IFF(stage_name_rank IN (8,10,11),base.max_closed_lost_unqualified_duplicate_date,Null) AS stage_close_lost_unqualified_duplicate_date,

        -- was stage skipped flag
        CASE
            WHEN base.max_stage_0_date IS NULL
                AND stage_0_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_0_skipped_flag,
        CASE
            WHEN base.max_stage_1_date IS NULL
                AND stage_1_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_1_skipped_flag,
        CASE
            WHEN base.max_stage_2_date IS NULL
                AND stage_2_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_2_skipped_flag,
        CASE
            WHEN base.max_stage_3_date IS NULL
                AND stage_3_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_3_skipped_flag,
        CASE
            WHEN base.max_stage_4_date IS NULL
                AND stage_4_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_4_skipped_flag,
        CASE
            WHEN base.max_stage_5_date IS NULL
                AND stage_5_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_5_skipped_flag,
        CASE
            WHEN base.max_stage_6_date IS NULL
                AND stage_6_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_6_skipped_flag,
        CASE
            WHEN base.max_stage_7_date IS NULL
                AND stage_7_date IS NOT  NULL
                THEN 1
            ELSE 0
        END AS was_stage_7_skipped_flag

    FROM pivoted_combined base
    INNER JOIN sfdc_opportunity_xf opty
      ON opty.opportunity_id = base.opportunity_id
)
, final AS (

SELECT
        base.*,

        -- calculate age in stage
        DATEDIFF(day, base.stage_0_date, coalesce(base.stage_1_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_0,
        DATEDIFF(day, base.stage_1_date, coalesce(base.stage_2_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_1,
        DATEDIFF(day, base.stage_2_date, coalesce(base.stage_3_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_2,
        DATEDIFF(day, base.stage_3_date, coalesce(base.stage_4_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_3,
        DATEDIFF(day, base.stage_4_date, coalesce(base.stage_5_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_4,
        DATEDIFF(day, base.stage_5_date, coalesce(base.stage_6_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_5,
        DATEDIFF(day, base.stage_6_date, coalesce(base.stage_7_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_6,
        DATEDIFF(day, base.stage_7_date, coalesce(base.stage_closed_date,CURRENT_DATE))                                             AS days_in_stage_7,

        -- stage date helpers
        stage_0.fiscal_quarter_name_fy      AS stage_0_fiscal_quarter_name,
        stage_0.first_day_of_fiscal_quarter AS stage_0_fiscal_quarter_date,
        stage_0.fiscal_year                 AS stage_0_fiscal_year,

        stage_1.fiscal_quarter_name_fy      AS stage_1_fiscal_quarter_name,
        stage_1.first_day_of_fiscal_quarter AS stage_1_fiscal_quarter_date,
        stage_1.fiscal_year                 AS stage_1_fiscal_year,

        stage_2.fiscal_quarter_name_fy      AS stage_2_fiscal_quarter_name,
        stage_2.first_day_of_fiscal_quarter AS stage_2_fiscal_quarter_date,
        stage_2.fiscal_year                 AS stage_2_fiscal_year,

        stage_3.fiscal_quarter_name_fy      AS stage_3_fiscal_quarter_name,
        stage_3.first_day_of_fiscal_quarter AS stage_3_fiscal_quarter_date,
        stage_3.fiscal_year                 AS stage_3_fiscal_year,

        stage_4.fiscal_quarter_name_fy      AS stage_4_fiscal_quarter_name,
        stage_4.first_day_of_fiscal_quarter AS stage_4_fiscal_quarter_date,
        stage_4.fiscal_year                 AS stage_4_fiscal_year,

        stage_5.fiscal_quarter_name_fy      AS stage_5_fiscal_quarter_name,
        stage_5.first_day_of_fiscal_quarter AS stage_5_fiscal_quarter_date,
        stage_5.fiscal_year                 AS stage_5_fiscal_year,

        stage_6.fiscal_quarter_name_fy      AS stage_6_fiscal_quarter_name,
        stage_6.first_day_of_fiscal_quarter AS stage_6_fiscal_quarter_date,
        stage_6.fiscal_year                 AS stage_6_fiscal_year,

        stage_7.fiscal_quarter_name_fy      AS stage_7_fiscal_quarter_name,
        stage_7.first_day_of_fiscal_quarter AS stage_7_fiscal_quarter_date,
        stage_7.fiscal_year                 AS stage_7_fiscal_year

        -- calculated cycle times
        

FROM pre_final base
  LEFT JOIN  date_details stage_0
    ON stage_0.date_actual = base.stage_0_date::date
  LEFT JOIN  date_details stage_1
    ON stage_1.date_actual = base.stage_1_date::date
  LEFT JOIN  date_details stage_2
    ON stage_2.date_actual = base.stage_2_date::date
  LEFT JOIN  date_details stage_3
    ON stage_3.date_actual = base.stage_3_date::date
  LEFT JOIN  date_details stage_4
    ON stage_4.date_actual = base.stage_4_date::date
  LEFT JOIN  date_details stage_5
    ON stage_5.date_actual = base.stage_5_date::date
  LEFT JOIN  date_details stage_6
    ON stage_6.date_actual = base.stage_6_date::date
  LEFT JOIN  date_details stage_7
    ON stage_7.date_actual = base.stage_7_date::date

)

SELECT *
FROM final

