{%- macro product_category(product_column, output_column_name = 'product_category') -%}

CASE
  WHEN LOWER({{product_column}}) LIKE '%saas - ultimate%'
    THEN 'SaaS - Ultimate'
  WHEN LOWER({{product_column}}) LIKE '%saas - premium%'
    THEN 'SaaS - Premium'
  WHEN LOWER({{product_column}}) LIKE '%dedicated - ultimate%'
    THEN 'Dedicated - Ultimate'
  WHEN LOWER({{product_column}}) LIKE '%ultimate%'
    THEN 'Self-Managed - Ultimate'
  WHEN LOWER({{product_column}}) LIKE '%premium%'
    THEN 'Self-Managed - Premium'
  WHEN LOWER({{product_column}}) LIKE '%gold%'
    THEN 'SaaS - Ultimate'
  WHEN LOWER({{product_column}}) LIKE '%silver%'
    THEN 'SaaS - Premium'
  WHEN LOWER({{product_column}}) LIKE '%bronze%'
    THEN 'SaaS - Bronze'
  WHEN LOWER({{product_column}}) LIKE '%starter%'
    THEN 'Self-Managed - Starter'
  WHEN LOWER({{product_column}}) LIKE 'gitlab enterprise edition%'
    THEN 'Self-Managed - Starter'
  WHEN {{product_column}} = 'Pivotal Cloud Foundry Tile for GitLab EE'
    THEN 'Self-Managed - Starter'
  WHEN LOWER({{product_column}}) LIKE 'plus%'
    THEN 'Plus'
  WHEN LOWER({{product_column}}) LIKE 'standard%'
    THEN 'Standard'
  WHEN LOWER({{product_column}}) LIKE 'basic%'
    THEN 'Basic'
  WHEN {{product_column}} = 'Trueup'
    THEN 'Trueup'
  WHEN LOWER({{product_column}}) LIKE 'saas - code suggestions%'
    THEN 'SaaS - AI Add Ons'
  WHEN LTRIM(LOWER({{product_column}})) LIKE 'githost%'
    THEN 'GitHost'
  WHEN LOWER({{product_column}}) LIKE ANY ('%quick start with ha%', '%proserv training per-seat add-on%')
    THEN 'Support'
  WHEN TRIM({{product_column}}) IN (
                                      'GitLab Service Package'
                                    , 'Implementation Services Quick Start'
                                    , 'Implementation Support'
                                    , 'Implementation Quick Start'
                                    , 'Support Package'
                                    , 'Admin Training'
                                    , 'CI/CD Training'
                                    , 'GitLab Project Management Training'
                                    , 'GitLab with Git Basics Training'
                                    , 'Travel Expenses'
                                    , 'Training Workshop'
                                    , 'GitLab for Project Managers Training - Remote'
                                    , 'GitLab with Git Basics Training - Remote'
                                    , 'GitLab for System Administrators Training - Remote'
                                    , 'GitLab CI/CD Training - Remote'
                                    , 'InnerSourcing Training - Remote for your team'
                                    , 'GitLab DevOps Fundamentals Training'
                                    , 'Self-Managed Rapid Results Consulting'
                                    , 'Gitlab.com Rapid Results Consulting'
                                    , 'GitLab Security Essentials Training - Remote Delivery'
                                    , 'InnerSourcing Training - At your site'
                                    , 'Migration+'
                                    , 'One Time Discount'
                                    , 'LDAP Integration'
                                    , 'Dedicated Implementation Services'
                                    , 'Quick Start without HA, less than 500 users'
                                    , 'Jenkins Integration'
                                    , 'Hourly Consulting'
                                    , 'JIRA Integration'
                                    , 'Custom PS Education Services'
                                    , 'Dedicated Engineer - 3 Month (w/ Security Clearance)'
                                    , 'Dedicated Engineer - 12 Month'
                                    , 'Dedicated Engineer - 3 Month'
                                    , 'Dedicated Engineer - 6 Month (w/ Security Clearance)'
                                    , 'Dedicated Engineer - 6 Month'
                                    , 'GitLab System Administration Training - Remote'
                                    , 'Expert Services (48 Hours)'
                                    , 'GitLab CI/CD Training - Onsite'
                                    , 'GitLab DevSecOps Fundamentals Training - Remote'
                                    , 'GitLab Agile Portfolio Management Training - Remote'
                                    , 'GitLab System Administration Training - Onsite'
                                    , 'GitLab Agile Portfolio Management Training - Onsite'
                                    , 'GitLab Security Essentials Training - Remote'
                                    , 'GitLab with Git Fundamentals Training - Onsite'
                                    , 'GitLab with Git Fundamentals Training - Remote'
                                    , 'Implementation QuickStart - GitLab.com'
                                    , 'GitLab Security Essentials Training - Onsite'
                                    , 'Implementation QuickStart - Self Managed (HA)'
                                    )
    THEN 'Support'
  WHEN LOWER({{product_column}}) LIKE 'gitlab geo%'
    THEN 'SaaS - Other'
  WHEN LOWER({{product_column}}) LIKE 'ci runner%'
    THEN 'SaaS - Other'
  WHEN LOWER({{product_column}}) LIKE 'discount%'
    THEN 'Other'
  WHEN TRIM({{product_column}}) IN (
                                      '#movingtogitlab'
                                    , 'Payment Gateway Test'
                                    , 'EdCast Settlement Revenue'
                                    , 'GitLab Certification Exam'
                                    )
    THEN 'Other'
  WHEN TRIM({{product_column}}) IN (
                                      'File Locking'
                                    , 'Time Tracking'
                                    , '1,000 CI Minutes'
                                    , '1,000 Compute Minutes'
                                    , 'Training LMS Settlement Revenue'
                                    )
    THEN 'SaaS - Other'
  WHEN TRIM({{product_column}}) IN ('Gitlab Storage 10GB')
    THEN 'Storage'
  ELSE 'Not Applicable'
END AS {{output_column_name}}

{%- endmacro -%}
