USE DATABASE raw;
USE SCHEMA data_classification;

CREATE OR REPLACE PROCEDURE execute_data_classification(p_type      VARCHAR,
                                                        p_date_from VARCHAR DEFAULT '3000-01-01',
                                                        p_unset     BOOLEAN DEFAULT FALSE)
RETURNS STRING
--------------------------------------------------------------------------------
-- Purpose: Procedure to execute data classification for PII and MNPI data
-- Date: 2024-07-09
-- Author: rbacovic
-- Parameters:
--    - p_date_from - in case you want to have an incremental load, the variable should be set in the format `yyyy-mm-dd hh24:mi:ss` (as a string)
--    - p_type      - FULL/INCREMENTAL
--    - p_unset     - TRUE/FALSE (in uppercase) - If you want to unset/drop tags, then you should set this variable to TRUE, otherwise FALSE will create/set tags

-- Version(s):
-- 0.1.0 - 2024-07-09: Initial try with the function
-- 0.2.0 - 2024-07-09: Add MNPI tags dropping
-- 0.3.0 - 2024-07-22: Add incremental load
-- 0.4.0 - 2024-07-25: Fix the bug with database name with - (add double quote)
-- 0.5.0 - 2024-07-25: Improve logging with error messages
-- 0.6.0 - 2024-07-31: Test full and incremental load
-- 0.6.1 - 2024-07-31: Test include/exclude
-- 0.7.0 - 2024-08-01: End-to-end testing from Airflow
-- 0.7.1 - 2024-08-01: Improve transaction management
-- 0.7.2 - 2024-08-01: Improve logging and transaction management
-- 1.0.0 - 2024-09-12: Strict to do only MNPI tagging
--------------------------------------------------------------------------------
AS

DECLARE
   OUTPUT      STRING DEFAULT '';
   l_query     STRING DEFAULT '';
   l_counter   NUMBER DEFAULT 0;
   l_error     NUMBER DEFAULT 0;
   l_type      STRING DEFAULT p_type;
   l_date_from STRING DEFAULT p_date_from;
   l_cur   CURSOR FOR SELECT classification_type,
                             created,
                             last_altered,
                             last_ddl,
                             database_name,
                             schema_name,
                             table_name,
                             table_type
                        FROM sensitive_objects_classification
                       WHERE classification_type = 'MNPI'
                         AND ((? = 'FULL')
                          OR (? = 'INCREMENTAL' AND created >= ?));
BEGIN
   CALL log_me(p_log_text  => 'Start MNPI classification p_type='     ||:p_type||
                                                      ', p_date_from='||:p_date_from||
                                                      ', p_unset='    ||:p_unset,
               p_log_level => 'INFO');

   OPEN l_cur USING (l_type, l_type, l_date_from);

   FOR rec IN l_cur DO
       LET l_full_table_name := '"'||rec.database_name||'".'||rec.schema_name||'.'||rec.table_name;

       l_query := 'ALTER '||rec.table_type||' '||l_full_table_name;

       IF (p_unset) THEN
         l_query := l_query ||' UNSET TAG MNPI_DATA;';
       ELSE
         l_query := l_query ||' SET TAG MNPI_DATA=\'yes\';';
       END IF;


       -- OUTPUT := OUTPUT || l_query|| '\n';
       l_counter := l_counter + 1;
       --------------------------------------------------------------------------------
       -- Separate execute immediate block to handle each command separately.
       -- If one command fails, the LOOP block will continue with the next command.
       --------------------------------------------------------------------------------
      BEGIN TRANSACTION;
      BEGIN

          EXECUTE IMMEDIATE l_query;
          CALL log_me(p_log_text => 'CLASSIFIED: '||:l_query,
                      p_log_level => 'INFO');
      EXCEPTION
          WHEN OTHER THEN
            l_error := l_error + 1;
            CALL log_me(p_log_text => 'ERROR: '   || :l_full_table_name ||
                                      ' SQLCODE:' || :sqlcode ||
                                      ' SQLERRM: '|| :sqlerrm ||
                                      ' SQLSTATE:'|| :sqlstate,
                        p_log_level => 'ERROR');

      END;
      COMMIT;
   END FOR;
   CLOSE l_cur;

   CALL log_me(p_log_text  => 'End classification p_type='     ||:p_type||
                                               ', p_date_from='||:p_date_from||
                                               ', p_unset='    ||:p_unset,
               p_log_level => 'INFO');

   RETURN 'CLASSIFIED: '||TO_VARCHAR(l_counter)||' ERROR_NO: '||TO_VARCHAR(l_error);

END;