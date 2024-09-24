USE DATABASE raw;
USE SCHEMA data_classification;

CREATE OR REPLACE PROCEDURE log_me(p_log_text  VARCHAR DEFAULT '',
                                   p_log_level VARCHAR DEFAULT 'INFO')
RETURNS STRING
--------------------------------------------------------------------------------
-- Purpose: Procedure to log processing
-- Date: 2024-07-09
-- Author: rbacovic
-- Parameters:
--    - p_log_text - Text we should log
--    - p_level    - INFO, DEBUG, WARN, ERROR - level of logging
-- Version(s):
-- 0.1.0 - 2024-07-09: Initial version
-- 0.2.0 - 2024-08-01: Add user of the execution
--------------------------------------------------------------------------------
LANGUAGE SQL
AS
BEGIN
    BEGIN TRANSACTION;
    INSERT INTO log_classification(log_level, log_text) VALUES (:p_log_level, :p_log_text);
    COMMIT;
    RETURN 'OK';
END;