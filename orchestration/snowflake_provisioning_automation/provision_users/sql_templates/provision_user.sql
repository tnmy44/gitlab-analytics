-- assumes securityadmin role and admin warehouse are being used
set username = (select upper('{{ username }}'));

CREATE USER identifier($username);
CREATE ROLE identifier($username);

GRANT ROLE identifier($username) TO ROLE "SYSADMIN";
GRANT ROLE identifier($username) TO USER identifier($username);
