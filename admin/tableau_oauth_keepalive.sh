## Assumes tableau is installed. Will add CLIENT_SESSION_KEEP_ALIVE = True 
## to /opt/snowflake/snowflake-odbc/Setup/odbc.ini to prevent frequent OAUTH
## refresh prompt

# Check for sudo
if [[ $(id -u) -ne 0 ]]; then
  echo "This script requires administrator privileges. Please re-run with sudo."
  exit 1
fi

ODBC_INI_FILE="/opt/snowflake/snowflakeodbc/Setup/odbc.ini"

# Ensure file exists
if [[ ! -f "$ODBC_INI_FILE" ]]; then
  echo "Configuration file not found at: $ODBC_INI_FILE"
  exit 1
fi

# Check if the line exists, if not, add it under the correct section
if ! grep -q "^CLIENT_SESSION_KEEP_ALIVE" $ODBC_FILE; then
  # Insert the line under the [SnowflakeDSII] section, followed by a newline
  sed -i.bak "/^\[SnowflakeDSII\]/a \\
$KEEP_ALIVE_LINE\\
" $ODBC_FILE 

  echo "CLIENT_SESSION_KEEP_ALIVE setting added to $ODBC_FILE"
else
  echo "CLIENT_SESSION_KEEP_ALIVE setting already exists in $ODBC_FILE"
fi 
