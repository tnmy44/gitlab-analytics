"""
This module does the following:
- Reads in sql file that is Jinja templated
- Renders the Jinja template with the Snowflake username
- Converts the sql file to a series of sql statements
"""

import os
import sqlparse
from jinja2 import Template

PARENT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_template(filename: str) -> Template:
    """
    Return as Jinja Template based on filename of sql file
    """
    file_path = os.path.join(PARENT_DIR, "sql_templates/", filename)
    with open(file_path, "r", encoding="utf-8") as f:
        template = f.read()

    # Create a Jinja2 Template object
    return Template(template)


def render_template(template: Template, username: str) -> str:
    """Render the jinja template with the Snowflake username"""
    # Define variables to be used in the template
    variables = {"username": username}

    # Render the template with variables
    rendered_template = template.render(variables)
    return rendered_template


def convert_to_sql_statements(sql_file_contents: str) -> list:
    """
    The sql file is read in as one string
    Using sqlparse lib, this function converts the string into a series of sql statements
    """
    statements = sqlparse.split(sql_file_contents)
    return [str(statement) for statement in statements if statement.strip()]


def process_template(template: Template, username: str) -> list:
    """
    Renders the sql file with Jinja templating
    and converts the string to a list of sql statements
    """
    rendered_template = render_template(template, username)
    sql_statements = convert_to_sql_statements(rendered_template)
    return sql_statements
