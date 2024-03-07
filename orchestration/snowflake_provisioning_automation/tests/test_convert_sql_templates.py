""" Test convert_sql_templates.py module """

from jinja2 import Template

from convert_sql_templates import render_template, convert_to_sql_statements


def get_template():
    """helper function to return jinja-templated sql statements"""
    sql_str = """
    -- some comment
    set username = (select upper('{{ username }}'));
    DROP USER IF EXISTS identifier($username);
    """
    return sql_str


def test_render_template_for_username():
    """
    Test that after rendering the jinja template with the username
    that we get the expected value
    """
    sql_str = get_template()
    template = Template(sql_str)
    username = "some_user1"

    expected_rendered_template = f"""
    -- some comment
    set username = (select upper('{username}'));
    DROP USER IF EXISTS identifier($username);
    """
    rendered_template = render_template(template, username)
    assert rendered_template == expected_rendered_template


def test_convert_to_sql_statements():
    """
    Test that from the jinja-templated sql statements
    we can obtain the expected individual sql statemnts
    """

    sql_str = get_template()
    template = Template(sql_str)
    username = "some_user1"

    rendered_template = render_template(template, username)
    sql_statements = convert_to_sql_statements(rendered_template)

    expected_sql_statement1 = f"""
    -- some comment
    set username = (select upper('{username}'));
    """
    expected_sql_statement2 = """
    DROP USER IF EXISTS identifier($username);
    """

    assert sql_statements[0].strip() == expected_sql_statement1.strip()
    assert sql_statements[1].strip() == expected_sql_statement2.strip()
