import sys
from update_roles_yaml import main


def test_run_update_roles_yaml():
    """Check that update_roles_yaml.main() runs without errors"""
    original_argv = sys.argv
    # by default argparse checks sys.argv[1:], which in this case will be blank
    sys.argv = ["some_placeholder"]
    try:
        main()
    except:
        raise

    # Restore original sys.argv
    sys.argv = original_argv
