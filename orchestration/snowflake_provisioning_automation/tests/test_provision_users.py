import sys
from provision_users import main


def test_run_provision_users():
    """Check that provision_users.main() runs without errors"""
    original_argv = sys.argv
    # by default argparse checks sys.argv[1:], which in this case will be blank
    sys.argv = ["some_placeholder"]
    try:
        main()
    except:
        raise

    # Restore original sys.argv
    sys.argv = original_argv
