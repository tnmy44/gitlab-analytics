from TableauConMan import execute
from fire import Fire


if __name__ == "__main__":
    Fire(
        {
            "migrate_content": execute.migrate_content,
            "provision_settings": execute.provision_settings
        }
    )
