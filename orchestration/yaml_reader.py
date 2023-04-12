import yaml
from fire import Fire


def read_seed_schema(dbt_project_path: str):
    """

    :param dbt_project_path:
    :return:
    """
    with open(dbt_project_path, "r") as stream:
        try:
            seed_schema = yaml.safe_load(stream).get("seeds").get("+schema")
            return seed_schema
        except yaml.YAMLError as exc:
            print(exc)


def read_seed_name(seed_file_path: str):
    """

    :param seed_file_path:
    :return:
    """
    with open(seed_file_path, "r") as stream:
        try:
            seed_name = yaml.safe_load(stream).get("seeds")[0]["name"]
            return seed_name
        except yaml.YAMLError as exc:
            print(exc)


if __name__ == "__main__":
    Fire()
