import yaml
from fire import Fire


class YamlReader:

    def read_seed_schema(self):

        with open("../transform/snowflake-dbt/dbt_project.yml", "r") as stream:
            try:
                seed_schema = yaml.safe_load(stream).get('seeds').get('+schema')
                return seed_schema
            except yaml.YAMLError as exc:
                print(exc)


    def read_seed_name(self):
        with open("../transform/snowflake-dbt/data/seeds.yml", "r") as stream:
            try:
                seed_name = yaml.safe_load(stream).get('seeds')[0]['name']
                return seed_name
            except yaml.YAMLError as exc:
                print(exc)


if __name__ == "__main__":
    yaml_reader = YamlReader()
    Fire(yaml_reader)
