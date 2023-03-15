from os import environ as env


def get_affinity_with_key_value(key, values):
    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {"key": key, "operator": "In", "values": values}
                        ]
                    }
                ]
            }
        }
    }


def get_toleration_with_value(value):
    return [
        {"key": value, "operator": "Equal", "value": "true", "effect": "NoSchedule"}
    ]


scd_affinity = get_affinity_with_key_value("pgp", ["scd"])

scd_tolerations = get_toleration_with_value("scd")

test_affinity = get_affinity_with_key_value("test", ["true"])

test_tolerations = get_toleration_with_value("test")

production_affinity = get_affinity_with_key_value("production", ["true"])

production_tolerations = get_toleration_with_value("production")

data_science_affinity = get_affinity_with_key_value("data_science", ["true"])

data_science_tolerations = get_toleration_with_value("data_science")


def is_local_test():
    return "NAMESPACE" in env and env["NAMESPACE"] == "testing"


def get_affinity(is_scd, is_data_science):
    if is_local_test():
        return test_affinity
    if is_scd:
        return scd_affinity
    if is_data_science:
        return data_science_affinity
    return production_affinity


def get_toleration(is_scd, is_data_science):
    if is_local_test():
        return test_tolerations
    if is_scd:
        return scd_tolerations
    if is_data_science:
        return data_science_tolerations
    return production_tolerations
