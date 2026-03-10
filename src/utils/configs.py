import json
from utils.exceptions import MissingPipelineConfigs


def get_confs(data_layer, process_name):
    conf_path = f"src/{data_layer}/config.json"

    with open(conf_path, "r") as f:
        config_dict = json.load(f)

    process_dict = config_dict.get(process_name)

    if process_dict is None:
        raise MissingPipelineConfigs(
            f"Could not find configs for '{process_name}' at '{conf_path}'"
        )

    return process_dict