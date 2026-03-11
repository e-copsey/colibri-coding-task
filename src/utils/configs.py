import json
from utils.exceptions import MissingPipelineConfigs


def get_confs(data_layer, process_name):
    """
    Retrieves the configuration dictionary for a specific process from the JSON config file of the given data layer, raising an exception if not found.
    """

    conf_path = f"src/{data_layer}/config.json"

    with open(conf_path, "r") as f:
        config_dict = json.load(f)

    process_dict = config_dict.get(process_name)

    if process_dict is None:
        raise MissingPipelineConfigs(
            f"Could not find configs for '{process_name}' at '{conf_path}'"
        )

    return process_dict