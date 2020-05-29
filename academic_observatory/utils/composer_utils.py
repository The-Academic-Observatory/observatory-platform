import os
from academic_observatory.utils.config_utils import ObservatoryConfig


def indent(string: str, num_spaces: int) -> str:
    return string.rjust(len(string) + num_spaces)


def gen_config_file():
    print("Generating config.yaml...")
    config = ObservatoryConfig.make_default()
    config_path = ObservatoryConfig.HOST_DEFAULT_PATH

    config.save(config_path)
    print(f'config.yaml saved to: "{config_path}"')
    print(f'Please customise the following parameters in config.yaml:')
    params = config.to_dict()
    for key, val in params.items():
        if val is None:
            print(f'  - {key}')


def config_path_valid(config_airflow_variable):
    # Indentation variables
    indent1 = 2
    indent2 = 3
    indent3 = 4

    print(indent("config.yaml:", indent1))

    try:
        config_path = config_airflow_variable
    except KeyError:
        print(indent("- 'CONFIG_FILE' airflow variable not set, please set in UI", indent2))
        return False

    config_valid, config_validator, config = ObservatoryConfig.load(config_path)
    config_exists = os.path.exists(config_path)
    if config_exists:
        print(indent(f"- path: {config_path}", indent2))

        if config_valid:
            print(indent("- file valid", indent2))
            return True
        else:
            print(indent("- file invalid", indent2))
            for key, value in config_validator.errors.items():
                print(indent(f'- {key}: {", ".join(value)}', indent3))
    else:
        print(indent("- file not found, generating a default file", indent2))
        gen_config_file()
    return False


