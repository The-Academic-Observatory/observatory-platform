import os
from pathlib import Path


def test_data_dir(current_file):
    """ Get the path to the Academic Observatory test data directory.

    :param current_file: pass the `__file__` handle from the test you are running.
    :return: the Academic Observatory test data directory.
    """

    path = str(Path(current_file).resolve())
    subdir = os.path.join('academic-observatory', 'tests')

    assert subdir in path, f"`{subdir}` not found in path `{path}`"

    start_i = path.find(subdir)
    end_i = start_i + len(subdir)
    tests_path = path[:end_i]
    return os.path.join(tests_path, 'data')
