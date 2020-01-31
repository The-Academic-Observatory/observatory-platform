import os
from pathlib import Path


def ao_home(*subdirs) -> str:
    """Get the Academic Observatory home directory. If the home directory doesn't exist then create it.

    :return: the Academic Observatory home directory.
    """

    user_home = str(Path.home())
    ao_home_ = os.path.join(user_home, ".academic_observatory", *subdirs)

    if not os.path.exists(ao_home_):
        os.makedirs(ao_home_, exist_ok=True)

    return ao_home_
