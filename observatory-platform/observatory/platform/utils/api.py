import logging

from airflow.hooks.base import BaseHook
from observatory.platform.utils.airflow_utils import AirflowConns


def make_observatory_api() -> "ObservatoryApi":  # noqa: F821
    """Make the ObservatoryApi object, configuring it with a host and api_key.

    :return: the ObservatoryApi.
    """

    try:
        from observatory.api.client.api.observatory_api import ObservatoryApi
        from observatory.api.client.api_client import ApiClient
        from observatory.api.client.configuration import Configuration
    except ImportError as e:
        logging.error("Please install the observatory-api Python package to use the make_observatory_api function")
        raise e

    # Get connection
    conn = BaseHook.get_connection(AirflowConns.OBSERVATORY_API)

    # Assert connection has required fields
    assert (
        conn.conn_type != "" and conn.conn_type is not None
    ), f"Airflow Connection {AirflowConns.OBSERVATORY_API} conn_type must not be None"
    assert (
        conn.host != "" and conn.host is not None
    ), f"Airflow Connection {AirflowConns.OBSERVATORY_API} host must not be None"

    # Make host
    host = f'{str(conn.conn_type).replace("_", "-").lower()}://{conn.host}'
    if conn.port:
        host += f":{conn.port}"

    # Only api_key when password present in connection
    api_key = None
    if conn.password != "" and conn.password is not None:
        api_key = {"api_key": conn.password}

    # Return ObservatoryApi
    config = Configuration(host=host, api_key=api_key)
    api_client = ApiClient(config)
    return ObservatoryApi(api_client=api_client)
