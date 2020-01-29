import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Tuple


def retry_session(num_retries: int = 3, backoff_factor: float = 0.5,
                  status_forcelist: Tuple = (500, 502, 503, 50)) -> requests.Session:
    """ Create a session object that is able to retry a given number of times.
    :param num_retries: the number of times to retry.
    :param backoff_factor: the factor to use for determining how long to wait before retrying. See
    urllib3.util.retry.Retry for more details.
    :param status_forcelist: which integer status codes (that would normally not trigger a retry) should
    trigger a retry.
    :return: the session.
    """
    session = requests.Session()
    retry = Retry(total=num_retries, connect=num_retries, read=num_retries, redirect=num_retries,
                  status_forcelist=status_forcelist, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
