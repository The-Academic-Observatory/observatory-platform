import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def retry_session(num_retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 503, 50)):
    session = requests.Session()
    retry = Retry(total=num_retries, connect=num_retries, read=num_retries, redirect=num_retries,
                  status_forcelist=status_forcelist, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
