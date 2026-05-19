from retrying import retry  # type: ignore
from ratelimit import limits, sleep_and_retry  # type: ignore
import requests  # type: ignore
from utils.logger import get_logger

logger = get_logger()


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=60000)
@sleep_and_retry
@limits(calls=60, period=60)
def make_api_request(url: str, headers: dict, timeout=10):
    """Gernal function: makes call to any given urls, with any given headers

    Args:
        url (str): the url endpoint to make calls
        headers (dict): headers of the api call
        timeout (int, optional): time to wait for response. Defaults to 10 seconds.

    Returns:
        json: returns a response.json() object
    """
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.exception(f'error happened during the request: {e}')
        raise

    except Exception as e:
        logger.exception(f'unexpected error happend: {e}')
        raise

    finally:
        logger.info(f'a new get call to url: {url}')
