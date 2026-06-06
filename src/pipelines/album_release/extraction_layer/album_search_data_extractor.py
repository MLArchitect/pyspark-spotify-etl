# create a file that every day, gets the album releases of the last 7 days
from datetime import datetime, timedelta
from utils.config import base_rul, new_releases_endpoint
from utils.logger import get_logger
import json
from src.general_functions.parser import make_api_request

logger = get_logger()


def ingestion(path: str, days: int, access_token: str) -> json:  # type: ignore
    """makes calls to the "new_releases" endpoint of the spotify app and returns a json file per function call

    Args:
        path (str): path to ingest the json file, received from the api call
        days (int): the `new releases endpoint` accepts a parameter to retrieve the newest albums in the last n days
        access_token (str): needs to be generated in the previous steps

    Returns:
        json: a json file gets created in the given path with every call of the function
    """

    last_n_days = (datetime.now() - timedelta(days)).date().isoformat()
    results = []

    headers = {"Authorization": f"Bearer {access_token}"}

    url = f'{base_rul}/{new_releases_endpoint}'
    logger.info(f'starting to call the api endpoint at: {url}')

    try:
        while url:

            data = make_api_request(url, headers)
            for album in data.get('albums', {}).get('items', []):
                print(f"{album['name']} — {album['release_date']}")
                if album['release_date'] >= last_n_days:
                    results.append(album)
            url = data.get('albums', {}).get('next')
            logger.debug(f'next url to inspect: {url}')

        # print(len(results))
        logger.info(f'detected {len(results)} new results')
        try:
            with open(f'{path}/new_albums_{last_n_days}.json', 'w') as output_file:
                json.dump(results, output_file, indent=2)

        except Exception as e:
            logger.exception(f'error in writing the data to dis: {e}')
            raise

    except Exception as e:
        logger.exception(f'unexpected error happened: {e}')
        raise


# if __name__ == '__main__':
#     from src.access_token_generator import generate_temp_token
#     from utils.config import token_url
#     access_token = generate_temp_token(token_url)
#     ingestion(5, access_token)
