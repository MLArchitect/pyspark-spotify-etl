import requests
import base64
from dotenv import load_dotenv
import os
from utils.logger import get_logger

logger = get_logger()
load_dotenv()


def generate_temp_token(token_url: str):
    """function for internal usaage, connects to the spotify app and generates an access token, will be further used to call the api

    Args:
        token_url (str): the token url of your spotify app

    Returns:
        access_token: this access token will be used in the main api call function
    """

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    credentials = f"{client_id}:{client_secret}"
    client_creds_b64 = base64.b64encode(credentials.encode('ascii')).decode()
    token_data = {"grant_type": "client_credentials"}

    token_headers = {"Authorization": f"Basic {client_creds_b64}"}
    logger.info('calling post api to get the access token')
    req = requests.post(token_url, data=token_data, headers=token_headers)
    req.raise_for_status()
    try:
        token_response = req.json()
        access_token = token_response['access_token']
        logger.info(f'access token generate {access_token[:5]}')
        return access_token
    except Exception as e:
        logger.exception(f'unexpected error in creating access token {e}')
        raise
