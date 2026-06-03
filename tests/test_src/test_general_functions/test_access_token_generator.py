import pytest
from unittest.mock import patch, Mock, MagicMock
import requests
from src.general_functions.access_token_generator import generate_temp_token


def test_generate_temp_token_http_401_error():
    """Test token generation when Spotify returns 401 Unauthorized error"""

    # Arrange
    test_token_url = "https://accounts.spotify.com/api/token"

    # Create a mock response that simulates a 401 error
    mock_response = Mock()
    # TBD: Make raise_for_status() raise an HTTPError with message "401 Client Error: Unauthorized"
    mock_response.raise_for_status.side_effect = [
        requests.HTTPError("401 Client Error: Unauthorized")]

    with patch('src.general_functions.access_token_generator.os.getenv') as mock_getenv, \
            patch('src.general_functions.access_token_generator.requests.post') as mock_post:

        # TBD: Configure mock_getenv to return invalid credentials
        # It should return "bad_client_id" for client_id and "bad_secret" for client_secret
        mock_getenv.side_effect = lambda key: {
            "client_id": "bad_client_id",
            "client_secret": "bad_secret"
        }.get(key)

        # TBD: Make requests.post return our mock_response
        # YOUR CODE HERE
        mock_post.return_value = mock_response

        # Act & Assert
        # TBD: Use pytest.raises to verify that calling generate_temp_token raises an HTTPError
        # YOUR CODE HERE
        with pytest.raises(requests.HTTPError) as exc_info:
            generate_temp_token(test_token_url)
        # Optional: verify the error message
        assert "401 Client Error: Unauthorized" in str(exc_info.value)
        # TBD: Verify that requests.post was called exactly once
        # YOUR CODE HERE
        mock_post.assert_called_once()


def test_generate_temp_token_token_generation():
    """ Happy Path: when everything goes well and token gets generated"""
    token_url = "https://some_path.com"
    mock_response = Mock()

    with patch('src.general_functions.access_token_generator.os.getenv') as mock_getenv, \
            patch("src.general_functions.access_token_generator.requests.post") as mocked_requests_post:
        mock_getenv.side_effect = lambda key: {
            "client_id": "valid_client_id",
            "client_secret": "valid_secret"
        }.get(key)

        mock_response.status_code = 200
        mock_response.json.return_value = {'access_token': 'xxxxxxxx'}
        mocked_requests_post.return_value = mock_response

        result = generate_temp_token(token_url)
        assert result == 'xxxxxxxx'
