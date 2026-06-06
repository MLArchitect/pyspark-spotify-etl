from unittest.mock import Mock, patch
from src.general_functions.parser import make_api_request
import requests
import pytest


@patch("src.general_functions.parser.requests.get")
def test_make_api_request_happy_path(req_get):
    """Happy path test """
    mock_response = Mock()
    mock_response.json.return_value = {"some_key": 12}
    req_get.return_value = mock_response

    result = make_api_request('https://abc.com', {"a": "b"}, 10)
    assert result == {"some_key": 12}


# testing assertions
@patch("time.sleep", return_value=None)
@patch("src.general_functions.parser.requests.get")
def test_make_api_request_timeout(req_get, mock_sleep):
    req_get.side_effect = requests.exceptions.Timeout()
    with pytest.raises(requests.exceptions.Timeout):
        make_api_request('https://abc.com', {"a": "b"}, 10)
    req_get.assert_called_with(
        'https://abc.com', headers={"a": "b"}, timeout=10)

# testing the general exception function


@patch("time.sleep", return_value=None)
@patch("src.general_functions.parser.requests.get")
def test_make_api_request_exception(req_get, mock_sleep):
    req_get.side_effect = Exception("unexpected error happend")
    with pytest.raises(Exception) as exc_info:
        make_api_request('https://abc.com', {"a": "b"}, 10)
    assert "unexpected error happend" in str(exc_info.value)

# testing the logger call


@patch("time.sleep", return_value=None)
@patch("src.general_functions.parser.logger")
@patch("src.general_functions.parser.requests.get")
def test_make_api_request_logger_call(req_get, mocked_logger, mock_sleep):
    req_get.side_effect = Exception("unexpected error happend")
    with pytest.raises(Exception) as exc_info:
        make_api_request('https://abc.com', {"a": "b"}, 10)
    mocked_logger.exception.assert_called()

# testing retry happens


@patch("time.sleep", return_value=None)
@patch("src.general_functions.parser.requests.get")
def test_make_api_request_retries_5_times(req_get, mock_sleep):
    req_get.side_effect = Exception("something happened")
    with pytest.raises(Exception) as _:
        make_api_request('https://abc.com', {"a": "b"}, 10)

    assert req_get.call_count == 5

# testing retry with different values


@patch("time.sleep", return_value=None)
@patch("src.general_functions.parser.requests.get")
def test_make_api_request_retry_succeeds_on_third_attempt(req_get, mock_sleep):
    mock_response = Mock()
    mock_response.json.return_value = {"key": "value"}
    req_get.side_effect = [
        Exception(),
        requests.ConnectionError(),
        mock_response  # success at 3rd attempt
    ]
    result = make_api_request('https://abc.com', {"a": "b"}, 10)
    assert result == {"key": "value"}
    assert req_get.call_count == 3
    assert mock_sleep.call_count == 2
