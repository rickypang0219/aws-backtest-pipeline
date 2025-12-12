import requests
import logging

from typing import Any
from aws_lambda_powertools.utilities.parameters import get_parameter
from data_processing.shared.logger import logger


class HyblockClient:
    def __init__(self) -> None:
        self.client_id = get_parameter("hyblock-client-id", decrypt=True)
        self.client_secret = get_parameter("hyblock-client-secret", decrypt=True)
        self.api_key = get_parameter("hyblock-api-key", decrypt=True)
        self.auth_url = "https://auth-api.hyblockcapital.com/oauth2/token"
        self.base_url = "https://api.hyblockcapital.com/v2"
        self.access_token = None

    def update_access_token(self) -> str:
        auth_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        try:
            response = requests.post(
                self.auth_url,
                data=auth_data,
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            token_data = response.json()
            if "access_token" not in token_data:
                raise Exception(
                    f"Access token not found in response: {token_data}"
                ) from None
            self.access_token = token_data["access_token"]
            logging.info("Access token retrieved %s", f"{self.access_token[:10]}..")
            return self.access_token
        except requests.RequestException as e:
            logging.error("Authentication request failed %s", e)
            raise Exception(f"Authentication failed: {e!s}") from e
        except ValueError as e:
            logging.error("Invalid JSON response %s", e)
            raise Exception(f"Invalid JSON response: {e!s}") from e

    def _get_request(
        self, endpoint: str, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        if not self.access_token:
            self.update_access_token()
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
        }
        response = None
        try:
            response = requests.get(
                f"{self.base_url}{endpoint}",
                headers=headers,
                params=payload,
                timeout=60,
            )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            if response is None:
                logger.error(
                    "No response received for HTTP error for endpoint %s with payload %s: %s",
                    endpoint,
                    payload,
                    e,
                )
                raise Exception(f"HTTP Error: {e!s}") from e
            if response.status_code == 429:
                logger.error(
                    "Rate limit exceeded (429) for endpoint %s with payload %s",
                    endpoint,
                    payload,
                )
                raise Exception(f"Rate limit exceeded: {e!s}") from e
            if response.status_code >= 500:
                logger.error(
                    "Server error (%s) for endpoint %s with payload %s",
                    response.status_code,
                    endpoint,
                    payload,
                )
                raise Exception(f"Server error: {e!s}") from e
            if response.status_code >= 400:
                logger.error(
                    "Client error (%s) for endpoint %s with payload %s",
                    response.status_code,
                    endpoint,
                    payload,
                )
                return None  # Do not raise for 400-level errors (e.g., 404)
            logger.error("Unexpected HTTP error %s for endpoint %s", e, endpoint)
            raise Exception(f"HTTP Error: {e!s}") from e
