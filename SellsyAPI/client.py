from time import time, sleep
from functools import wraps
from requests import post, request, RequestException
from tqdm import tqdm
from .helpers import flatten_dict, expand_list_of_dicts_column,treat_custom_fields
import pandas as pd

class SellsyAPI:
    """A class to interact with the Sellsy API, handling authentication and requests."""

    def __init__(self,
                 client_id: str,
                 client_secret: str,
                 with_custom_fields: bool = False):
        """
        Initializes the API client with client credentials.

        Args:
            client_id (str): The client ID for Sellsy API authentication.
            client_secret (str): The client secret for Sellsy API authentication.
        """
        self.auth_url = "https://login.sellsy.com/oauth2/access-tokens"
        self.api_base_url = "https://api.sellsy.com/v2/"
        self.post_content_type = {
            "batch": "text/plain",
            "opportunities/search": "application/json",
            "individuals/search": "application/json",
            "invoices/search": "application/json",
            "credit-notes/search": "application/json",
            "estimates/search": "application/json",
            "comments/search": "application/json",
            "companies/search": "application/json",
            "contacts/search": "application/json"
        }
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token, self.token_expiry = self._request_new_token()
        self.with_custom_fields = with_custom_fields
        if self.with_custom_fields:
            self.fetch_custom_field_ids()

    def _get_access_token(self) -> str:
        """
        Retrieves the current access token, requesting a new one if it's expired.
        """
        if time() >= self.token_expiry:
            self.access_token, self.token_expiry = self._request_new_token()
        return self.access_token

    def _request_new_token(self) -> (str, float):
        """
        Requests a new access token from the Sellsy API.

        Returns:
            tuple: A tuple containing the access token and its expiry time.

        Raises:
            RuntimeError: If the request for a new token fails.
        """
        try:
            response = post(self.auth_url, data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }, timeout=10)  # Add timeout argument
            response.raise_for_status()
            res = response.json()
            expiry_time = time() + res["expires_in"]
            return res['access_token'], expiry_time
        except RequestException as e:
            raise RuntimeError(f"Failed to obtain access token: {e}") from e

    def _check_access_token(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self._get_access_token()  # Refresh the token if needed
            return func(self, *args, **kwargs)
        return wrapper

    @_check_access_token
    def _request(self, method: str, endpoint: str, params: dict = None, data: dict = None) -> dict:
        """
        Sends a GET or POST request to the Sellsy API.
        Args:
            method (str): The HTTP method to use ('get' or 'post').
            endpoint (str): The API endpoint to call.
            params (dict, optional): URL parameters for the request.
            data (dict, optional): Data to be sent in the request body.
        
        Returns:
            dict: The JSON response from the API.

        Raises:
            requests.HTTPError: For HTTP-related errors.
            ValueError: If the response body does not contain valid JSON.
        """
        MAX_RETRIES = 5
        params = params or {}
        params.setdefault('limit', 100)
        params.setdefault('order', 'id')
        params.setdefault('direction', 'asc')
        params.setdefault('embed[]',[f"cf.{cf_id}" for cf_id in self.custom_field_ids])

        data = data or {}
        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"{self.api_base_url}{endpoint}"
        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = request(method, url, headers=headers, params=params, data=data, timeout=10)  # Add timeout argument
                response.raise_for_status()
                return response.json()
            except RequestException as err:
                retries += 1
                sleep(2 ** retries)  # Exponential backoff
                if retries == MAX_RETRIES:
                    raise Exception(f"All retries failed for {endpoint}: {e}") from err

    def fetch_custom_field_ids(self) -> None:
        """
        Fetches all custom field IDs from the API and stores them in self.custom_field_ids.
        """
        self.custom_field_ids = []
        self.custom_field = []
        endpoint = "custom-fields"
        params = {"order": "id", "direction": "asc", "limit": 100}
        offset = 0

        while True:
            params["offset"] = offset
            response = self._request("get", endpoint, params=params)
            custom_fields = response.get('data', [])
            self.custom_field += custom_fields
            self.custom_field_ids.extend([field['id'] for field in custom_fields])

            # Pagination handling
            pagination_info = response.get('pagination', {})
            if pagination_info.get('count', 0) < params['limit']:
                break
            offset += params['limit']

    @_check_access_token
    def get(self, endpoint: str, params: dict) -> dict:
        """
        Sends a GET request to the specified API endpoint and iterates over all pages.

        Args:
            endpoint (str): The API endpoint to call.
            params (dict, optional): URL parameters for the GET request.

        Returns:
            list: The JSON responses from all pages of the API.
        """
        # Initialize the number of results expected
        response = self._request("get", endpoint, params={'limit': 100})
        all_data = response.get('data', [])
        all_data = pd.DataFrame([flatten_dict(d) for d in all_data])
        all_data['_embed_custom_fields'] = all_data['_embed_custom_fields'].apply(func=treat_custom_fields)
        all_data = expand_list_of_dicts_column(all_data)
        total_results = response.get('pagination', {}).get('total', 0)
        params['offset'] = response.get('pagination', {}).get('offset', 0)
        params['limit'] = 100
        
        with tqdm(total=total_results, desc="Downloading") as pbar:
            pbar.update(len(all_data))  # update the progress bar
            while len(all_data) < total_results:
                response = self._request("get", endpoint, params=params)
                data = response.get('data', [])
                data = pd.DataFrame([flatten_dict(d) for d in data])
                data['_embed_custom_fields'] = data['_embed_custom_fields'].apply(func=treat_custom_fields)
                data = expand_list_of_dicts_column(data)
                data.dropna(axis=1, inplace=True, how = 'all')
                all_data = pd.concat([all_data, data])
                pbar.update(len(data))  # update the progress bar
                pagination_info = response.get('pagination', {})
                params['offset'] = pagination_info['offset']

        return all_data
