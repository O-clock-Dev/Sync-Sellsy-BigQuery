from time import time, sleep
from functools import wraps
from requests import post, request, RequestException, HTTPError
from tqdm import tqdm
from .helpers import flatten_dict
import pandas as pd
import json

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
    def _simple_request(self, method: str, endpoint: str, params: dict = None, data: dict = None) -> dict:

        MAX_RETRIES = 5
        params = params or {}
        params.setdefault('limit', 100)
        params.setdefault('order', 'id')
        params.setdefault('direction', 'asc')

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
        Fetches all custom field IDs and their related objects from the API and stores them in self.custom_fields.
        """
        self.custom_fields = {}
        endpoint = "custom-fields"
        params = {"order": "id", "direction": "asc", "limit": 100}
        offset = 0

        while True:
            params["offset"] = offset
            response = self._simple_request("get", endpoint, params=params)
            custom_fields_data = response.get('data', [])

            for field in custom_fields_data:
                for related_object in field.get("related_objects", []):
                    if related_object not in self.custom_fields:
                        self.custom_fields[related_object] = []
                    self.custom_fields[related_object].append(field['id'])

            # Pagination handling
            pagination_info = response.get('pagination', {})
            if pagination_info.get('count', 0) < params['limit']:
                break
            offset += params['limit']

    @_check_access_token
    def get(self, endpoint: str, params: dict, custom_fields=None, pagination=None) -> dict:

        # initial informations
        MAX_RETRIES = 5
        CUSTOM_FIELDS_PER_REQUEST = 300

        params = params or {}
        params.setdefault('limit', 100)
        params.setdefault('order', 'created_at')
        params.setdefault('direction', 'desc')

        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"{self.api_base_url}{endpoint}"

        # Determine the entity type from the endpoint
        # client, prospect, contact, opportunity, document, supplier, purchase
        custom_field_ids = self.custom_fields.get('client', [])
        for item in ['prospect', 'contact', 'opportunity', 'document', 'supplier', 'purchase']:
            custom_field_ids.extend(self.custom_fields.get(item, []))

        # Start of script
        # Si la pagination est fournie, l'utiliser pour initialiser le paramètre 'offset'
        if pagination is not None:
            params['offset'] = pagination

        # Fetch initial batch of data
        params['embed[]'] = [f"cf.{cf_id}" for cf_id in custom_field_ids[:CUSTOM_FIELDS_PER_REQUEST]]
        response = request(method='get', url=url, headers=headers, params=params)
        response = response.json()

        # Get Pagination parameters
        total_results = int((response.get('pagination', {}).get('total', 0))/4)
        #total_results = 300

        # Fetch additional custom fields in batches and merge them with initial items
        for i in range(CUSTOM_FIELDS_PER_REQUEST, len(custom_field_ids), CUSTOM_FIELDS_PER_REQUEST):
            batch_custom_fields = custom_field_ids[i:i+CUSTOM_FIELDS_PER_REQUEST]
            params['embed[]'] = [f"cf.{cf_id}" for cf_id in batch_custom_fields]

            additional_data = request('get', url, headers=headers, params=params, timeout=10)
            additional_data = additional_data.json()

            # Merge custom fields into each item

            for additional_item in additional_data['data']: # obligé de faire ça au cas où il y a i un écalage dans l'ordre de réponse de Sellsy
                if additional_item['_embed']['custom_fields'] is not None :
                    for original_data in response['data']:
                        if original_data['id']==additional_item['id'] and original_data['created']==additional_item['created']:
                            original_data['_embed']['custom_fields'].extend(additional_item['_embed']['custom_fields'])
                            break

        raw_data = [flatten_dict(d) for d in response.get('data', [])]
        for client in raw_data:
            for cf in client['_embed_custom_fields']:
                if cf['value'] is not None:
                    for item in cf['parameters'].get('items', []):
                        if item['id'] == cf['value']:
                            client[cf['name']] = item['label']
                            break
            del client['_embed_custom_fields']
        all_data = pd.DataFrame(raw_data)
        all_data.dropna(axis=1, inplace=True, how = 'all')
        params['offset'] = response.get('pagination', {}).get('offset', 0)

        with tqdm(total=total_results, desc=f"Downloading {endpoint}-{custom_fields}") as pbar:
            pbar.update(len(response.get('data', [])))  # update the progress bar
            while len(all_data) < total_results:
                retries = 0
                try:
                    params['embed[]'] = [f"cf.{cf_id}" for cf_id in custom_field_ids[:CUSTOM_FIELDS_PER_REQUEST]]
                    response = request(method='get', url=url, headers=headers, params=params).json()
                    # get custom fields
                    for i in range(CUSTOM_FIELDS_PER_REQUEST, len(custom_field_ids), CUSTOM_FIELDS_PER_REQUEST):
                        batch_custom_fields = custom_field_ids[i:i+CUSTOM_FIELDS_PER_REQUEST]
                        params['embed[]'] = [f"cf.{cf_id}" for cf_id in batch_custom_fields]
                        additional_data = request('get', url, headers=headers, params=params).json()

                        for additional_item in additional_data['data']: # obligé de faire ça au cas où il y a i un écalage dans l'ordre de réponse de Sellsy
                            if additional_item['_embed']['custom_fields'] is not None :
                                for original_data in response['data']:
                                    if original_data['id']==additional_item['id'] and original_data['created']==additional_item['created']:
                                        original_data['_embed']['custom_fields'].extend(additional_item['_embed']['custom_fields'])
                                        break

                except :
                    retries += 1
                    print(f'error - doing {retries} retry')
                    sleep(2 ** retries)  # Exponential backoff
                    if retries == MAX_RETRIES:
                        print(f"error on the {params['offset']} page of {endpoint}")
                        return all_data, pagination_info['offset']                   

                raw_data = [flatten_dict(d) for d in response.get('data', [])]
                for client in raw_data:
                    for cf in client['_embed_custom_fields']:
                        if cf['value'] is not None:
                            for item in cf['parameters'].get('items', []):
                                if item['id'] == cf['value']:
                                    client[cf['name']] = item['label']
                                    break
                    del client['_embed_custom_fields']
                raw_data = pd.DataFrame(raw_data)
                raw_data.dropna(axis=1, inplace=True, how = 'all')
                all_data = pd.concat([all_data, raw_data])

                pbar.update(len(response.get('data', [])))  # update the progress bar
                pagination_info = response.get('pagination', {})
                if pagination_info['offset']:
                    params['offset'] = pagination_info['offset']
                else:
                    print('End of pagination')
                    break
 
        print(f"Stoped at the pagination index : {pagination_info['offset']}")
        return all_data, pagination_info['offset']
