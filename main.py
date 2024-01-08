from SellsyAPI.client import SellsyAPI
from project_secrets import client_id, client_secret

api_client = SellsyAPI(
    client_id = client_id,
    client_secret = client_secret,
    with_custom_fields = True
)

#for table in ['contacts', 'opportunities', 'companies', ]:


companies = api_client.get('individuals', {})
companies.dropna(axis=1, inplace=True, how = 'all')
companies.to_csv('individuals-2.csv', index=False)