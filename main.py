from SellsyAPI.client import SellsyAPI
from project_secrets import client_id, client_secret

api_client = SellsyAPI(
    client_id = client_id,
    client_secret = client_secret,
    with_custom_fields = True
)

#for table in ['contacts', 'opportunities', 'companies', ]:


data = api_client.get('individuals', {})
data.dropna(axis=1, inplace=True, how = 'all')
data.to_csv('individuals-2.csv', index=False)
