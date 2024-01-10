from pandas import to_datetime
from SellsyAPI.client import SellsyAPI
from project_secrets import client_id, client_secret
import os

api_client = SellsyAPI(
    client_id = client_id,
    client_secret = client_secret,
    with_custom_fields = True
)
mapper = [ #client, prospect, contact, opportunity, document, supplier, purchase
    ['individuals', 'prospect'],
    #['opportunities', 'opportunity'],
    #['contacts', 'contact'],
    #['individuals', 'client']
]

for endpoint, custom_fields in mapper:
    print(f'{endpoint} - {custom_fields}')

    # Vérifier l'existence d'un fichier précédent
    existing_file = f'{endpoint}-{custom_fields}-*.csv'
    matching_files = [f for f in os.listdir('.') if f.startswith(existing_file)]
    if matching_files:
        # Prendre le dernier fichier (supposé le plus récent)
        latest_file = sorted(matching_files)[-1]
        # Extraire la pagination du nom de fichier
        pagination = latest_file.split('-')[-1].replace('.csv', '')
    else:
        pagination = None

    data, pagination= api_client.get(
        endpoint=endpoint,
        params={},
        custom_fields=custom_fields,
        pagination=pagination
    )
    for item in ['due_date', 'created', 'created_at', 'birth_date', 'Date de Rituel choisie', 'updated_status', 'updated_at', "Date d'admissibilite", 'Date de naissance']:
        try:
            data[item] = to_datetime(data[item], format='ISO8601', utc=True)
        except:
            pass

    data.to_csv(f'{endpoint}-{custom_fields}-{pagination}.csv', index=False)
