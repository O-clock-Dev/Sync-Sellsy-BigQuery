import pandas as pd

def flatten_dict(d, parent_key='', sep='_'):
    """
    Aplatit un dictionnaire en concaténant les clés des sous-dictionnaires.

    Args:
        d (dict): Dictionnaire à aplatir.
        parent_key (str, optional): Clé parente pour la concaténation. Par défaut vide.
        sep (str, optional): Séparateur entre les clés parentes et enfants. Par défaut '_'.

    Returns:
        dict: Dictionnaire aplati.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)



def find_label_by_id(data_dict, search_id):
    """
    Cherche et renvoie le label correspondant à un id donné dans un dictionnaire spécifique.
    
    Args:
        data_dict (dict): Dictionnaire contenant une liste de dictionnaires sous la clé 'items'.
        search_id (int): L'id pour lequel le label correspondant doit être trouvé.
    
    Returns:
        str or None: Le label correspondant à l'id donné, ou None si l'id n'est pas trouvé.
    """
    for item in data_dict.get('items', []):
        if item.get('id') == search_id:
            return item.get('label')
    return search_id

def treat_custom_fields(custom_field):
    return_list=[]
    mapping = [('é', 'e'), ('è', 'e'), ('ë', 'e'), ('ê', 'e'), ('ô', 'o'), ('(',''),(')','')]
    for item in custom_field:
        for k, v in mapping:
            item['name'] = item['name'].replace(k, v)
        name = item['name']
        
        
        if item['value'] in [0, '0', '']:
            value = None
        elif isinstance(item['value'], dict) and "amount" in item['value'] and "currency" in item['value']:
            if item['value']['amount'] not in [0,'0',None]:
                value = item['value']['amount'] + ' ' + item['value']['currency']
            else :
                value = None
        else :
            value = find_label_by_id(item['parameters'],item['value'])
            #value = item['value']
            if value == 'Inconnu' or value == 'N/C' or value == 'Aucun':
                value = None

        return_list.append({name:value})
    return return_list

def expand_list_of_dicts_column(df: pd.DataFrame, column_name='_embed_custom_fields') -> pd.DataFrame:
    def dict_list_to_df(dict_list):
        # S'assurer que chaque élément de la liste est un dictionnaire
        dict_list = [d for d in dict_list if isinstance(d, dict)]
        combined_dict = {k: v for d in dict_list for k, v in d.items()}
        return pd.Series(combined_dict)

    # Appliquer la fonction sur chaque ligne de la colonne spécifiée et créer un nouveau DataFrame
    expanded_df = df[column_name].apply(dict_list_to_df)

    # Joindre le nouveau DataFrame avec l'original en excluant la colonne transformée
    return df.drop(columns=[column_name]).join(expanded_df)
