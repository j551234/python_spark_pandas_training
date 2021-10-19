import pandas as pd


def get_category(file_name):
    df = pd.read_json(file_name)
    data = pd.json_normalize(df['items'])
    return data['id','snippet.title']


def get_rap_info(file_name):
    df = pd.read_csv(file_name)
    rap_df = df[df.tags.str.contains("rap")]
    result = rap_df[['title', 'dislikes']]
    return result


result = get_category('./archive/CA_category_id.json')

print(result)
