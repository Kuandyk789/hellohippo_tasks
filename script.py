import os
import pandas as pd
from pandasql import sqldf
import json

claims_dir = './claims/'
claims_columns = {
    'id': 'string',
    'npi': 'string',
    'ndc': 'string',
    'price': 'float64',
    'quantity': 'float64',
    'timestamp': 'datetime64[ns]'
}
pharmacies_dir = './pharmacies/'
pharmacies_columns = {
    'npi': 'string',
    'chain': 'string'
}
reverts_dir = './reverts/'
reverts_columns = {
    'id': 'string',
    'claim_id': 'string',
    'timestamp': 'datetime64[ns]'
}

def json_to_df(dir,column_types):
    data = []
    for file_name in os.listdir(dir):
        file_path = os.path.join(dir, file_name)
        
        if file_name.endswith('.json'):
            df = pd.read_json(file_path,dtype=column_types)
        elif file_name.endswith('.csv'):
            df = pd.read_csv(file_path,dtype=column_types)

        df = df[column_types.keys()]
        df = df.astype(column_types)
        data.append(df)

    all_data = pd.concat(data, ignore_index=True)
    return all_data

claims = json_to_df(claims_dir,claims_columns)
pharmacies = json_to_df(pharmacies_dir,pharmacies_columns)
reverts = json_to_df(reverts_dir,reverts_columns)

##########################################################################
task2_query = """
    SELECT claims.npi as npi
    ,claims.ndc as ndc
    ,count(claims.id) as fills
    ,count(reverts.id) as reverted
    ,avg(claims.price) as avg_price
    ,sum(claims.price) as total_price
    FROM claims
    LEFT JOIN reverts ON claims.id = reverts.claim_id
    group by npi, ndc
"""
task2_result = sqldf(task2_query)
#print(task2_result)
task2_result.to_json('./task2.json', orient='records', indent=4)

##########################################################################
task3_query = """
    SELECT ndc, chain, avg_price
    FROM(
        SELECT claims.ndc as ndc
        ,pharmacies.chain as chain
        ,sum(claims.price)/sum(claims.quantity) as avg_price
        ,ROW_NUMBER() OVER (PARTITION BY claims.ndc ORDER BY SUM(claims.price) / SUM(claims.quantity)) as row
        FROM claims
        JOIN pharmacies ON claims.npi = pharmacies.npi
        group by ndc, chain
        order by ndc, avg_price
    ) as a
    WHERE row < 3
"""
result = (
    sqldf(task3_query).groupby('ndc')
    .apply(lambda group: {
        "ndc": group['ndc'].iloc[0],
        "chain": [
            {"name": row['chain'], "avg_price": row['avg_price']}
            for _, row in group.iterrows()
        ]
    })
    .tolist()
)
task3_result = json.dumps(result, indent=4)
#print(task3_result)
with open('task3.json', 'w') as file:
    file.write(task3_result)

##########################################################################
task4_query = """
SELECT ndc,quantity as most_prescribed_quantity
FROM(
    SELECT ndc, quantity, count(quantity) as count_quant
    ,ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY count(quantity) DESC) as row
    FROM claims
    group by ndc, quantity
    order by ndc, count_quant desc
) as a
WHERE row <= 5
"""
task4_result = sqldf(task4_query).groupby('ndc')['most_prescribed_quantity'].apply(list).reset_index()
#print(task4_result)
task4_result.to_json('./task4.json', orient='records', indent=4)
