import json
from time import process_time_ns
import pandas as pd


def revisar_dicc(data, titulo, nconst):
    flag = False
    for dicc_data in data:
        if dicc_data['tconst'] == titulo:
            dicc_data['elenco'].append(nconst)
            flag = True
            break

    if flag == False:
        data.append({'tconst':titulo, 'elenco':[nconst]})
 

    return data

personas = pd.read_csv('data/name_basics.tsv', sep='\t', header=0)
titulos = pd.read_csv('data/title_basics.tsv', sep='\t', header=0)


data = [{'tconst':'id', 'elenco':[]}]

for row_personas in personas.itertuples():
    for titulo in row_personas.knownForTitles.split(','):
        data = revisar_dicc(data, titulo, row_personas.nconst)

with open('elencos.json', 'w') as file:
    json.dump(data, file, indent=4)




