import json
from unittest import result
import numpy
import pandas as pd
from multiprocessing import Pool
import multiprocessing


def revisar_dicc(data, profession, nconst):
    flag = False
    for dicc_data in data:
        if dicc_data['profession'] == profession:
            dicc_data['names'].append(nconst)
            flag = True
            break

    if flag == False:
        data.append({'profession':profession, 'names':[nconst]})
 

    return data


def main(df):
    #data = [{'profession':'id', 'names':[]}]
    data = pd.DataFrame(columns=['nconst','profession'])
    for row_personas in df.itertuples():
        print(row_personas.nconst)
        for profession in str(row_personas.primaryProfession).split(','):
            #data = revisar_dicc(data, profession, row_personas.nconst)
            data = pd.concat([data, pd.DataFrame.from_records([{'nconst': row_personas.nconst, 'profession': profession}])])

    return data



if __name__ == '__main__':

    personas = pd.read_csv('../data/name_basics.tsv', sep='\t', header=0)
    #titulos = pd.read_csv('data/title_basics.tsv', sep='\t', header=0, nrows=100000)

    cores = multiprocessing.cpu_count()
    personas_split = numpy.array_split(personas, cores)

    with multiprocessing.Pool(cores) as pool:
        results = pool.map(main, personas_split)
    
    data = pd.concat(results)
 
    data.to_csv(path_or_buf='professions.csv', index=False)
    #with open('professions.json', 'w') as file:
        #json.dump(results, file, indent=4)




