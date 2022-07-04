import json
from unittest import result
import numpy
import pandas as pd
from multiprocessing import Pool
import multiprocessing


def revisar_dicc(data,  profession, nconst):
    #for dicc_data in data:
    for i in  profession:
        if data.get(nconst, False):
            data[nconst] = data[nconst].append(profession)
                 
        else:
            data[nconst]=[]
            data[nconst].append(profession) 


    return data


def main(df):
    array = []
    for row_personas in df.itertuples():
        print(row_personas.nconst)
        profession = row_personas.primaryProfession.split(',')
        data = {row_personas.nconst:profession}
        array.append(data)
            #data = pd.concat([data, pd.DataFrame.from_records([{'nconst': row_personas.nconst, 'profession': profession}])])

    return array



if __name__ == '__main__':

    personas = pd.read_csv('../data/name_basics.tsv', sep='\t', header=0, nrows=10)
    #titulos = pd.read_csv('data/title_basics.tsv', sep='\t', header=0, nrows=100000)

    cores = multiprocessing.cpu_count()
    personas_split = numpy.array_split(personas, cores)

    with multiprocessing.Pool(cores) as pool:
        results = pool.map(main, personas_split)

    results = numpy.concatenate(results)

    list_edges = []
    for i in results:
        for key, value in i.items():
            list_edges.append([key, value])

    df_export = pd.DataFrame(list_edges, columns=['nconst', 'rol'])
    #df_export= df_export.drop_duplicates()
    df_export.to_csv("rol.csv", index=False, sep='|')




