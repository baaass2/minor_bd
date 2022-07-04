import json
import pandas as pd
import multiprocessing
import numpy
import csv


def revisar_dicc(data, titulo, nconst):

    for i in titulo:
        if data.get(i, False) == False:
            data[i]=[]
            data[i].append(nconst)
                 
        else:
            elenco = data[i]
            elenco.append(nconst)
            data[i] = elenco
 


    return data

def main(personas):
    data = {'tconst':[]}

    for row_personas in personas.itertuples():
        #print(row_personas.nconst)
        titulo =  row_personas.knownForTitles.split(',')
        data = revisar_dicc(data, titulo, row_personas.nconst)

    return data
if __name__ == '__main__':

    personas = pd.read_csv('../data/name_basics.tsv', sep='\t', header=0, nrows=1000000)
    #titulos = pd.read_csv('../data/title_basics.tsv', sep='\t', header=0, nrows=1000)

    
    cores = multiprocessing.cpu_count()
    personas_split = numpy.array_split(personas, cores)

    with multiprocessing.Pool(cores) as pool:
        results = pool.map(main, personas_split)
    #print(results)
    list_edges = []
    for i in results:
        for key, value in i.items():
            list_edges.append([key, str(value)])

    df_export = pd.DataFrame(list_edges, columns=['tconst', 'elenco'])
    df_export = df_export.drop_duplicates()
    df_export.to_csv("elencos.csv", index=False,  sep='|')   
    #data = pd.DataFrame.from_dict(results[0])
    #data.to_csv(path_or_buf='elencos.csv', index=False)

    #header = ['tconst', 'elenco']

    #with open('elencos.csv', 'w') as csvfile: 
        #writer = csv.DictWriter(csvfile, fieldnames = header) 
        #writer.writeheader() 
        #writer.writerows(results[0]) 

    




