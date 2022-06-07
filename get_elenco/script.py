import json
import pandas as pd
import multiprocessing
import numpy
import csv


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

def main(personas):
    data = [{'tconst':'id', 'elenco':[]}]

    for row_personas in personas.itertuples():
        print(row_personas.nconst)
        for titulo in str(row_personas.knownForTitles).split(','):
            data = revisar_dicc(data, titulo, row_personas.nconst)

    return data
if __name__ == '__main__':

    personas = pd.read_csv('../data/name_basics.tsv', sep='\t', header=0, nrows=100)
    #titulos = pd.read_csv('../data/title_basics.tsv', sep='\t', header=0, nrows=1000)

    
    cores = multiprocessing.cpu_count()
    personas_split = numpy.array_split(personas, cores)

    with multiprocessing.Pool(cores) as pool:
        results = pool.map(main, personas_split)
 
    data = pd.DataFrame.from_dict(results[0])
    data.to_csv(path_or_buf='elencos.csv', index=False)

    #header = ['tconst', 'elenco']

    #with open('elencos.csv', 'w') as csvfile: 
        #writer = csv.DictWriter(csvfile, fieldnames = header) 
        #writer.writeheader() 
        #writer.writerows(results[0]) 

    




