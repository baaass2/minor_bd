import json
import pandas as pd
import multiprocessing
import numpy


def main(df):
    array = []
    for row_titulos in titulos.itertuples():
        print(row_titulos.tconst)
        genres = row_titulos.genres.split(',')
        data = {row_titulos.tconst:genres}
        array.append(data)
            #data = pd.concat([data, pd.DataFrame.from_records([{'nconst': row_personas.nconst, 'profession': profession}])])

    return array

if __name__ == '__main__':
    #personas = pd.read_csv('data/name_basics.tsv', sep='\t', header=0, nrows=100)
    titulos = pd.read_csv('../data/title_basics.tsv', sep='\t', header=0)


    #data = [{'genre':'id', 'titles':[]}]

    cores = multiprocessing.cpu_count()
    titulo_split = numpy.array_split(titulos, cores)

    with multiprocessing.Pool(cores) as pool:
        results = pool.map(main, titulo_split)
    
    results = numpy.concatenate(results)

    list_edges = []
    for i in results:
        for key, value in i.items():
            list_edges.append([key, value])

    df_export = pd.DataFrame(list_edges, columns=['tconst', 'genres'])
    #df_export= df_export.drop_duplicates()
    df_export.to_csv("genres.csv", index=False, sep='|')





