import json
import pandas as pd
import multiprocessing
import numpy


def main(titulos):
    data = pd.DataFrame(columns=['tconst','genre'])

    for row_titulos in titulos.itertuples():
        print(row_titulos.tconst)
        for genre in str(row_titulos.genres).split(','):
            #data = revisar_dicc(data, genre, row_titulos.tconst)
            data = pd.concat([data, pd.DataFrame.from_records([{'tconst': row_titulos.tconst, 'genre':genre}])])

    return data


if __name__ == '__main__':
    #personas = pd.read_csv('data/name_basics.tsv', sep='\t', header=0, nrows=100)
    titulos = pd.read_csv('../data/title_basics.tsv', sep='\t', header=0)


    #data = [{'genre':'id', 'titles':[]}]

    cores = multiprocessing.cpu_count()
    titulo_split = numpy.array_split(titulos, cores)

    with multiprocessing.Pool(cores) as pool:
        results = pool.map(main, titulo_split)
    #print(results)
    data = pd.concat(results)

    data.to_csv(path_or_buf='genres.csv', index=False)





