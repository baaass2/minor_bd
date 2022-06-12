import pandas as pd
import multiprocessing
import numpy

def get_nodes_profession(personas):
    list_profession = []
    for row_personas in personas.itertuples():
        for profession in str(row_personas.primaryProfession).split(','):
            row_export2 = [profession, 'PROFESSION']
            list_profession.append(row_export2)

    return list_profession

def get_profession(personas):
    list_edges = []
    for row_personas in personas.itertuples():
        for profession in str(row_personas.primaryProfession).split(','):
            row_export = [row_personas.nconst, profession, 'WORKED_OF']
            list_edges.append(row_export)


    return list_edges 
def get_elenco(personas):
    list_edges = []

    for row_personas in personas.itertuples():
        for titulo in str(row_personas.knownForTitles).split(','):
            row_export = [row_personas.nconst, titulo, 'WORKED_IN']
            list_edges.append(row_export)


    return list_edges

def get_nodes_person(personas):
    list_person = []
    for row_personas in personas.itertuples():
        row_export2 = [row_personas.nconst, 'PERSON']
        list_person.append(row_export2)

    return list_person

def get_nodes_title(titles):
    list_titles = []
    for row_titles in titles.itertuples():
        row_export = [row_titles.tconst, 'TITLE']
        list_titles.append(row_export)

    return list_titles

def get_genres(titulos):
    list_edges= []

    for row_titulos in titulos.itertuples():
        for genre in str(row_titulos.genres).split(','):
            row_export = [row_titulos.tconst, genre, 'TYPE_OF']
            list_edges.append(row_export)

    return list_edges

def get_nodes_genres(titulos):
    list_edges= []

    for row_titulos in titulos.itertuples():
        for genre in str(row_titulos.genres).split(','):
            row_export = [genre, 'GENRE']
            list_edges.append(row_export)

    return list_edges
if __name__ == '__main__':

    personas = pd.read_csv('/home/sanzana/Documents/GitHub/data_minorbd/name_basics.tsv', sep='\t', header=0, nrows=1000000, low_memory=False)
    titulos = pd.read_csv('/home/sanzana/Documents/GitHub/data_minorbd/title_basics.tsv', sep='\t', header=0, nrows=1000000, low_memory=False)

    
    cores = multiprocessing.cpu_count()
    personas_split = numpy.array_split(personas, cores)
    titulos_split = numpy.array_split(titulos, cores)

    # OBTENER ELENCO
    with multiprocessing.Pool(cores) as pool:
        print('Get edges person-title')
        results = pool.map(get_elenco, personas_split)
 
    data = numpy.concatenate(results)
    df = pd.DataFrame(data, columns=[':START_ID',':END_ID', ':TYPE'])
    df.to_csv(path_or_buf='edges_elencos.csv', index=False)

    #OBTENER PROFESION
    with multiprocessing.Pool(cores) as pool:
        print('Get edges person-profesion')
        results = pool.map(get_profession, personas_split)

    data = numpy.concatenate(results)
    df = pd.DataFrame(data, columns=[':START_ID',':END_ID', ':TYPE'])
    df.to_csv(path_or_buf='edges_profession.csv', index=False)
    
    #OBTENER NODOS PROFESION

    with multiprocessing.Pool(cores) as pool:
        print('Get Nodes Profesion')
        results = pool.map(get_nodes_profession, personas_split)

    data = numpy.concatenate(results)
    df = pd.DataFrame(data, columns=['profession:ID',':LABEL'])
    df = df.drop_duplicates()
    df.to_csv(path_or_buf='nodes_profession.csv', index=False)


    #OBTENER NODOS PERSONA
    print('Get Nodes persona')
    with multiprocessing.Pool(cores) as pool:
        results = pool.map(get_nodes_person, personas_split)

    data = numpy.concatenate(results)
    df = pd.DataFrame(data, columns=['person:ID',':LABEL'])
    df = df.drop_duplicates()
    df.to_csv(path_or_buf='nodes_person.csv', index=False)

    #OBTENER NODOS TITULO

    print('Get Nodes titulo')
    with multiprocessing.Pool(cores) as pool:
        results = pool.map(get_nodes_title, titulos_split)

    data = numpy.concatenate(results)
    df = pd.DataFrame(data, columns=['title:ID',':LABEL'])
    df.to_csv(path_or_buf='nodes_title.csv', index=False)

    #OBTENER Get edges TITULO-genre

    print('Get edges titulo-genre')
    with multiprocessing.Pool(cores) as pool:
        results = pool.map(get_genres, titulos_split)

    data = numpy.concatenate(results)
    df = pd.DataFrame(data, columns=[':START_ID',':END_ID', ':TYPE'])
    df.to_csv(path_or_buf='edges_genre.csv', index=False)

    
    #OBTENER NODOS GENRE

    print('Get Nodes genre')
    with multiprocessing.Pool(cores) as pool:
        results = pool.map(get_nodes_genres, titulos_split)

    data = numpy.concatenate(results)
    df = pd.DataFrame(data, columns=['genre:ID',':LABEL'])
    df = df.drop_duplicates()
    df.to_csv(path_or_buf='nodes_genre.csv', index=False)



