import pandas as pd

personas = pd.read_csv('data/name_basics.tsv', sep='\t', header=0)
titulos = pd.read_csv('data/title_basics.tsv', sep='\t', header=0)


data = {}
for a in titulos['tconst'][0]:

    for i in personas.itertuples():
        for titulo in i.knownForTitles:
            elenco = []
            if i == titulo:
                elenco.append(i.nconst)
        
    dicc = {'tconst':a, 'elenco':elenco}
    data.update(dicc)

print(data)

#print(actores['nconst'])

