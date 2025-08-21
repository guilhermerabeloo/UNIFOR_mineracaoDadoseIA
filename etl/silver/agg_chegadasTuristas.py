import pandas as pd 
import os

def main_etl():
    # EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'raw', 'ChegadaTuristasInternacionais')

    dfs_fechados = []
    dfs_acumulados = []
    for arquivo in os.listdir(pasta_source):
        if arquivo.endswith('.csv') and 'acum' not in arquivo:
            caminho_arquivo = os.path.join(pasta_source, arquivo)
            
            df = pd.read_csv(caminho_arquivo, sep=';')
            
            dfs_fechados.append(df)
        
        elif arquivo.endswith('.csv') and 'acum' in arquivo:
            caminho_arquivo = os.path.join(pasta_source, arquivo)
            
            df = pd.read_csv(caminho_arquivo, sep=';')
            
            dfs_acumulados.append(df)
            
            
    # TRANSFORM
        # Tratando arquivos no padrao de daos fechados
    chegadas_turistas_fechados = pd.concat(dfs_fechados, ignore_index=True)
    chegadas_turistas_fechados = chegadas_turistas_fechados.rename(columns={
        'Continente': 'continente',
        'cod continente': 'cod_continente',
        'País': 'pais',
        'cod pais': 'cod_pais',
        'UF': 'uf',
        'cod uf': 'cod_uf',
        'Via': 'via',   
        'cod via': 'cod_via',
        'ano': 'ano',
        'Mês': 'mes',
        'cod mes': 'cod_mes',
        'Chegadas': 'chegadas'
    })

        # Tratando arquivos no padrao de daos acumulados
    chegadas_turistas_acumulados = pd.concat(dfs_acumulados, ignore_index=True)
    chegadas_turistas_acumulados = chegadas_turistas_acumulados.rename(columns={
                'Via_de_acesso': 'via',
                'UF': 'uf',
                'nome_pais_correto': 'pais',
                'mes': 'mes',   
                'ano': 'ano',
                'Chegadas': 'chegadas'
            })

            # Adicionando coluna continente aos dados acumulados
    map_continente = chegadas_turistas_fechados[['continente', 'pais']].drop_duplicates()
    chegadas_turistas_acumulados = chegadas_turistas_acumulados.merge(map_continente, on='pais', how='left')

        # Tratando df final
    chegadas_turistas = pd.concat([chegadas_turistas_fechados, chegadas_turistas_acumulados], ignore_index=True)

            # Adicionando coluna de data
    map_meses = {
        'janeiro': 1,
        'fevereiro': 2,
        'março': 3,
        'abril': 4,
        'maio': 5,
        'junho': 6,
        'julho': 7, 
        'agosto': 8,
        'setembro': 9,
        'outubro': 10,
        'novembro': 11,
        'dezembro': 12
    }
    chegadas_turistas['num_mes'] = chegadas_turistas['mes'].map(lambda x: map_meses.get(x.lower(), None))
    chegadas_turistas['data'] = pd.to_datetime(dict(year=chegadas_turistas['ano'], month=chegadas_turistas['num_mes'], day=1))

            # Mantendo apenas as colunas necessarias
    chegadas_turistas = chegadas_turistas.drop(columns=['cod_continente', 'cod_pais', 'cod_uf', 'cod_mes', 'num_mes'])


    # LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'ChegadaTuristasInternacionais', 'chegadas_turistas.csv')
    pasta_sink = os.path.abspath(pasta_sink)

    chegadas_turistas.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

