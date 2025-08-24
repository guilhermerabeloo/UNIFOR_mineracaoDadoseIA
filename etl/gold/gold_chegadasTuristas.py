import pandas as pd 
import os

def main_etl():
# EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'ChegadaTuristasInternacionais')


    caminho_arquivo = os.path.join(pasta_source, 'chegadas_turistas.csv')
    df = pd.read_csv(caminho_arquivo, sep=';')



# TRANSFORM
    
    # mantem apenas dados do ceara
    df_ceara = df[df['uf'] == 'Ceará'].reset_index(drop=True)
    
    chegadas_turistas = df_ceara[['data', 'ano', 'mes', 'continente', 'pais', 'uf', 'chegadas']]



# LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'ChegadaTuristasInternacionais', 'chegadas_turistas.csv')
    pasta_sink = os.path.abspath(pasta_sink)

    chegadas_turistas.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

