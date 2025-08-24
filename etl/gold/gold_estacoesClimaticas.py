import pandas as pd 
import os

def main_etl():
# EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'EstacoesClimaticas')


    caminho_arquivo = os.path.join(pasta_source, 'estacoes_climaticas.csv')
    estacoes_climaticas = pd.read_csv(caminho_arquivo, sep=';')



# TRANSFORM



# LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'EstacoesClimaticas', 'estacoes_climaticas.csv')
    pasta_sink = os.path.abspath(pasta_sink)

    estacoes_climaticas.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

