import pandas as pd
import numpy as np 
import os

def main_etl():
    # EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_raw = os.path.join(pasta_raiz, '..', '..', 'data', 'raw', 'OfertaHoteleiraGrandeFor')

    dfs = []
    
    for arquivo in os.listdir(pasta_raw):
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(pasta_raw, arquivo)
            
            df = pd.read_csv(caminho_arquivo, sep='\t', header=None, encoding='utf-16', skiprows=1, engine='python')
            
            # obtem o ano do arquivo
            ano = df.iloc[1,3]
            
            df = df.drop(df[[1]], axis=1)
            df = df.drop(1, axis=0)
        
            # renomeia as colunas
            df.columns = df.iloc[0].values
            df = df.iloc[1:].reset_index(drop=True)
            df.columns.name = None
            df.rename(columns={np.nan: 'cidade'}, inplace=True)
            df.columns = df.columns.str.strip()

            df = df.rename(columns={
                'Oferta dos meios de hospedagem nos municípios turísticos - Estabelecimentos': 'estabelecimentos',
                'Oferta dos meios de hospedagem nos municípios turísticos - Leitos': 'leitos',
                'Oferta dos meios de hospedagem nos municípios turísticos - Unidades habitacionais (UHs)': 'unidades_habitacionais'
            })
            
            # adiciona a coluna de ano
            df['ano'] = ano
            
            
            dfs.append(df)
        
            
            
    # TRANSFORM
    oferta_hoteleira = pd.concat(dfs, ignore_index=True)

    
    
    # LOAD
    pasta_sink = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'OfertaHoteleira', 'oferta_hoteleira.csv'))
    oferta_hoteleira.to_csv(pasta_sink, sep=';', index=False)
    


if __name__ == "__main__":
    main_etl()

