import pandas as pd 
import os

def main_etl():
    # EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'raw', 'EstacoesClimaticas')

    dfs = []

    for arquivo in os.listdir(pasta_source):
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(pasta_source, arquivo)
            
            df = pd.read_csv(caminho_arquivo, sep=';')
            
            dfs.append(df)
    
    
    
    # TRANSFORM
    estacoes_climaticas = pd.concat(dfs, ignore_index=True)
    estacoes_climaticas = estacoes_climaticas.rename(columns={
        'Data': 'data',
        'Hora (UTC)': 'hora',
        'Temp. Ins. (C)': 'temp_instant',
        'Temp. Max. (C)': 'temp_max',
        'Temp. Min. (C)': 'temp_min',
        'Umi. Ins. (%)': 'umi_instant',
        'Umi. Max. (%)': 'umi_max',
        'Umi. Min. (%)': 'umi_min',
        'Pto Orvalho Ins. (C)': 'pto_orvalho_instant',
        'Pto Orvalho Max. (C)': 'pto_orvalho_max',
        'Pto Orvalho Min. (C)': 'pto_orvalho_min',
        'Pressao Ins. (hPa)': 'pressao_instant',
        'Pressao Max. (hPa)': 'pressao_max',
        'Pressao Min. (hPa)': 'pressao_min',
        'Vel. Vento (m/s)': 'vel_vento',
        'Dir. Vento (m/s)': 'dir_vento',
        'Raj. Vento (m/s)': 'raj_vento',
        'Radiacao (KJ/m²)': 'radiacao',
        'Chuva (mm)': 'chuva'
    })
    
    estacoes_climaticas = estacoes_climaticas.dropna(subset=['temp_instant', 'temp_max', 'temp_min', 'umi_instant', 'umi_max', 'umi_min', 'pto_orvalho_instant', 'pto_orvalho_max', 'pto_orvalho_min', 'pressao_instant', 'pressao_max', 'pressao_min', 'vel_vento', 'dir_vento', 'raj_vento', 'radiacao', 'chuva'])
    estacoes_climaticas['data'] = pd.to_datetime(estacoes_climaticas['data'], format='%d/%m/%Y')
    estacoes_climaticas['hora'] = pd.to_timedelta(estacoes_climaticas['hora'] / 100, unit='h')
    estacoes_climaticas['data_hora'] = estacoes_climaticas['data'] + estacoes_climaticas['hora']
    
    estacoes_climaticas = estacoes_climaticas.drop(columns=['data', 'hora', 'radiacao'])



    # LOAD
    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'EstacoesClimaticas', 'estacoes_climaticas.csv')
    pasta_sink = os.path.abspath(pasta_sink)

    estacoes_climaticas.to_csv(pasta_sink, sep=';', index=False)



if __name__ == "__main__":
    main_etl()

