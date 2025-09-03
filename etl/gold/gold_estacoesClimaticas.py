import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from utils.parse_float import parse_float
import pandas as pd 

def main_etl():
# EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'EstacoesClimaticas')


    caminho_arquivo = os.path.join(pasta_source, 'estacoes_climaticas.csv')
    estacoes_climaticas = pd.read_csv(caminho_arquivo, sep=';')



# TRANSFORM
    estacoes_climaticas = estacoes_climaticas.drop(columns=['temp_instant', 'umi_instant', 'pto_orvalho_instant', 'pto_orvalho_max', 'pto_orvalho_min', 'pressao_max', 'pressao_min', 'vel_vento', 'dir_vento', 'raj_vento'])

    estacoes_climaticas['temp_max'] = estacoes_climaticas['temp_max'].map(parse_float)
    estacoes_climaticas['temp_min'] = estacoes_climaticas['temp_min'].map(parse_float)
    estacoes_climaticas['umi_max'] = estacoes_climaticas['umi_max'].map(parse_float)
    estacoes_climaticas['umi_min'] = estacoes_climaticas['umi_min'].map(parse_float)
    estacoes_climaticas['chuva'] = estacoes_climaticas['chuva'].map(parse_float)

    estacoes_climaticas['data_hora'] = pd.to_datetime(estacoes_climaticas['data_hora'])
    estacoes_climaticas['mes'] = estacoes_climaticas['data_hora'].dt.to_period('M').dt.to_timestamp()
    
    def count_rainy_days(x):
        return (x>0).sum()
    agg_funcs = {
        'temp_max': ['mean'],
        'temp_min': ['mean'],
        'chuva': ['sum','mean', count_rainy_days] 
    }

    df_monthly_clima = estacoes_climaticas.groupby('mes').agg(agg_funcs)
    
    # arrumando nomes das colunas 
    df_monthly_clima.columns = [
        '_'.join([c if isinstance(c, str) else str(c) for c in col]).rstrip('_') 
        for col in df_monthly_clima.columns.to_flat_index()
    ]
    df_monthly_clima = df_monthly_clima.reset_index().rename(columns={'mes':'data'})

    df_monthly_clima = df_monthly_clima.rename(columns={col: col.replace('<lambda>','rainy_hours') for col in df_monthly_clima.columns})



# LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'EstacoesClimaticas', 'estacoes_climaticas.csv')
    pasta_sink = os.path.abspath(pasta_sink)

    df_monthly_clima.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

