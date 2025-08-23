import pandas as pd 
import os

def main_etl():
    # EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_raw = os.path.join(pasta_raiz, '..', '..', 'data', 'raw', 'DemandaTuristicaCeara')

    dfs_demanda = []
    dfs_origem_demanda = []
    dfs_motivo_viagens = []
    dfs_sexo = []
    dfs_hospedes_faixa_etaria = []

    for arquivo in os.listdir(pasta_raw):
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(pasta_raw, arquivo)
            
            df = pd.read_csv(caminho_arquivo, sep='\t', header=None, encoding='utf-16', skiprows=1, engine='python')
            
            # obtem o ano do arquivo
            ano = df.iloc[1,2]
            
            # remove as linhas e colunas desnecessarias
            df = df.drop(df[[0, 1]], axis=1)
            df = df.drop(1, axis=0)
            
            # renomeia as colunas
            df.columns = df.iloc[0].values
            df = df.iloc[1:].reset_index(drop=True)
            df.columns.name = None
            
            
            # mantem colunas de demanda(prefixo abaixo) e que não sejam totais)
            prefixo_col_demanda = 'Demanda turística mensal via Fortaleza'
            colunas_demanda = [c for c in df.columns if prefixo_col_demanda in c and 'Total' not in c and c.count('-') > 1]
            df_demanda = df[colunas_demanda].copy()
            df_demanda['ano'] = ano
            dfs_demanda.append(df_demanda)
            
            # mantem colunas de origem da demanda(prefixo abaixo)
            prefixo_col_origem_demanda = 'Demanda turística via Fortaleza - '
            colunas_origem_demanda = [c for c in df.columns if prefixo_col_origem_demanda in c and c.count('-') > 1]
            df_origem_demanda = df[colunas_origem_demanda].copy()
            df_origem_demanda['ano'] = ano
            dfs_origem_demanda.append(df_origem_demanda)
            
            # mantem colunas com motivos para as viagens (prefixo abaixo)
            prefixo_col_motivo_viagens = 'Viagem para '
            colunas_motivo_viagens = [c for c in df.columns if prefixo_col_motivo_viagens in c and c.count('-') > 1]
            df_motivo_viagens = df[colunas_motivo_viagens].copy()
            df_motivo_viagens['ano'] = ano
            dfs_motivo_viagens.append(df_motivo_viagens)
            
            # mantem colunas com informações de sexo dos viajantes 
            prefixo_col_sexo = ['Homens', 'Mulheres']
            colunas_sexo = [c for c in df.columns if (prefixo_col_sexo[0] in c or prefixo_col_sexo[1] in c) and c.count('-') > 1]
            df_sexo = df[colunas_sexo].copy()
            df_sexo['ano'] = ano
            dfs_sexo.append(df_sexo)
            
            # mantem colunas com informações de hospedes registrados por faixa etaria
            prefixo_col_hospedes_faixa_etaria = 'Hóspedes registrados em Fortaleza - '
            colunas_hospedes_faixa_etaria = [c for c in df.columns if prefixo_col_hospedes_faixa_etaria in c and 'Meio de transporte' not in c and 'Viagem para' not in c and prefixo_col_sexo[0] not in c and prefixo_col_sexo[1] not in c and c.count('-') > 1]
            df_hospedes_faixa_etaria = df[colunas_hospedes_faixa_etaria].copy()
            df_hospedes_faixa_etaria['ano'] = ano
            dfs_hospedes_faixa_etaria.append(df_hospedes_faixa_etaria)
            

    
    # TRANSFORM
    demanda = pd.concat(dfs_demanda, ignore_index=True)
    origem_demanda = pd.concat(dfs_origem_demanda, ignore_index=True)
    motivo_viagens = pd.concat(dfs_motivo_viagens, ignore_index=True)
    sexo = pd.concat(dfs_sexo, ignore_index=True)
    hospedes_faixa_etaria = pd.concat(dfs_hospedes_faixa_etaria, ignore_index=True)
    


    # LOAD
    pasta_sink = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'DemandaTuristica', 'demanda_turistica.csv'))
    demanda.to_csv(pasta_sink, sep=';', index=False)
    
    pasta_sink = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'DemandaOrigem', 'demanda_origem.csv'))
    origem_demanda.to_csv(pasta_sink, sep=';', index=False)
    
    pasta_sink = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'MotivoViagens', 'motivo_viagens.csv'))
    motivo_viagens.to_csv(pasta_sink, sep=';', index=False)
    
    pasta_sink = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'DemandaSexo', 'demanda_sexo.csv'))
    sexo.to_csv(pasta_sink, sep=';', index=False)
    
    pasta_sink = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'FaixaEtariaHospedes', 'faixa_etaria_hospedes.csv'))
    hospedes_faixa_etaria.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

