import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from utils.parse_float import parse_float
import pandas as pd 

def main_etl():
# EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'MotivoViagens')


    caminho_arquivo = os.path.join(pasta_source, 'motivo_viagens.csv')
    df = pd.read_csv(caminho_arquivo, sep=';')



# TRANSFORM

    # ajusta nomes das colunas
    df.columns = df.columns.str.strip()
    motivo_viagens = df.rename(columns={
        'Hóspedes registrados em Fortaleza - Internacionais - Viagem para convenções e eventos': 'hospedes_internacionais_convencoesEEventos',
        'Hóspedes registrados em Fortaleza - Internacionais - Viagem para negócios': 'hospedes_internacionais_negocios',
        'Hóspedes registrados em Fortaleza - Internacionais - Viagem para turismo e lazer': 'hospedes_internacionais_turismoELazer',
        'Hóspedes registrados em Fortaleza - Internacionais - Viagem para Outros motivos': 'hospedes_internacionais_outros',
        'Hóspedes registrados em Fortaleza - Nacionais - Viagem para convenções e eventos': 'hospedes_nacionais_convencoesEEventos',
        'Hóspedes registrados em Fortaleza - Nacionais - Viagem para negócios': 'hospedes_nacionais_negocios',
        'Hóspedes registrados em Fortaleza - Nacionais - Viagem para turismo e lazer': 'hospedes_nacionais_turismoELazer',
        'Hóspedes registrados em Fortaleza - Nacionais - Viagem para Outros motivos': 'hospedes_nacionais_outros',
    })  

    # realiza pivot no dataframe
    df_long = motivo_viagens.melt(id_vars=['ano'], var_name='variavel', value_name='hospedes')
    
    vars_normalizadas = df_long['variavel'].astype(str).str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
    
    parts = vars_normalizadas.str.rsplit('_', n=2, expand=True)
    
    nacionalidade_map = {
        'internacionais': 'internacional',
        'nacionais': 'nacional'
    }
    
    motivo_map = {
        'convencoeseeventos': 'Convencoes e Eventos',
        'negocios': 'Negocios',
        'outros': 'Outros Motivos',
        'turismoelazer': 'Turismo e Lazer',
    }

    df_long['nacionalidade'] = parts[1].map(nacionalidade_map).fillna(parts[1].str.capitalize())
    df_long['motivo'] = parts[2].map(motivo_map).fillna(parts[2].str.capitalize())
    df_long['hospedes'] = df_long['hospedes'].map(parse_float)
    
    # ajusta ordenacao
    motivo_viagens = df_long[['ano', 'nacionalidade', 'motivo', 'hospedes']]
    motivo_viagens = motivo_viagens.sort_values(by=['ano', 'nacionalidade', 'motivo']).reset_index(drop=True)
    
    
    
# LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'MotivoViagens', 'motivo_viagens.csv')
    pasta_sink = os.path.abspath(pasta_sink)
    
    motivo_viagens.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

