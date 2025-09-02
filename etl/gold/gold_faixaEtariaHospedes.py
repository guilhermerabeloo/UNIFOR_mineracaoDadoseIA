import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from utils.parse_float import parse_float
import pandas as pd 


def main_etl():
# EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'FaixaEtariaHospedes')


    caminho_arquivo = os.path.join(pasta_source, 'faixa_etaria_hospedes.csv')
    df = pd.read_csv(caminho_arquivo, sep=';')



# TRANSFORM

    # ajusta nomes das colunas
    df.columns = df.columns.str.strip()
    
    faixa_etaria_hospedes = df.rename(columns={
        'Hóspedes registrados em Fortaleza - Internacionais - Até 18 anos': 'hospedes_internacionais_ate18anos',
        'Hóspedes registrados em Fortaleza - Internacionais - Mais de 18 a 25 anos': 'hospedes_internacionais_de18a25anos',
        'Hóspedes registrados em Fortaleza - Internacionais - Mais de 25 a 35 anos': 'hospedes_internacionais_de25a35anos',
        'Hóspedes registrados em Fortaleza - Internacionais - Mais de 35 a 50 anos': 'hospedes_internacionais_de35a50anos',
        'Hóspedes registrados em Fortaleza - Internacionais - Mais de 50 a 65 anos': 'hospedes_internacionais_de50a65anos',
        'Hóspedes registrados em Fortaleza - Internacionais - Acima de 65 anos': 'hospedes_internacionais_acima65anos',
        'Hóspedes registrados em Fortaleza - Nacionais - Até 18 anos': 'hospedes_nacionais_ate18anos',
        'Hóspedes registrados em Fortaleza - Nacionais - Mais de 18 a 25 anos': 'hospedes_nacionais_de18a25anos',
        'Hóspedes registrados em Fortaleza - Nacionais - Mais de 25 a 35 anos': 'hospedes_nacionais_de25a35anos',
        'Hóspedes registrados em Fortaleza - Nacionais - Mais de 35 a 50 anos': 'hospedes_nacionais_de35a50anos',
        'Hóspedes registrados em Fortaleza - Nacionais - Mais de 50 a 65 anos': 'hospedes_nacionais_de50a65anos',
        'Hóspedes registrados em Fortaleza - Nacionais - Acima de 65 anos': 'hospedes_nacionais_acima65anos',
    })  

    # realiza pivot no dataframe
    df_long = faixa_etaria_hospedes.melt(id_vars=['ano'], var_name='variavel', value_name='hospedes')
    
    vars_normalizadas = df_long['variavel'].astype(str).str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
    
    parts = vars_normalizadas.str.rsplit('_', n=2, expand=True)
    
    nacionalidade_map = {
        'internacionais': 'internacional',
        'nacionais': 'nacional'
    }
    
    faixa_etaria_map = {
        'ate18anos': 'Até 18 anos',
        'de18a25anos': 'De 18 a 25 anos',
        'de25a35anos': 'De 25 a 35 anos',
        'de35a50anos': 'De 35 a 50 anos',
        'de50a65anos': 'De 50 a 65 anos',
        'acima65anos': 'Acima de 65 anos',
    }
    
    df_long['nacionalidade'] = parts[1].map(nacionalidade_map).fillna(parts[1].str.capitalize())
    df_long['faixaetaria'] = parts[2].map(faixa_etaria_map).fillna(parts[2].str.capitalize())
    df_long['hospedes'] = df_long['hospedes'].map(parse_float)
    
    # ajusta ordenacao
    faixa_etaria_hospedes = df_long[['ano', 'nacionalidade', 'faixaetaria', 'hospedes']]
    faixa_etaria_hospedes = faixa_etaria_hospedes.sort_values(by=['ano', 'nacionalidade']).reset_index(drop=True)
    
    
# LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'FaixaEtariaHospedes', 'faixa_etaria_hospedes.csv')
    pasta_sink = os.path.abspath(pasta_sink)
    
    faixa_etaria_hospedes.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

