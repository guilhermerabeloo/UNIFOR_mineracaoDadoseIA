import pandas as pd 
import os

def main_etl():
# EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'DemandaSexo')


    caminho_arquivo = os.path.join(pasta_source, 'demanda_sexo.csv')
    df = pd.read_csv(caminho_arquivo, sep=';')



# TRANSFORM

    # ajusta nomes das colunas
    df.columns = df.columns.str.strip()
    demanda_sexo = df.rename(columns={
        'Hóspedes registrados em Fortaleza - Internacionais - Homens': 'hospedes_internacionais_homens',
        'Hóspedes registrados em Fortaleza - Internacionais - Mulheres': 'hospedes_internacionais_mulheres',
        'Hóspedes registrados em Fortaleza - Nacionais - Homens': 'hospedes_nacionais_homens',
        'Hóspedes registrados em Fortaleza - Nacionais - Mulheres': 'hospedes_nacionais_mulheres',
    })
    
    # realiza pivot no dataframe
    df_long = demanda_sexo.melt(id_vars=['ano'], var_name='variavel', value_name='hospedes')
    
    vars_normalizadas = df_long['variavel'].astype(str).str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
    parts = vars_normalizadas.str.rsplit('_', n=2, expand=True)

    nacionalidade_map = {
        'internacionais': 'internacional',
        'nacionais': 'nacional'
    }
    
    sexo_map = {
        'homens': 'masculino',
        'mulheres': 'feminino'
    }

    df_long['nacionalidade'] = parts[1].map(nacionalidade_map).fillna(parts[1].str.capitalize())
    df_long['sexo'] = parts[2].map(sexo_map).fillna(parts[2].str.capitalize())
    
    # ajusta ordenacao
    demanda_sexo = df_long[['ano', 'sexo', 'nacionalidade', 'hospedes']]
    demanda_sexo = demanda_sexo.sort_values(by=['ano', 'sexo', 'nacionalidade']).reset_index(drop=True)
    

    
# LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'DemandaSexo', 'demanda_sexo.csv')
    pasta_sink = os.path.abspath(pasta_sink)

    demanda_sexo.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

