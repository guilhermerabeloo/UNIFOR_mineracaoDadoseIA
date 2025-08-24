import pandas as pd 
import os

def main_etl():
# EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'DemandaTipoHospedagem')


    caminho_arquivo = os.path.join(pasta_source, 'demanda_tipo_hospedagem.csv')
    df = pd.read_csv(caminho_arquivo, sep=';')



# TRANSFORM

    # ajusta nomes das colunas
    df.columns = df.columns.str.strip()
    
    demanda_tipo_hospedagem = df.rename(columns={
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Janeiro': 'demanda_extrahoteleira_janeiro',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Fevereiro': 'demanda_extrahoteleira_fevereiro',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Março': 'demanda_extrahoteleira_marco',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Abril': 'demanda_extrahoteleira_abril',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Maio': 'demanda_extrahoteleira_maio',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Junho': 'demanda_extrahoteleira_junho',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Julho': 'demanda_extrahoteleira_julho',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Agosto': 'demanda_extrahoteleira_agosto',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Setembro': 'demanda_extrahoteleira_setembro',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Outubro': 'demanda_extrahoteleira_outubro',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Novembro': 'demanda_extrahoteleira_novembro',
        'Demanda turística mensal via Fortaleza - Extra hoteleira - Dezembro': 'demanda_extrahoteleira_dezembro',
        'Demanda turística mensal via Fortaleza - Hoteleira - Janeiro': 'demanda_hoteleira_janeiro',
        'Demanda turística mensal via Fortaleza - Hoteleira - Fevereiro': 'demanda_hoteleira_fevereiro',
        'Demanda turística mensal via Fortaleza - Hoteleira - Março': 'demanda_hoteleira_marco',
        'Demanda turística mensal via Fortaleza - Hoteleira - Abril': 'demanda_hoteleira_abril',
        'Demanda turística mensal via Fortaleza - Hoteleira - Maio': 'demanda_hoteleira_maio',
        'Demanda turística mensal via Fortaleza - Hoteleira - Junho': 'demanda_hoteleira_junho',
        'Demanda turística mensal via Fortaleza - Hoteleira - Julho': 'demanda_hoteleira_julho',
        'Demanda turística mensal via Fortaleza - Hoteleira - Agosto': 'demanda_hoteleira_agosto',
        'Demanda turística mensal via Fortaleza - Hoteleira - Setembro': 'demanda_hoteleira_setembro',
        'Demanda turística mensal via Fortaleza - Hoteleira - Outubro': 'demanda_hoteleira_outubro',
        'Demanda turística mensal via Fortaleza - Hoteleira - Novembro': 'demanda_hoteleira_novembro',
        'Demanda turística mensal via Fortaleza - Hoteleira - Dezembro': 'demanda_hoteleira_dezembro',
    })  

    # realiza pivot no dataframe
    df_long = demanda_tipo_hospedagem.melt(id_vars=['ano'], var_name='variavel', value_name='demanda')
    
    vars_normalizadas = df_long['variavel'].astype(str).str.strip().str.replace(r'\s+', '_', regex=True).str.lower()
    
    parts = vars_normalizadas.str.rsplit('_', n=2, expand=True)
    
    tipo_hospedagem_map = {
        'extrahoteleira': 'Extra Hoteleira',
        'hoteleira': 'Hoteleira'
    }
    
    mes_map = {
        'janeiro': '01',
        'fevereiro': '02',
        'marco': '03',
        'abril': '04',
        'maio': '05',
        'junho': '06',
        'julho': '07',
        'agosto': '08',
        'setembro': '09',
        'outubro': '10',
        'novembro': '11',
        'dezembro': '12',
    }
    
    df_long['tipo_hospedagem'] = parts[1].map(tipo_hospedagem_map).fillna(parts[1].str.capitalize())
    df_long['mes'] = parts[2].map(mes_map).fillna(parts[2].str.capitalize())
    
    # ajusta ordenacao
    demanda_tipo_hospedagem = df_long[['ano', 'mes', 'tipo_hospedagem', 'demanda']]
    demanda_tipo_hospedagem = demanda_tipo_hospedagem.sort_values(by=['ano', 'tipo_hospedagem', 'mes']).reset_index(drop=True)
    
    # cria coluna data
    demanda_tipo_hospedagem['data'] = pd.to_datetime(demanda_tipo_hospedagem['ano'].astype(str) + '-' + demanda_tipo_hospedagem['mes'].astype(str) + '-01')
    demanda_tipo_hospedagem = demanda_tipo_hospedagem[['data', 'tipo_hospedagem', 'demanda']]
    
    
    
# LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'DemandaTipoHospedagem', 'demanda_tipo_hospedagem.csv')
    pasta_sink = os.path.abspath(pasta_sink)
    
    demanda_tipo_hospedagem.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

