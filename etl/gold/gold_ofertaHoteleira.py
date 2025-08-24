import pandas as pd 
import os

def main_etl():
# EXTRACT
    pasta_raiz = os.path.dirname(os.path.abspath(__file__))
    pasta_source = os.path.join(pasta_raiz, '..', '..', 'data', 'silver', 'OfertaHoteleira')


    caminho_arquivo = os.path.join(pasta_source, 'oferta_hoteleira.csv')
    oferta_hoteleira = pd.read_csv(caminho_arquivo, sep=';')



# TRANSFORM



# LOAD

    pasta_sink = os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'OfertaHoteleira', 'oferta_hoteleira.csv')
    pasta_sink = os.path.abspath(pasta_sink)

    oferta_hoteleira.to_csv(pasta_sink, sep=';', index=False)


if __name__ == "__main__":
    main_etl()

