from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
import pandas as pd
import numpy as np
import os

pasta_raiz = os.path.dirname(os.path.abspath(__file__))

def carrega_dados_csv(caminho_arquivo: str) -> pd.DataFrame:
    try:
        return pd.read_csv(caminho_arquivo, sep=';')
    except Exception as e:
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}. Erro: {e}")
    
arquivo_demanda = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'DemandaTipoHospedagem', 'demanda_tipo_hospedagem.csv'))
df_demanda = carrega_dados_csv(arquivo_demanda)




# preparando os dados para treino e teste
df = df_demanda.copy()
df['data'] = pd.to_datetime(df['data'])
df = df.sort_values('data').set_index('data')

    # criando uma feature simples (lag 1) usando a série completa
df_lag = df.copy()
df_lag['lag_1'] = df_lag['demanda'].shift(1)
df_lag = df_lag.dropna() 

    # dividindo os dados entre treino e teste
treino = df_lag[:'2022-12-31']
teste  = df_lag['2023-01-01':]

    # X/y para treino (apenas lag_1 como característica)
X_treino = treino[['lag_1']].values
y_treino = treino['demanda'].values




# treinando e fazendo predicoes com os modelos Regressao Linear e Random Forest
    # Linear Regression
lr = LinearRegression()
lr.fit(X_treino, y_treino)

    # Random Forest Regressor 
rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X_treino, y_treino)

    # prepara X_teste e y_teste
X_teste = teste[['lag_1']].values
y_teste = teste['demanda'].values

    # realizando previsoes com os dois modelos
y_pred_lr = lr.predict(X_teste)
y_pred_rf = rf.predict(X_teste)




# Avaliando os modelos
def mape(y_true, y_pred, eps=1e-8):
    return np.mean(np.abs((y_true - y_pred) / np.where(np.abs(y_true) < eps, eps, y_true))) * 100

    # calculando percentual de acertividade dos modelos
acc_lr = max(0.0, 100.0 - mape(y_teste, y_pred_lr))
acc_rf = max(0.0, 100.0 - mape(y_teste, y_pred_rf))

print(f"Acertividade (LinearRegression): {acc_lr:.2f}%")
print(f"Acertividade (RandomForest): {acc_rf:.2f}%")
