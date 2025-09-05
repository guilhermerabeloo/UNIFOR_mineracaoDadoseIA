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


# Preparando dados
df = df_demanda.copy()
df['data'] = pd.to_datetime(df['data'])
df = df.sort_values('data').set_index('data')

# criando lags de 1 até 12 meses + variáveis sazonais
df_lag = df.copy()
for lag in range(1, 13):
    df_lag[f'lag_{lag}'] = df_lag['demanda'].shift(lag)
df_lag['month'] = df_lag.index.month
df_lag['sin_month'] = np.sin(2 * np.pi * df_lag['month'] / 12)
df_lag['cos_month'] = np.cos(2 * np.pi * df_lag['month'] / 12)
df_lag = df_lag.dropna()


# Separando treino e teste
pandemic_years = {2020, 2021}

lag1_years = (df_lag.index - pd.DateOffset(months=1)).year
mask_conservative = (df_lag.index <= pd.to_datetime('2022-12-31')) & \
                    (~df_lag.index.year.isin(pandemic_years)) & \
                    (~pd.Series(lag1_years, index=df_lag.index).isin(pandemic_years))

treino = df_lag.loc[mask_conservative].copy()


if len(treino) == 0:
    mask_relaxed = (df_lag.index <= pd.to_datetime('2022-12-31')) & \
                   (~df_lag.index.year.isin(pandemic_years))
    treino = df_lag.loc[mask_relaxed].copy()


if len(treino) == 0:
    treino = df_lag.loc[df_lag.index <= pd.to_datetime('2022-12-31')].copy()
    

teste = df_lag['2023-01-01':]


# Montando X e y
feature_cols = [f'lag_{i}' for i in range(1, 13)] + ['sin_month', 'cos_month']
X_treino = treino[feature_cols].values
y_treino = treino['demanda'].values
X_teste = teste[feature_cols].values
y_teste = teste['demanda'].values


# Treinando os modelos
lr = LinearRegression()
lr.fit(X_treino, y_treino)

rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X_treino, y_treino)


# Avaliacao no conjunto de teste
y_pred_lr = lr.predict(X_teste)
y_pred_rf = rf.predict(X_teste)

def mape(y_true, y_pred, eps=1e-8):
    return np.mean(np.abs((y_true - y_pred) / np.where(np.abs(y_true) < eps, eps, y_true))) * 100

acc_lr = max(0.0, 100.0 - mape(y_teste, y_pred_lr))
acc_rf = max(0.0, 100.0 - mape(y_teste, y_pred_rf))

print(f"Acertividade (LinearRegression): {acc_lr:.2f}%")
print(f"Acertividade (RandomForest): {acc_rf:.2f}%")


# Previsão para 2024
last_date = df.index.max()
last_vals = list(df['demanda'].iloc[-12:])  # últimos 12 meses (2023)

for i in range(1, 13):
    target_date = (last_date + pd.DateOffset(months=i))
    lags = [last_vals[-k] for k in range(1, 13)]
    sin_m = np.sin(2 * np.pi * target_date.month / 12)
    cos_m = np.cos(2 * np.pi * target_date.month / 12)
    feat = np.array(lags + [sin_m, cos_m]).reshape(1, -1)

    pred_lr = float(lr.predict(feat)[0])
    pred_rf = float(rf.predict(feat)[0])
    print(f"{target_date.strftime('%Y-%m-%d')};LR:{pred_lr:.2f};RF:{pred_rf:.2f}")

    # atualiza buffer (previsao iterativa)
    last_vals.append(pred_lr)
    last_vals.pop(0)
