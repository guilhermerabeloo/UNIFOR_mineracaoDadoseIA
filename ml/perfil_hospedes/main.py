#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import numpy as np
import os
import joblib

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# --- raiz do projeto (arquivo) ---
pasta_raiz = os.path.dirname(os.path.abspath(__file__))

# -------------------------
# Utilitários de I/O e pivot
# -------------------------
def carrega_dados_csv(caminho_arquivo: str) -> pd.DataFrame:
    try:
        return pd.read_csv(caminho_arquivo, sep=';')
    except Exception as e:
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}. Erro: {e}")

def pivot_proporcao(df, index_keys, col, val='hospedes', prefix=None):
    p = df.pivot_table(index=index_keys, columns=col, values=val, aggfunc='sum', fill_value=0)
    totals = p.sum(axis=1).replace(0, np.nan)
    props = p.div(totals, axis=0).fillna(0)
    if prefix is None:
        prefix = f"{col.lower()}_"
    props.columns = [f"{prefix}{str(c).strip().lower().replace(' ','_')}_prop" for c in props.columns]
    props = props.reset_index()
    return props

def leitura_e_features(pasta_raiz):
    arquivo_demanda_sexo = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'DemandaSexo', 'demanda_sexo.csv'))
    arquivo_faixa_etaria_hospedes = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'FaixaEtariaHospedes', 'faixa_etaria_hospedes.csv'))
    arquivo_motivo_viagens = os.path.abspath(os.path.join(pasta_raiz, '..', '..', 'data', 'gold', 'MotivoViagens', 'motivo_viagens.csv'))

    df_demanda_sexo = carrega_dados_csv(arquivo_demanda_sexo)
    df_faixa_etaria_hospedes = carrega_dados_csv(arquivo_faixa_etaria_hospedes)
    df_motivo_viagens = carrega_dados_csv(arquivo_motivo_viagens)

    group_keys = ['ano', 'nacionalidade']

    sexo_proporcao = pivot_proporcao(df_demanda_sexo, group_keys, 'sexo', prefix='sexo_')
    faixa_proporcao = pivot_proporcao(df_faixa_etaria_hospedes, group_keys, 'faixaetaria', prefix='faixa_')
    motivo_proporcao = pivot_proporcao(df_motivo_viagens, group_keys, 'motivo', prefix='motivo_')

    df_feature = sexo_proporcao.merge(faixa_proporcao, on=group_keys, how='outer') \
                               .merge(motivo_proporcao, on=group_keys, how='outer') \
                               .fillna(0)

    return df_feature, group_keys

# -------------------------
# Treino (sem salvar nada)
# -------------------------
def treina_kmeans_sem_salvar(df_feature, group_keys, n_clusters=3):
    # colunas numéricas (proporções)
    num_cols = [c for c in df_feature.columns if c not in group_keys and df_feature[c].dtype.kind in 'fi']

    scaler = StandardScaler()
    X_num = df_feature[num_cols].values
    X_scaled = scaler.fit_transform(X_num)

    df_scalado = pd.concat([df_feature[group_keys].reset_index(drop=True),
                            pd.DataFrame(X_scaled, columns=num_cols)], axis=1)

    X = df_scalado[num_cols].values
    n_samples = X.shape[0]
    k = min(n_clusters, max(1, n_samples))

    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X)

    assignments = pd.concat([df_scalado[group_keys].reset_index(drop=True),
                             pd.DataFrame({'cluster': labels})], axis=1)

    centroids_scaled = pd.DataFrame(km.cluster_centers_, columns=num_cols)
    centroids_original = None
    try:
        centroids_original = pd.DataFrame(scaler.inverse_transform(centroids_scaled.values), columns=num_cols)
    except Exception:
        centroids_original = None

    sil = float("nan")
    if n_samples > 1 and k > 1:
        try:
            sil = silhouette_score(X, labels)
        except Exception:
            sil = float("nan")

    return {
        "km": km,
        "assignments": assignments,
        "centroids_scaled": centroids_scaled,
        "centroids_original": centroids_original,
        "scaler": scaler,
        "num_cols": num_cols,
        "silhouette": sil
    }

# -------------------------
# Resumo alto nível (versão ajustada)
# -------------------------
def resumo_alto_nivel(result, df_feature, group_keys):
    assignments = result["assignments"]
    num_cols = result["num_cols"]
    sil = result["silhouette"]

    # juntar proporções originais com o cluster (preserva ordem)
    df_feat_idx = df_feature.reset_index(drop=True)
    df_with_cluster = pd.concat([df_feat_idx, assignments['cluster']], axis=1)

    # médias das proporções por cluster
    cluster_props = df_with_cluster.groupby('cluster')[num_cols].mean()

    def top_label_val(row, prefix):
        cols = [c for c in row.index if c.startswith(prefix)]
        if not cols:
            return ("-", 0.0)
        top = row[cols].idxmax()
        val = float(row[top])
        label = top.replace(prefix, "").replace("_prop", "").replace("_", " ").title()
        return (label, val)

    total_rows = len(assignments)
    textos = []

    for c in cluster_props.index:
        row = cluster_props.loc[c]
        cluster_size = int((assignments['cluster'] == c).sum())
        share = cluster_size / total_rows if total_rows > 0 else 0.0

        sex_label, sex_val = top_label_val(row, "sexo_")
        faixa_label, faixa_val = top_label_val(row, "faixa_")
        motivo_label, motivo_val = top_label_val(row, "motivo_")

        # rótulo curto
        label_short = f"{motivo_label} - {faixa_label} - {sex_label}"

        def pct(v):
            return f"{v*100:.1f}%"

        txt_lines = [
            f"Cluster {c} — {cluster_size} linhas ({share*100:.1f}% do conjunto).",
            f"Perfil principal: {sex_label} ({pct(sex_val)}), faixa {faixa_label} ({pct(faixa_val)}), motivo: {motivo_label} ({pct(motivo_val)}).",
            f"Rótulo sugerido: \"{label_short}\"."
        ]

        # recomendações automatizadas (matching case-insensitive)
        motivo_low = motivo_label.lower()
        recs = []
        if any(x in motivo_low for x in ["tur", "tour", "turismo"]):
            recs.append("Oferecer pacotes/experiências turísticas e focar redes sociais.")
            recs.append("Promoções sazonais e parcerias com operadores locais.")
        elif any(x in motivo_low for x in ["neg", "bus", "trab", "business"]):
            recs.append("Tarifas corporativas, pacotes para reuniões e internet rápida.")
            recs.append("Comunicação via e-mail e LinkedIn; parcerias B2B.")
        else:
            recs.append("Testar pacotes promocionais e mensagens A/B para entender preferências.")

        # recomendações por faixa etária (heurística simples)
        faixa_low = faixa_label.lower()
        if any(d in faixa_low for d in ["18", "20", "25", "30", "34"]):
            recs.append("Investir em formatos visuais (Instagram, Reels) e experiências.")
        else:
            recs.append("Comunicação com foco em conforto, segurança e informações práticas.")

        recs = recs[:3]
        txt_lines.append("Sugestões de ação: " + " ".join(recs))
        textos.append("\n".join(txt_lines))

    qualidade = f"Qualidade do agrupamento (silhouette): {sil:.3f}" if not pd.isna(sil) else "Silhouette: não calculável (poucas amostras ou 1 cluster)."

    min_cluster = (assignments['cluster'].value_counts().min() if len(assignments)>0 else 0)
    aviso = ""
    if min_cluster < 5:
        aviso = "Atenção: há clusters muito pequenos (menos que 5 linhas) — insights podem não ser representativos."

    # imprimir
    print("\n=== Resumo alto nível por cluster ===\n")
    for t in textos:
        print(t)
        print("-"*60)
    print("\n=== Visão geral ===")
    print(qualidade)
    if aviso:
        print("AVISO:", aviso)
    print("\nFim do resumo.\n")

# -------------------------
# Exemplo de fluxo mínimo que garante 'result' existir:
# -------------------------
df_feature, group_keys = leitura_e_features(pasta_raiz)
result = treina_kmeans_sem_salvar(df_feature=df_feature, group_keys=group_keys, n_clusters=3)
resumo_alto_nivel(result, df_feature, group_keys)
