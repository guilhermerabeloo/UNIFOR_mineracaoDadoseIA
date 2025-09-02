import pandas as pd
import numpy as np

def parse_float(numero: str) -> float:
    if pd.isna(numero):
        return np.nan
    
    s = str(numero).strip()
    if s == "":
        return np.nan

    if ',' in s and '.' in s:
        s = s.replace(',', '')
    elif ',' in s and '.' not in s:
        s = s.replace(',', '.')

    s = s.replace(' ', '')
    try:
        return float(s)
    except:
        return np.nan
    
    
    