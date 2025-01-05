#coding: utf-8

import pandas as pd
import numpy as np

# 假设你的DataFrame叫df，包含'ts_code', 'dt', 'price', 'high', 'low', 'name'
# 示例数据：
# df = pd.DataFrame({
#     'ts_code': [...],
#     'dt': [...],
#     'price': [...],
#     'high': [...],
#     'low': [...],
#     'name': [...],
# })

def calculate_bollinger_bands(df, window=20, num_std=2):
    """
    计算Bollinger Bands
    """
    df['mean'] = df['price'].rolling(window=window).mean()
    df['std'] = df['price'].rolling(window=window).std()
    df['upper_band'] = df['mean'] + num_std * df['std']
    df['lower_band'] = df['mean'] - num_std * df['std']
    return df


df = calculate_bollinger_bands()