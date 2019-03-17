#!/usr/bin/env python
# coding: utf-8

from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import numpy as np
import requests
import re
import calendar
import time
import json
import os

def getStockCodes(dir=os.path.join(os.path.dirname(__file__), '../config/')):
    print('[INFO] Downloading stock codes......')
    headers = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15',
      'X-Requested-With': 'XMLHttpRequest'
    }

    res = requests.get('http://isin.twse.com.tw/isin/C_public.jsp?strMode=2', headers = headers)
    res.encoding =  res.apparent_encoding

    df = pd.read_html(res.text)[0]
    df = df.drop([0])
    df = df.drop([1, 2, 3, 4, 5, 6], axis = 1)
    df = df.iloc[df.loc[df[0] == '股票'].index[0]:df.loc[df[0] == '上市認購(售)權證'].index[0] - 1]

    res = requests.get('http://isin.twse.com.tw/isin/C_public.jsp?strMode=4', headers = headers)
    res.encoding =  res.apparent_encoding

    gdf = pd.read_html(res.text)[0]
    gdf = gdf.drop([0])
    gdf = gdf.drop([1, 2, 3, 4, 5, 6], axis = 1)
    gdf = gdf.iloc[gdf.loc[gdf[0] == '股票'].index[0]:gdf.loc[gdf[0] == '臺灣存託憑證'].index[0] - 1]
    
    fdf = pd.DataFrame({'code': df[0].map(lambda x: re.compile("\d*").findall(x)[0]).append(gdf[0].map(lambda x: re.compile("\d*").findall(x)[0])), '名字': df[0].map(lambda x: x[5:].strip()).append(gdf[0].map(lambda x: x[5:].strip()))}, columns = ['code', '名字'])
    fdf = fdf[fdf['code'].apply(lambda x: str(x).isdigit())]
    fdf = fdf.sort_values(by = ['code']) #code順序排序
    fdf = fdf.reset_index(drop=True) #重組索引
    fdf.to_csv(dir + '/stock_name.csv', index = None, encoding = "utf_8")
    print('[DONE] Successfully downloaded')