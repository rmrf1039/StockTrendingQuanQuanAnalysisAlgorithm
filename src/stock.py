#!/usr/bin/env python
# coding: utf-8

from datetime import datetime, timedelta
from dateutil.parser import parse
from retry import retry
from io import StringIO

import pandas as pd
import numpy as np
import time
import math
import requests
import configparser
import re
import calendar
import json
import os


class Stock():
    def __init__(self, code, mode, date, source=pd.DataFrame([]), download_only=False, modifyArchive=True, configPath=os.path.join(os.path.dirname(__file__), '../config/config.ini')):
        ''' Decode Config '''
        config = configparser.ConfigParser()
        config.read(configPath)
        
        ''' Properties '''
        self.__code = code
        self.__mode = mode
        self.__date = date
        self.__modifyArchive = modifyArchive

        ''' Files '''
        self.__archiveDir = config['Dir']['Archive']
        self.__archivePath = self.__archiveDir + 'save_' + str(self.__code) + '_' + self.__mode + '.csv'
        
        ''' Process archive file '''
        self.__archiveData = pd.DataFrame([])

        if os.path.exists(self.__archivePath):
            self.__archiveData = pd.read_csv(self.__archivePath, index_col=0)
            self.__lastPCDate = self.__archiveData['date'].iloc[-1] if not self.__archiveData.empty else ''
            self.__archiveData = self.__archiveData.iloc[:-1]

        ''' Public '''
        self.result = {}

        ''' Get data from source '''
        self.__data = self.download() if source.empty else source

        ''' If the object is going to run, run all the analysis '''
        if not self.__data.empty and not download_only:
            ''' Stock information '''
            self.result['gain_rate'] = round((self.__data['close'].iloc[-1] - self.__data['close'].iloc[-2])/self.__data['close'].iloc[-2], 3) if len(self.__data) >= 2 else 0
            self.result['volume_rate'] = self.__data['amount'].iloc[-1] / math.ceil(float(self.__data[::-1][['amount']].iloc[1:20 + 1].sum() / 20)) if len(self.__data) >= 21 else 0
            self.result['last_date'] = self.__data['date'].iloc[-1]
            #self.__data = self.getMean(20)
            #self.__data = self.getMean(60)

            ''' Algorithm '''
            self.__data = self.trend()
            self.__data = self.circle()
            self.__data = self.validate()
            self.result['state'] = self.estimate()
        else:
            self.result['state'] = 'fail'

    @retry(delay = 1, max_delay = 5, backoff = 1.5)
    def download(self, all=False):
        rs = requests.session()
        df = pd.DataFrame([])

        ''' Mark the timestamp if the has archive file '''
        fromDate = '&from=' + str(parse(self.__lastPCDate).timestamp()) if not all and not self.__archiveData.empty else ''

        try:
            ''' Connect to server '''
            res = rs.get('https://histock.tw/Stock/tv/udf.asmx/history?symbol={}&resolution={}&to={}'.format(self.__code, self.__mode, int(self.__date.timestamp())) + fromDate, \
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15'}, \
                timeout=10, \
                stream=True)

            data = json.loads(res.text)
            df = pd.DataFrame.from_dict(data = data)

            if data['s'] == 'ok':
                ''' Organize the df from network '''
                df = df.drop(['s', 'nextTime'], axis=1)
                df.columns = ['date', 'close', 'open', 'high', 'low', 'amount']
                df['date'] = df['date'].map(lambda x: datetime.fromtimestamp(x - 37800).strftime('%Y-%m-%d'))
            else:
                print(u'\u001b[41;1m[WARNING/Stock: {}]\u001b[0m successful connection but with incorrect status'.format(self.__code))
        except:
            print(u'\u001b[41;1m[ERROR/Stock: {}]\u001b[0m failed to connect source'.format(self.__code))
        finally:
            return df
    
    def trend(self):
        df = self.__data

        df['turn'] = pd.Series(index = df.index).fillna(0)
        lastPoint = 0

        if not self.__archiveData.empty: 
            df.at[0, 'turn'] = self.__archiveData['turn'].iloc[-1]

        try:
            ''' Loop each stick '''
            for i in range(1, len(df) - 1):
                pH = df.iloc[i - 1]['high']
                pL = df.iloc[i - 1]['low']
                cH = df.iloc[i]['high']
                cL = df.iloc[i]['low']
                nH = df.iloc[i + 1]['high']
                nL = df.iloc[i + 1]['low']

                ''' Backward check '''
                if ((pH < cH or (pH == cH and pL >= cL)) and cH >= nH) or ((cL < pL or (cL == pL and pH <= cH)) and cL <= nL):
                    ''' Check is swallowing possible '''
                    pSa = df.iloc[i + 1:].loc[(df['high'] >= cH) & (df['low'] <= cL)]

                    if not pSa.empty:
                        pSa = pSa.iloc[0]
                        
                        if df.iloc[i + 1:pSa.name].loc[(df['high'] > pSa['high']) | (df['low'] < pSa['low'])].empty:
                            continue

                    ''' Check the past '''
                    for j, row in df[:i].loc[df['turn'] != 0][::-1].iterrows():
                        ptH = row['high']
                        ptL = row['low']

                        if ptH <= cH and ptL >= cL:
                            df.at[j, 'turn'] = 0
                            continue
                        else:
                            lastPoint = j
                            break

                ''' re-V type check '''
                if (pH < cH or (pH == cH and pL >= cL)) and cH >= nH:
                    ok = True

                    for x in range(lastPoint, i): 
                        pH = df.iloc[x]['high']
                        pL = df.iloc[x]['low']

                        if pH > cH or (pH == cH and pL < cL) or (len(df['turn'] != 0) == 0 and (cH >= df[['high']].iloc[0:i]).any().bool() and (cL <= df[['low']].iloc[0:i]).any().bool()):
                            ok = False
                            break

                    if ok:
                        for x in range(i + 1, len(df)):
                            nH = df.iloc[x]['high']
                            nL = df.iloc[x]['low']

                            if nH > cH or (nH == cH and nL <= cL):
                                break

                            if nH < cH and cL > nL:
                                if lastPoint == 0:
                                    for y in range(0, i):
                                        if df['low'].iloc[y] <= cL:
                                            df.at[y, 'turn'] = -1
                                            break

                                df.at[i, 'turn'] = 1
                                lastPoint = i
                                break

                ''' V type check '''
                if(cL < pL or (cL == pL and pH <= cH)) and cL <= nL:
                    ok = True

                    for x in range(i - 1, lastPoint - 1, -1):
                        pH = df.iloc[x]['high']
                        pL = df.iloc[x]['low']

                        if pL < cL or (pL == cL and pH > cH) or (len(df['turn'] != 0) == 0 and (cH >= df[['high']].iloc[0:i]).any().bool() and (cL <= df[['low']].iloc[0:i]).any().bool()): #若任意一根K棒的低點小於目標K棒，則絕對invalid
                            ok = False
                            break

                    if ok:
                        for x in range(i + 1, len(df)):
                            nH = df.iloc[x]['high']
                            nL = df.iloc[x]['low']

                            if nL < cL or (cL == nL and nH >= cH):
                                break

                            if nH > cH and nL > cL:
                                if lastPoint == 0:
                                    for y in range(0, i):
                                        if df['high'].iloc[y] >= cH:
                                            df.at[y, 'turn'] = 1
                                            break

                                df.at[i, 'turn'] = -1
                                lastPoint = i
                                break
        except:
            print(u'\u001b[41;1m[ERROR/Stock: {}]\u001b[0m failed to trend'.format(self.__code))
        finally:
            return df

    def circle(self):
        df = self.__data

        tpDF = df.loc[df['turn'] != 0].reset_index()
        df['kT'] = pd.Series(index = df.index).fillna(0)
        df['dT'] = pd.Series(index = df.index).fillna(0)

        if not self.__archiveData.empty:
            if self.__archiveData['kT'].iloc[-1] == 1:
                df.at[0, 'kT'] = 1
            elif self.__archiveData['dT'].iloc[-1] == 1:
                df.at[0, 'dT'] = 1

        try:
            for i, row in tpDF.iterrows():
                if i == 0:
                    continue

                pK = tpDF.iloc[i - 1]

                if row['turn'] == 1:
                    if (pK['low'] > df[['close']].iloc[pK['index']:]).any().bool():
                        df.at[row['index'], 'kT'] = 1
                else:
                    if (pK['high'] < df[['close']].iloc[pK['index']:]).any().bool():
                        df.at[row['index'], 'dT'] = 1
        except:
            print(u'\u001b[41;1m[ERROR/Stock: {}]\u001b[0m failed to circle'.format(self.__code))
        finally:
            return df

    def validate(self):
        df = self.__data
        ktDF = df.loc[df['kT'] == 1].reset_index(drop=True)
        dtDF = df.loc[df['dT'] == 1].reset_index(drop=True)

        try:
            if len(ktDF) > 1:
                #df.loc[df['high'] < float(df['close'].iloc[df[['close']].iloc[ktDF.iloc[len(ktDF) - 1]['index']:].idxmax()]), 'kT'] = 0 #最後一個Drop的K棒之後的最高收盤價高於任何Drop圈圈，則該圈圈invalid

                for i, row in ktDF.iloc[len(ktDF) - 2::-1].iterrows():
                    cH = row['high']
                    nH = ktDF.iloc[i + 1]['high']

                    if cH <= nH:
                        df.loc[df['date'] == row['date'], 'kT'] = 0

            if len(dtDF) > 1:
                #df.loc[df['low'] > float(df['close'].iloc[df[['close']].iloc[dtDF.iloc[len(dtDF) - 1]['index']:].idxmin()]), 'dT'] = 0 #最後一個Rise的K棒之後的最低收盤價低於任何Rise圈圈，則該圈圈invalid
                
                for i, row in dtDF.iloc[len(dtDF) - 2::-1].iterrows():
                    cL = row['low']
                    nL = dtDF.iloc[i + 1]['low']

                    if cL >= nL:
                        df.loc[df['date'] == row['date'], 'dT'] = 0

            df = pd.concat([self.__archiveData, df], sort=False) \
            .drop_duplicates(subset=['date'], keep='last') \
            .sort_values(by=['date']) \
            .reset_index(drop=True) if not self.__archiveData.empty else df    
        except:
            print(u'\u001b[41;1m[ERROR/Stock: {}]\u001b[0m failed to validate'.format(self.__code))
        finally:
            return df

    def estimate(self):
        df = self.__data
        result = ''

        try:
            ''' Get the last stick '''
            cK = df.iloc[-1]

            ''' Check is all types of turn existed '''
            lkT = df.loc[df['turn'] == 1].iloc[[-1]].index if len(df.loc[df['turn'] == 1]) > 0 else -1 #若有倒v轉折點，則選最後一個點；若無，則 -1
            ldT = df.loc[df['turn'] == -1].iloc[[-1]].index if len(df.loc[df['turn'] == -1]) > 0 else -1 #若有v轉折點，則選最後一個點；若無，則 -1

            if lkT == -1 or ldT == -1:
                result = 'invalid'

                raise ValueError('either one of turns is empty')

            ''' Get all re-V turns and V turns separately '''
            lkT = df.loc[df['turn'] == 1]
            ldT = df.loc[df['turn'] == -1]

            ''' Checl is one of circles existed '''
            lKT = df.loc[df['kT'] == 1].iloc[[-1]].index if not (df.loc[df['kT'] == 1]).empty else -1 #若Drop突破有圈，則選最後一個圈；若無，則 -1
            lDT = df.loc[df['dT'] == 1].iloc[[-1]].index if not (df.loc[df['dT'] == 1]).empty else -1 #若RiseBT有圈，則選最後一個圈；若無，則 -1

            if lKT == -1 and lDT == -1:
                result = 'toss'

                raise ValueError('no circle exists')

            ''' Get the last circle '''
            lP = df.iloc[max(lKT, lDT)]

            ''' Get all types of circles separately '''
            lKT = df.loc[df['kT'] == 1]
            lDT = df.loc[df['dT'] == 1]


            ndT = nkT = tDT = tKT = pd.DataFrame([])

            ''' Calculate Drop type circles if needed '''
            if (not os.path.exists(self.__archivePath) or int(lP['turn']) == 1):
                if not lDT.empty:
                    ndT = lDT.loc[lDT.date <= lDT.iloc[-1].date]

                    for i, r in ndT.iloc[::-1].iterrows():
                        if not ndT.loc[ndT.date < r.date].empty:
                            pI = ndT.loc[ndT.date < r.date].iloc[-1]
                        else:
                            break

                        rP = ldT.loc[ldT.date > pI.date]
                        rP = rP.loc[rP.date < r.date]
                        ndT = ndT.append(rP)

                    ndT = ndT.append(ldT.loc[ldT.date < ndT.date.iloc[0]], ignore_index = True)
                    ndT = ndT.reset_index(drop=True)

                    for i, r in ndT.iterrows():
                        if (float(r['low']) > df[['close']].loc[(df.date > r.date) & (df.date < df.date.iloc[-1])]).any().bool():
                            ndT = ndT.drop([ndT.loc[ndT.date == r.date].index[0]])

                ndT = ndT.append(ldT.loc[ldT.date > lDT.date.iloc[-1]] if not lDT.empty else ldT).sort_values(by = ['date'])
                tDT = ndT.loc[ndT['low'] > float(cK['close'])].sort_values(by = ['low']).iloc[0] if len(ndT.loc[ndT['low'] > float(cK['close'])]) > 0 else pd.DataFrame([])

            ''' Calculate Rise type circles if needed '''
            if (not os.path.exists(self.__archivePath) or int(lP['turn']) == -1):
                if not lKT.empty:
                    nkT = lKT.loc[lKT.date <= lKT.iloc[-1].date]
                    
                    for i, r in nkT.iloc[::-1].iterrows():
                        if not nkT.loc[nkT.date < r.date].empty:
                            pI = nkT.loc[nkT.date < r.date].iloc[-1]
                        else:
                            break

                        rP = lkT.loc[lkT.date > pI.date]
                        rP = rP.loc[rP.date < r.date]
                        nkT = nkT.append(rP)
                    
                    nkT = nkT.append(lkT.loc[lkT.date < nkT.date.iloc[0]], ignore_index = True)
                    nkT = nkT.reset_index(drop=True)
                
                    for i, r in nkT.iterrows():
                        if (float(r['high']) < df[['close']].loc[(df.date > r.date) & (df.date < df.date.iloc[-1])]).any().bool():
                            nkT = nkT.drop([nkT.loc[nkT.date == r.date].index[0]])

                nkT = nkT.append(lkT.loc[lkT.date > lKT.date.iloc[-1]] if not lKT.empty else lkT).sort_values(by = ['date'])
                tKT = nkT.loc[nkT['high'] < float(cK['close'])].sort_values(by = ['high'], ascending = False).iloc[0] if len(nkT.loc[nkT['high'] < float(cK['close'])]) > 0 else pd.DataFrame([])

            ''' Save the result of calculation if permitted '''
            if self.__modifyArchive:
                (pd.concat([
                    ndT.loc[(((ndT.low <= tDT.low) if not tDT.empty else True) & (ndT.date < cK.date)) | (ndT.date >= cK.date)] if not ndT.empty else df.loc[df['turn'] == -1],
                    nkT.loc[(((nkT.high >= tKT.high) if not tKT.empty else True) & (nkT.date < cK.date)) | (nkT.date >= cK.date)] if not nkT.empty  else df.loc[df['turn'] == 1],
                    df.loc[(df['kT'] != 0) | (df['dT'] != 0)].iloc[[-2]] if len(df.loc[(df['kT'] != 0) | (df['dT'] != 0)]) >= 2 else df.iloc[[0]]
                ]).reset_index(drop=True)).to_csv(self.__archivePath, encoding='utf_8')

            '''if not self.__archiveData.empty and not self.__archiveData.equals(pd.read_csv(self.__archivePath, index_col=0).iloc[:-1]):
                print(str(self.__code) + " is changed.")'''

            ''' Determine the state of the stock by comparing the close and the closest potential break point '''
            if int(lP['turn']) == 1:
                if lDT.empty and tDT.empty:
                    for x in range(2):
                        tK = df.loc[(df.date < lKT.date.iloc[-1]) & (df['turn'] == -1)]

                        if tK.empty:
                            self.__data = self.download(all=True)
                            self.__data = self.trend()
                            self.__data = self.circle()
                            self.__data = self.validate()
                            df = self.__data

                    if tK.empty:
                        result = 'invalid'

                        raise ValueError('tK is empty')
                    else:
                        tK = tK.iloc[-1]
                else:
                    tK = tDT

                if not tK.empty and not (float(tK['low']) > df[['close']].loc[(df.date > tK.date) & (df.date < df.date.iloc[-1])]).any().bool():
                    self.result['break_date'] = tK['date']

                    result = 'DropFT'
                else:
                    result = 'Drop'
            else:
                if lKT.empty and tKT.empty:
                    for x in range(2):
                        tK = df.loc[(df.date < lDT.date.iloc[-1]) & (df['turn'] == 1)]

                        if tK.empty:
                            self.__data = self.download(all=True)
                            self.__data = self.trend()
                            self.__data = self.circle()
                            self.__data = self.validate()
                            df = self.__data

                    if tK.empty:
                        result = 'invalid'

                        raise ValueError('tK is empty')
                    else:
                        tK = tK.iloc[-1]
                else:
                    tK = tKT

                if not tK.empty and not (float(tK['high']) < df[['close']].loc[(df.date > tK.date) & (df.date < df.date.iloc[-1])]).any().bool():
                    self.result['break_date'] = tK['date']

                    result = 'RiseBT'
                else:
                    result = 'Rise'
        except ValueError as e:
            print(u'\u001b[41;1m[ERROR/Stock: {}]\u001b[0m waived to estimate because {}'.format(self.__code, e))
        except:
            print(u'\u001b[41;1m[ERROR/Stock: {}]\u001b[0m failed to estimate'.format(self.__code))
        finally:
            return result

    def getMean(self, days):
        df = self.__data
        columnName = 'ma_{}'.format(days)

        try:
            df[columnName] = df['close'].rolling(days).mean()
            df[columnName] = df[columnName].map(lambda x: round(round(x, 3), 2))
        except:
            print(u'\u001b[41;1m[ERROR/Stock: {}]\u001b[0m failed to calculate mean {}'.format(self.__code, days))
        finally:
            return df