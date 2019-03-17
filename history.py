from multiprocessing import Queue
from datetime import datetime, timedelta
from dateutil.parser import parse

import pandas as pd
import numpy as np
import multiprocessing as mp
import configparser
import time
import os
import json
import redis

from src.stock import Stock
from src.track import *

''' Decode Config '''
config = configparser.ConfigParser()
config.read('./config/config.ini')

resultDir = config['Dir']['Result']
resourcesDir = config['Dir']['Resource']

def calc(x):
    try:
        res = TrackStock().historicalCalc(x['code'], x['date'])
    except:
        print(u"\u001b[41;1m[ERROR/Worker:" + str(x['code']) + "]\u001b[0m failed to call Track class")
        res = {'code': x['code'], 'state': 'failed calling'}

    r = redis.Redis(host='localhost', port=6379, db=0)
    r.set(str(x['code']) + x['date'].strftime('%m%d'), str(res))

    return res

def download(x):
    try:
        weekilyData = Stock(x['code'], 'w', x['date'], download_only = True).download(all = True)
        dailyData = Stock(x['code'], 'd', x['date'], download_only = True).download(all = True)

        if not weekilyData.empty and not dailyData.empty:
            weekilyData.to_csv(x['resourcesDir'] + str(x['code']) + '_w.csv', encoding='utf_8')
            dailyData.to_csv(x['resourcesDir'] + str(x['code']) + '_d.csv', encoding='utf_8')

            return True
        
        raise ValueError('data is not downloaded completely')
    except:
        print(u"\u001b[41;1m[ERROR/Worker:" + str(x['code']) + "]\u001b[0m failed to download the resource")

        return False

#Time Counting Declaration
ts = time.time()

if __name__ == '__main__':
    date = datetime(2019, 2, 27).replace(hour = 0, minute = 0, second = 0, microsecond = 0) #目前日期

    #Processing
    if not isHoliday(date):
        r = redis.Redis(host='localhost', port=6379, db=0)
        codes = pd.read_csv(config['Codes']['CsvFile'])['code']

        ''' Prepare materials '''
        resourcesDir = resourcesDir + date.strftime('%Y-%-m-%d') + '/'

        if not os.path.exists(resourcesDir):
            os.makedirs(resourcesDir)

        r.flushdb()

        ''' Multiple Processing '''
        with mp.Pool() as pool:
            jobs = pool.map_async(download, \
                [{'code': int(codes.iloc[i]), 'date': date, 'resourcesDir': resourcesDir} for i in range(len(codes))], \
                int(len(codes) / mp.cpu_count()) + 1)

            pool.close()
            pool.join()

        ''' Multiple Processing '''
        with mp.Pool() as pool:
            jobs = pool.map_async(calc, \
                [{'code': int(codes.iloc[i]), 'date': date} for i in range(len(codes))], \
                int(len(codes) / mp.cpu_count() / 2) + 1)

            pool.close()
            pool.join()

            if jobs.ready():
                df = pd.DataFrame(columns = ['code', 'state', 'gain_rate'])

                for code in codes:
                    data = r.get(str(code) + date.strftime('%m%d'))

                    if data != None and data != 'None':
                        df = df.append(json.loads(data.replace("'", '"')), ignore_index=True)

                if not df.empty:
                    df = pd.concat([
                        df.loc[df['state'] == 'SynBT'].sort_values(by=['gain_rate'], ascending=False),
                        df.loc[df['state'] == 'SynFT'].sort_values(by=['gain_rate'], ascending=False),
                        df.loc[df['state'] == 'RiseBT'].sort_values(by=['gain_rate'], ascending=False),
                        df.loc[df['state'] == 'DropFT'].sort_values(by=['gain_rate'], ascending=False),
                        df.loc[(df['state'] == 'Rise') | (df['state'] == 'Drop')].sort_values(by=['code'])
                    ])
                    df['gain_rate'] = df['gain_rate'].map(lambda x: round(x, 3))

                    df.to_csv(resultDir + 'result_' + date.strftime('%Y-%-m-%d') + '.csv', encoding='utf_8')
                    print(u"[\u001b[32;1mDone/Master\u001b[0m] successfully analyzed historical calculations")
                else:
                    print(u"\u001b[41;1m[ERROR/Master]\u001b[0m failed to calculate date, " + date.strftime('%Y-%-m-%d') + ", analysis")
    else:
        print(u"\u001b[41;1m[ERROR/Master]\u001b[0m Stock market is not available in this date, " + date.strftime('%Y-%-m-%d'))

#Ending Area
te = time.time()
td = te - ts
print("\n運行時間: ", (td/60) , "分.")
