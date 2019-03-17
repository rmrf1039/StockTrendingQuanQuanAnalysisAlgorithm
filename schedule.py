#!/usr/bin/env python
# coding: utf-8

from datetime import datetime, timedelta
from src.stock import Stock
from src.track import TrackStock, isHoliday

import pandas as pd
import multiprocessing as mp
import configparser
import sys
import os
import time
import glob

''' Decode Config '''
config = configparser.ConfigParser()
config.read('./config/config.ini')

resultDir = config['Dir']['Result']

try:
    terminalCols, terminalRows = os.get_terminal_size(0)
except OSError:
    terminalCols, terminalRows = os.get_terminal_size(1)

def dayilyCalc(x):
    return TrackStock().dailyCalc(x['code'], x['preStat'], x['date'])

def weeklyCalc(x):
    return TrackStock().weeklyCalc(x['code'], x['date'])

#Time Counting Declaration
ts = time.time()

if __name__ == '__main__':
    startDate = datetime(2019, 3, 15).replace(hour=0, minute=0, second=0, microsecond=0)
    endDate = datetime(2019, 3, 15).replace(hour=0, minute=0, second=0, microsecond=0)

    print("\n" + u"\u001b[1m\u001b[4m\u001b[7m C.C Stock Analysis Program \u001b[0m".center(terminalCols) + "\n")

    for d in range((endDate - startDate).days, -1, -1):
        currentDate = (endDate - timedelta(days=d))
        resultFilePath = resultDir + 'result_' + currentDate.strftime('%Y-%-m-%d') + '.csv'

        #Only Work when available
        if not isHoliday(currentDate):
            df = pd.DataFrame([])

            ''' Daily K Calculation '''
            if not os.path.exists(resultDir + 'result_tmp_' + currentDate.strftime('%Y-%-m-%d') + '.csv') and not not glob.glob(resultDir + '*.csv'):
                lastResultDate = currentDate

                while True:
                    lastResultDate -= timedelta(days=1)

                    if not isHoliday(lastResultDate):
                        break

                lastResultPath = resultDir + 'result_' + lastResultDate.strftime('%Y-%-m-%d') + '.csv'
                
                ''' Repeat File Deletion '''
                if os.path.exists(resultFilePath):
                    os.remove(resultFilePath)
                    print(u"\u001b[41;1m[WARNING/Master]\u001b[0m removing overlaped file: " + os.path.basename(resultFilePath))

                print(u"[\u001b[1mINFO/Master\u001b[0m] start daily analysis, " + currentDate.strftime('%Y-%-m-%d'))
                print(u"[\u001b[1mINFO/Master\u001b[0m] reading " + lastResultPath)
                codes = pd.read_csv(lastResultPath, index_col=0)
                codes = codes.loc[(codes['state'] == 'Rise') | (codes['state'] == 'Drop')]

                ''' Multiple Processing '''
                with mp.Pool() as pool:
                    jobs = pool.map_async(dayilyCalc, \
                        [{'code': int(codes['code'].iloc[i]), 'preStat': codes['state'].iloc[i], 'date': currentDate} for i in range(len(codes))], \
                        int(len(codes) / mp.cpu_count() / 2) + 1)

                    pool.close()
                    pool.join()

                    if jobs.ready():
                        df = pd.DataFrame(jobs.get(), columns=['code', 'state', 'gain_rate'])

                        if not df.empty:
                            df = pd.concat([
                                df.loc[df['state'] == 'RiseBT'].sort_values(by=['gain_rate'], ascending=False),
                                df.loc[df['state'] == 'DropFT'].sort_values(by=['gain_rate'], ascending=False),
                                df.loc[(df['state'] == 'Rise') | (df['state'] == 'Drop')].sort_values(by=['code'])
                            ])

                            df.to_csv(resultDir + 'result_' + ('tmp_' if isHoliday((currentDate + timedelta(days=1))) else '') + currentDate.strftime('%Y-%-m-%d') + '.csv', encoding='utf_8')
                            print(u"[\u001b[32;1mDone/Master\u001b[0m] successfully analyzed daily calculations")
                        else:
                            print(u"\u001b[41;1m[ERROR/Master]\u001b[0m failed to calculate date, " + currentDate.strftime('%Y-%-m-%d') + ", analysis")

            ''' Weekly K Calculation '''
            if isHoliday((currentDate + timedelta(days=1))):
                print(u"[\u001b[1mINFO/Master\u001b[0m] continue calculation, start weekly analysis")

                codes = pd.read_csv(config['Codes']['CsvFile'])['code']

                ''' Multiple Processing '''
                with mp.Pool() as pool:
                    jobs = pool.map_async(weeklyCalc, \
                        [{'code': int(codes.iloc[i]), 'date': currentDate} for i in range(len(codes))], \
                        int(len(codes) / mp.cpu_count() / 2) + 1)

                    pool.close()
                    pool.join()

                    if jobs.ready():
                        df = pd.DataFrame(jobs.get(), columns=['code', 'state', 'gain_rate'])

                        if not df.empty:
                            df = pd.concat([pd.read_csv(resultDir + 'result_tmp_' + currentDate.strftime('%Y-%-m-%d') + '.csv', index_col=0), 
                            df.loc[(df['state'] != 'Unfit') & (df['state'] != 'fail')]], sort=False).drop_duplicates(['code'], keep='last')
                            df = pd.concat([
                                df.loc[df['state'] == 'SynBT'].sort_values(by=['gain_rate'], ascending=False),
                                df.loc[df['state'] == 'SynFT'].sort_values(by=['gain_rate'], ascending=False),
                                df.loc[df['state'] == 'RiseBT'].sort_values(by=['gain_rate'], ascending=False),
                                df.loc[df['state'] == 'DropFT'].sort_values(by=['gain_rate'], ascending=False),
                                df.loc[(df['state'] == 'Rise') | (df['state'] == 'Drop')].sort_values(by=['code'])
                            ])
                            df['gain_rate'] = df['gain_rate'].map(lambda x: round(x, 3))

                            df.to_csv(resultDir + 'result_' + currentDate.strftime('%Y-%-m-%d') + '.csv', encoding='utf_8')
                            os.remove(resultDir + 'result_tmp_' + currentDate.strftime('%Y-%-m-%d') + '.csv')
                            print(u"[\u001b[32;1mDone/Master\u001b[0m] successfully analyzed weekly calculations")
                        else:
                            print(u"\u001b[41;1m[ERROR/Master]\u001b[0m failed to calculate date, " + currentDate.strftime('%Y-%-m-%d') + ", analysis")
            
            ''' Service Boardcast '''
            if not df.empty:
                '''

                SQL Injection

                '''
        else:
            print(u"\u001b[41;1m[ERROR/Master]\u001b[0m Stock market is not available in this date, " + currentDate.strftime('%Y-%-m-%d'))

#Ending Area
te = time.time()
td = te - ts
print("\n運行時間: ", td , "秒.")