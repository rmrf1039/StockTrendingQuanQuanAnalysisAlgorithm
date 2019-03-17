#!/usr/bin/env python
# coding: utf-8

from datetime import datetime, timedelta
from dateutil.parser import parse
from .stock import Stock

import pandas as pd
import configparser
import time
import os

def isHoliday(date, holidayFilePath = os.path.join(os.path.dirname(__file__), '../config/holidays.csv')):
    configDf = pd.read_csv(holidayFilePath, index_col=0)

    if date.strftime('%Y/%-m/%-d') in configDf.index and (date.weekday() >= 5 or configDf['isHoliday'].loc[date.strftime('%Y/%-m/%-d')] == '是'):
        return True

    return False

class TrackStock():
    def __init__(self, configPath=os.path.join(os.path.dirname(__file__), '../config/config.ini')):
        ''' Decode Config '''
        config = configparser.ConfigParser()
        config.read(configPath)
        
        ''' Define all the default path for each functional folder '''
        self.__savesDir = config['Dir']['Archive']
        self.__resourcesDir = config['Dir']['Resource']
        self.__configDir = config['Dir']['Config']

    def dailyCalc(self, code, preStat, date=datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)):
        currentTime = datetime.now()

        try:
            ''' Run single daily stock analysis. If the time is in the opening time, record file won't save '''
            stock = Stock(code, 'd', date, modifyArchive=True if currentTime.hour < 9 or currentTime.hour >= 14 else False)
            state = stock.result['state']

            if state == 'fail':
                raise RuntimeError

            ''' Transform real state to camo-state '''
            if preStat == 'Rise' and state != 'RiseBT':
                state = 'Rise'
            elif preStat == 'Drop' and state != 'DropFT':
                state = 'Drop'

            return {'code': code, 'state': state, 'gain_rate': stock.result['gain_rate']}
        except:
            print(u'\u001b[41;1m[ERROR/Track: {}]\u001b[0m failed to process daily analysis'.format(code))

            return {'code': code, 'state': 'Daily analysis failed'}

    def weeklyCalc(self, code, date = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)):
        currentTime = datetime.now()

        try:
            stock = Stock(code, 'w', date, modifyArchive=True if currentTime.hour < 9 or currentTime.hour >= 14 else False)

            if stock.result['state'] == 'RiseBT' or stock.result['state'] == 'DropFT':
                try:
                    stock = Stock(code, 'd', date)
                    state = stock.result['state']

                    if state == 'RiseBT':
                        state = 'SynBT'
                    elif state == 'DropFT':
                        state = 'SynFT'

                    return {'code': code, 'state': state, 'gain_rate': stock.result['gain_rate']}
                except:
                    raise
            else:
                return {'code': code, 'state': 'Unfit'}
        except:
            print(u'\u001b[41;1m[ERROR/Track: {}]\u001b[0m failed to process weekly analysis'.format(code))

            return {'code': code, 'state': 'Weekly analysis failed'}

    def historicalCalc(self, code, date = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)):
        ''' Define the local resources dir path '''
        resourcesDir = self.__resourcesDir + date.strftime('%Y-%-m-%d') + '/'
        
        try:
            ''' Get resources from local '''
            weekDf = pd.read_csv(resourcesDir + str(code) + '_w.csv', index_col=0)
            dayDf = pd.read_csv(resourcesDir + str(code) + '_d.csv', index_col=0)
        except:
            ''' Get resources from network '''
            weekDf = Stock(code, 'w', date, download_only = True).download(all = True)
            dayDf = Stock(code, 'd', date, download_only = True).download(all = True)

        ''' Make sure the resources are all prepared. None of the resouces are empty '''
        if weekDf.empty or dayDf.empty:
            print(u'\u001b[41;1m[ERROR/Track: {}]\u001b[0m resources are incomplete, stop analyzing the stock'.format(code))

            return {'code': code, 'state': 'Missing the source'}

        intermediateDate = None
        
        try:
            ''' Loop each week from the finishing date to the beginning. It stops when break appears '''
            for w in range(len(weekDf) + (-1 if not isHoliday(date + timedelta(days = 1)) else 0), -1, -1):
                stock = weeklyStockIns = Stock(code, 'w', date, source = weekDf.iloc[:w].reset_index(drop=True), modifyArchive=False)

                if stock.result['state'] == 'fail':
                    raise RuntimeError

                if stock.result['state'] == 'RiseBT' or stock.result['state'] == 'DropFT':
                    intermediateDate = stock.result['last_date']

                    break

            ''' Make weekly save record '''
            Stock(code, 'w', date, source = weekDf)
        except:
            print(u'\u001b[41;1m[ERROR/Track: {}]\u001b[0m failed to process weekly analysis'.format(code))

            return {'code': code, 'state': 'Weekly analysis failed'}

        try:
            ''' Make sure the stock has week break, so it can process further for daily analysis '''
            if intermediateDate != None:
                intermediateDate = parse(intermediateDate) if type(intermediateDate) != datetime else intermediateDate
                print(intermediateDate)
                ''' Find the end of day that has opened stock market in that week '''
                while not isHoliday(intermediateDate + timedelta(days = 1)):
                    intermediateDate += timedelta(days = 1)

                ''' Loop each day from the week its break to the finishing date '''
                for d in range((date - intermediateDate).days, -1, -1):
                    daySourceEndIndex = (date - timedelta(days = d))
            
                    ''' Check is the processing date in the resource '''
                    if isHoliday(daySourceEndIndex) or not dayDf['date'].isin([daySourceEndIndex.strftime('%Y-%m-%d')]).any():
                        continue

                    ''' Find the corresponding index to current processing date '''
                    daySourceEndIndex = dayDf.loc[dayDf['date'] == daySourceEndIndex.strftime('%Y-%m-%d')].index[0] + 1
                    
                    ''' Find the beginning index of range of the mandatory resource '''
                    if os.path.exists(self.__savesDir + 'save_' + str(code) + '_d.csv'):
                        tempResourceStartDate = parse(pd.read_csv(self.__savesDir + 'save_' + str(code) + '_d.csv', index_col=0)['date'].iloc[-1])
                        
                        while isHoliday(tempResourceStartDate) or not dayDf['date'].isin([tempResourceStartDate.strftime('%Y-%m-%d')]).any():
                            tempResourceStartDate -= timedelta(days = 1)
                            
                        daySourceStartIndex = dayDf.loc[dayDf['date'] == tempResourceStartDate.strftime('%Y-%m-%d')].index[0]
                    else:
                        daySourceStartIndex = 0
                    
                    try:
                        stock = Stock(code, 'd', date, source = dayDf.iloc[daySourceStartIndex:daySourceEndIndex].reset_index(drop=True))
                        state = stock.result['state']

                        if state == 'fail':
                            raise RuntimeError

                        ''' Work on the business logic. If it breaks early, it doesn't follow the core '''
                        if d != 0 and ((weeklyStockIns.result['state'] == 'RiseBT' and state == 'RiseBT') or (weeklyStockIns.result['state'] == 'DropFT' and state == 'DropFT')):
                            
                            return {'code': code, 'state': 'cancel'}
                        elif d == 0:
                            if weeklyStockIns.result['state'] == 'RiseBT' and state != 'RiseBT':
                                state = 'Rise'
                            elif weeklyStockIns.result['state'] == 'DropFT' and state != 'DropFT':
                                state = 'Drop'

                            if intermediateDate == date:
                                if state == 'RiseBT':
                                    state = 'SynBT'
                                elif state == 'DropFT':
                                    state = 'SynFT'
                                    
                            return {'code': code, 'state': state, 'gain_rate': stock.result['gain_rate']}
                    except:
                        print(u'\u001b[41;1m[ERROR/Track: {}]\u001b[0m failed to process daily analysis'.format(code))

                        return {'code': code, 'state': 'Daily analysis failed'}
            else:
                raise ValueError('intermediateDate')
        except ValueError as e:
            print(u'\u001b[41;1m[ERROR/Track: {}]\u001b[0m missing {}'.format(code, e))

            return {'code': code, 'state': 'missing intermediate data'}