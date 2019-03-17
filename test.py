from datetime import datetime, timedelta
from dateutil.parser import parse

import pandas as pd
import numpy as np
import time
import os
import glob
import smtplib
import configparser

from src.stock import Stock
from src.track import TrackStock

''' Decode Config '''
config = configparser.ConfigParser()
config.read('./config/config.ini')

try:
    terminalCols, terminalRows = os.get_terminal_size(0)
except OSError:
    terminalCols, terminalRows = os.get_terminal_size(1)

#Time Counting Declaration
ts = time.time()

if __name__ == '__main__':
    print("\n" + u"\u001b[1m\u001b[4m\u001b[7m C.C Stock Analysis Test Program \u001b[0m".center(terminalCols) + "\n")
    date = datetime(2019, 3, 15).replace(hour = 0, minute = 0, second = 0, microsecond = 0)
    print(date)

    code = input("Search: ")
    
    print(u"[\u001b[32:1mDONE\u001b[0m] Result: " + str(TrackStock().historicalCalc(code, date)))

    if os.path.exists(config['Dir']['Archive'] + 'save_' + str(code) + '_w.csv'):
        os.remove(config['Dir']['Archive'] + 'save_' + str(code) + '_w.csv')

    if os.path.exists(config['Dir']['Archive'] + 'save_' + str(code) + '_d.csv'):
        os.remove(config['Dir']['Archive'] + 'save_' + str(code) + '_d.csv')

#Ending Area
te = time.time()
td = te - ts
print("\n運行時間: ", td , "秒.")
