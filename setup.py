import configparser
import os

config = configparser.ConfigParser()

config['Dir'] = {}
pathConfig = config['Dir']
pathConfig['Root'] = os.path.abspath(os.curdir)
pathConfig['Config'] = pathConfig['Root'] + '/config/'
pathConfig['Archive'] = pathConfig['Root'] + '/data/archive/'
pathConfig['Resource'] = pathConfig['Root'] + '/data/resource/'
pathConfig['Result'] = pathConfig['Root'] + '/data/result/'
pathConfig['Log'] = pathConfig['Root'] + '/log/'

config['Codes'] = {}
cfgConfig = config['Codes']
cfgConfig['CsvFile'] = pathConfig['Config'] + '/stock_name.csv'

config['Schedule'] = {}
scheduleConfig = config['Schedule']
scheduleConfig['CsvFile'] = pathConfig['Config'] + '/holidays.csv'

with open(pathConfig['Config'] + 'config.ini', 'w') as configfile:
    config.write(configfile)