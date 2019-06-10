import requests
import json
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import time
from binance.client import Client # Import the Binance Client
from binance.websockets import BinanceSocketManager # Import the Binance Socket Manager
import pymongo

class GetCrypto(object):

    def __init__(self, db_host='192.168.5.103:27017/', db_name='crypto_db', 
                    secrets_filepath='../binance-secrets.txt'):
        self.db_host = 'mongodb://' + db_host
        self.db_name = db_name
        self.secrets_filepath = secrets_filepath
        self.myclient = pymongo.MongoClient(self.db_host)
        self.mydb = self.myclient[self.db_name]

        with open(self.secrets_filepath, 'r') as file:
            content = file.read()
            self.apiKey = content.split()[2]
            self.secret = content.split()[5]


    def milliseconds_to_datetime(self, x):
        return datetime.fromtimestamp(x/1000.0).strftime("%m/%d/%Y, %H:%M:%S")


    def data_cleaner(self, data):
        df = pd.DataFrame(data, columns = ['Open Time', 'Open', 'High', 'Low', 
                                        'Close', 'Volume', 'Close Time', 'Quote asset volume',
                                        'Number of trades', 'Taker buy base asset volume',
                                        'Taker buy quote asset volume', 'Ignore'])
        df['High'] = df['High'].astype(np.float32)
        df['Low'] = df['Low'].astype(np.float32)
        df['Volume'] = df['Volume'].astype(np.float32)
        df['Open'] = df['Open'].astype(np.float32)
        df['Close'] = df['Close'].astype(np.float32)
        df['Quote asset volume'] = df['Quote asset volume'].astype(np.float32)
        df['Number of trades'] = df['Number of trades'].astype(np.float32)
        df['Taker buy base asset volume'] = df['Taker buy base asset volume'].astype(np.float32)

        df['Open Time'] = df['Open Time'].map(self.milliseconds_to_datetime)
        df['Open Time'] = pd.to_datetime(df['Open Time'])
        df['Close Time'] = df['Close Time'].map(self.milliseconds_to_datetime)
        df['Close Time'] = pd.to_datetime(df['Close Time'])
        
        return df


    def update_db(self, coin):
        '''
        :description - Given a coin abbrev. this will check for a similarly named collection,
                       pull the latest records for that coin, and update the table with whatever
                       data is missing.
        '''
        max_ = self.mydb[coin].find_one(sort=[("Close Time", -1)])["Close Time"]
        
        symbol = f'{coin}USDT'
        interval = Client.KLINE_INTERVAL_1MINUTE
        
        start = max_.strftime("%m/%d/%Y, %H:%M:%S") + ' EDT'
        end = datetime.now().strftime("%m/%d/%Y, %H:%M:%S") + ' EDT'

        
        if (datetime.now() - max_).seconds // 60:
        
            client = Client(api_key=self.apiKey, api_secret=self.secret)

            klines = client.get_historical_klines(symbol, interval, start, end)
            klines = self.data_cleaner(klines)

            self.mydb[coin].insert_many(klines.to_dict('records'))
            
        else:
            print(f'{max_} | {datetime.now()}')
            print('too soon...')


if __name__ == '__main__':
    crypto_getter = GetCrypto()

    for coin in crypto_getter.mydb.list_collection_names():
        print(f'Updating: {coin}')
        crypto_getter.update_db(coin)

    print('Finished updating!!!')