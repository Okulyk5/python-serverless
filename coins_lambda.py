 
import requests
import json
import boto3
from datetime import datetime
import os
from pymongo import MongoClient

s3 = boto3.client('s3')

def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port,
                                                  '?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')
        print(mongo_uri)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn

def lambda_handler():
    print()
    bucket = 'coins-historical'

    categories = ['All', 'DeFi', 'Stablecoins', 'Governance']

    coin_lst = []

    for catagory in categories:
        if catagory == 'All':
            for i in range(80):
                r = requests.get(
                    f'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=150&page={i}&sparkline=false&price_change_percentage=1h')
                if r.status_code == 200:
                    print('Executing for token:', catagory)
                    response = r.json()
                    for token in response:
                        insert_token = {}
                        insert_token['category'] ='All'
                        insert_token['coin_id'] = token['id']
                        insert_token['symbol'] = token['symbol']
                        insert_token['name'] = token['name']
                        insert_token['icon'] = token['image']
                        insert_token['current_price'] = token['current_price']
                        insert_token['market_cap'] = token['market_cap']
                        insert_token['market_cap_rank'] = token['market_cap_rank']
                        insert_token['fully_diluted_valuation'] = token['fully_diluted_valuation']
                        insert_token['total_volume'] = token['total_volume']
                        insert_token['high_24h'] = token['high_24h']
                        insert_token['low_24h'] = token['low_24h']
                        insert_token['price_change_24h'] = token['price_change_24h']
                        insert_token['price_change_percentage_24h'] = token['price_change_percentage_24h']
                        insert_token['market_cap_change_24h'] = token['market_cap_change_24h']
                        insert_token['market_cap_change_percentage_24h'] = token['market_cap_change_percentage_24h']
                        insert_token['circulating_supply'] = token['circulating_supply']
                        insert_token['total_supply'] = token['total_supply']
                        insert_token['max_supply'] = token['max_supply']
                        insert_token['ath'] = token['ath']
                        insert_token['ath_change_percentage'] = token['ath_change_percentage']
                        insert_token['ath_date'] = token['ath_date']
                        insert_token['atl'] = token['atl']
                        insert_token['atl_change_percentage'] = token['atl_change_percentage']
                        insert_token['atl_date'] = token['atl_date']
                        insert_token['last_updated'] = token['last_updated']
                       
                        insert_token['createdAt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        coin_lst.append(insert_token)
                else:
                    r.raise_for_status()

        if catagory == 'DeFi':
            for i in range(5):
                r = requests.get(
                    f'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=decentralized_finance_defi&order=market_cap_desc&per_page=100&page={i}&sparkline=false&price_change_percentage=1h')   

                if r.status_code == 200:
                    print('Executing for token:', catagory)
                    response = r.json()
                    for token in response:
                        insert_token = {}
                        insert_token['category'] ='DeFi'
                        insert_token['coin_id'] = token['id']
                        insert_token['symbol'] = token['symbol']
                        insert_token['name'] = token['name']
                        insert_token['icon'] = token['image']
                        insert_token['current_price'] = token['current_price']
                        insert_token['market_cap'] = token['market_cap']
                        insert_token['market_cap_rank'] = token['market_cap_rank']
                        insert_token['fully_diluted_valuation'] = token['fully_diluted_valuation']
                        insert_token['total_volume'] = token['total_volume']
                        insert_token['high_24h'] = token['high_24h']
                        insert_token['low_24h'] = token['low_24h']
                        insert_token['price_change_24h'] = token['price_change_24h']
                        insert_token['price_change_percentage_24h'] = token['price_change_percentage_24h']
                        insert_token['market_cap_change_24h'] = token['market_cap_change_24h']
                        insert_token['market_cap_change_percentage_24h'] = token['market_cap_change_percentage_24h']
                        insert_token['circulating_supply'] = token['circulating_supply']
                        insert_token['total_supply'] = token['total_supply']
                        insert_token['max_supply'] = token['max_supply']
                        insert_token['ath'] = token['ath']
                        insert_token['ath_change_percentage'] = token['ath_change_percentage']
                        insert_token['ath_date'] = token['ath_date']
                        insert_token['atl'] = token['atl']
                        insert_token['atl_change_percentage'] = token['atl_change_percentage']
                        insert_token['atl_date'] = token['atl_date']
                        insert_token['last_updated'] = token['last_updated']
                        
                        insert_token['createdAt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        coin_lst.append(insert_token)
                else:
                    r.raise_for_status()


        if catagory == 'Stablecoins':
            r = requests.get(
                f'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=stablecoins&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=1h')   

            if r.status_code == 200:
                print('Executing for token:', catagory)
                response = r.json()
                for token in response:
                    insert_token = {}
                    insert_token['category'] ='Stablecoins'
                    insert_token['coin_id'] = token['id']
                    insert_token['symbol'] = token['symbol']
                    insert_token['name'] = token['name']
                    insert_token['icon'] = token['image']
                    insert_token['current_price'] = token['current_price']
                    insert_token['market_cap'] = token['market_cap']
                    insert_token['market_cap_rank'] = token['market_cap_rank']
                    insert_token['fully_diluted_valuation'] = token['fully_diluted_valuation']
                    insert_token['total_volume'] = token['total_volume']
                    insert_token['high_24h'] = token['high_24h']
                    insert_token['low_24h'] = token['low_24h']
                    insert_token['price_change_24h'] = token['price_change_24h']
                    insert_token['price_change_percentage_24h'] = token['price_change_percentage_24h']
                    insert_token['market_cap_change_24h'] = token['market_cap_change_24h']
                    insert_token['market_cap_change_percentage_24h'] = token['market_cap_change_percentage_24h']
                    insert_token['circulating_supply'] = token['circulating_supply']
                    insert_token['total_supply'] = token['total_supply']
                    insert_token['max_supply'] = token['max_supply']
                    insert_token['ath'] = token['ath']
                    insert_token['ath_change_percentage'] = token['ath_change_percentage']
                    insert_token['ath_date'] = token['ath_date']
                    insert_token['atl'] = token['atl']
                    insert_token['atl_change_percentage'] = token['atl_change_percentage']
                    insert_token['atl_date'] = token['atl_date']
                    insert_token['last_updated'] = token['last_updated']
                    
                    insert_token['createdAt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    coin_lst.append(insert_token)
            else:
                r.raise_for_status()

        if catagory == 'Governance':
            for i in range(3):
                r = requests.get(
                    f'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=governance&order=market_cap_desc&per_page=100&page={i}&sparkline=false&price_change_percentage=1h')   

                if r.status_code == 200:
                    print('Executing for token:', catagory)
                    response = r.json()
                    for token in response:
                        insert_token = {}
                        insert_token['category'] ='Governance'
                        insert_token['coin_id'] = token['id']
                        insert_token['symbol'] = token['symbol']
                        insert_token['name'] = token['name']
                        insert_token['icon'] = token['image']
                        insert_token['current_price'] = token['current_price']
                        insert_token['market_cap'] = token['market_cap']
                        insert_token['market_cap_rank'] = token['market_cap_rank']
                        insert_token['fully_diluted_valuation'] = token['fully_diluted_valuation']
                        insert_token['total_volume'] = token['total_volume']
                        insert_token['high_24h'] = token['high_24h']
                        insert_token['low_24h'] = token['low_24h']
                        insert_token['price_change_24h'] = token['price_change_24h']
                        insert_token['price_change_percentage_24h'] = token['price_change_percentage_24h']
                        insert_token['market_cap_change_24h'] = token['market_cap_change_24h']
                        insert_token['market_cap_change_percentage_24h'] = token['market_cap_change_percentage_24h']
                        insert_token['circulating_supply'] = token['circulating_supply']
                        insert_token['total_supply'] = token['total_supply']
                        insert_token['max_supply'] = token['max_supply']
                        insert_token['ath'] = token['ath']
                        insert_token['ath_change_percentage'] = token['ath_change_percentage']
                        insert_token['ath_date'] = token['ath_date']
                        insert_token['atl'] = token['atl']
                        insert_token['atl_change_percentage'] = token['atl_change_percentage']
                        insert_token['atl_date'] = token['atl_date']
                        insert_token['last_updated'] = token['last_updated']
                        
                        insert_token['createdAt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        coin_lst.append(insert_token)
                else:
                    r.raise_for_status()

    file_name = datetime.now().strftime('%Y%m%d') + '/' + 'coins_' + \
        datetime.now().strftime('%Y%m%d%H%M%S') + '.json'
    uploadBytesStream = bytes(json.dumps(coin_lst).encode('UTF-8'))

    s3.put_object(Bucket=bucket, Key=file_name, Body=uploadBytesStream)

    print('coins list file upload Complete !!')

    try:

        client = _connect_mongo(host='', port=27017,
                                username='', password='', db='')
        client_db = client["keyfi"]
        client_col = client_db['coins']
        print('Connection Successful')

        print('Deleting all existing records in collection')

        res = client_col.delete_many({})
        print(res.deleted_count, " documents deleted.")

        client_col.insert_many(coin_lst)
        print(f'Mongo Documents Inserted Successfully')

    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to server: %s" % e)

    except pymongo.errors.WriteError as e:
        print("Mongodb insertion error: %s" % e)

    except Exception as e:
        print('Error raised: %s' % e)

    finally:
        client.close()
