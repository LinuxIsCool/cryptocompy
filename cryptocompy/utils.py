import requests
import pandas as pd
import datetime
from cryptocompy import top, coin

def get_all_coins():
    all_coins = coin.get_coin_list()
    return all_coins


def get_all_symbols():
    coin_list = coin.get_coin_list()
    all_coin_names = [c['Symbol'] for c in coin_list.values()]
    return all_coin_names

def get_all_symbols():
    coin_list = coin.get_coin_list()
    all_coin_names = [c['Symbol'] for c in coin_list.values()]
    return all_coin_names

def get_all_coin_names():
    coin_list = coin.get_coin_list()
    all_coin_names = [c['CoinName'] for c in coin_list.values()]
    return all_coin_names

def write_top_pricematrix(tsym='BTC', limit=20, exchange=''):
    pm = get_top_pricematrix()
    df = pd.DataFrame(pm)
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    df.to_csv(filename + ".csv")

def get_top_pricematrix(tsym='BTC', limit=20, exchange=''):
    symbols = get_top_symbols(tsym, limit)
    pm = get_pricematrix(symbols, exchange)
    return pm

def get_top_coin_names(tsym='BTC', limit=20):
    if limit:
        coins = pd.DataFrame(top.get_top_coins(tsym='BTC', limit=limit))
    else:
        coins = pd.DataFrame(top.get_top_coins(tsym='BTC'))
    symbols = [coin.split()[0] for coin in list(coins['FULLNAME'].values[1:])]
    return symbols

def get_top_symbols(tsym='BTC', limit=20):
    if limit:
        coins = pd.DataFrame(top.get_top_coins(tsym='BTC', limit=limit))
    else:
        coins = pd.DataFrame(top.get_top_coins(tsym='BTC'))
    symbols = list(coins['SYMBOL'].values[1:])
    return symbols

def get_pricematrix(symbols, exchange=''):
    """Get a matrix of trade pair rates where row and column indices of the
    matrix are symbols."""
    symbols_string = ','.join(symbols).upper()
    url = 'https://min-api.cryptocompare.com/data/pricemulti?fsyms={0}&tsyms={0}'\
            .format(symbols_string)
    if exchange:
        url += '&e='.formate(exchange)
    page = requests.get(url)
    data = page.json()
    return data

