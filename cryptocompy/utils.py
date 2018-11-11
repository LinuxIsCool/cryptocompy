import requests
import numpy as np
import pandas as pd
import xarray as xr
import datetime
from cryptocompy import top, coin

# This should be ran async
def get_series(symbols, freq, tsym, limit, aggregate, exchange=''):
    series = {}
    assert(freq in ['minute', 'hourly', 'daily'])
    if freq == 'minute':
        for fsym in symbols:
           series[fsym] = minute_price_historical(fsym, tsym, limit=limit, aggregate=aggregate, exchange=exchange)
    if freq == 'hourly':
        for fsym in symbols:
           series[fsym] = hourly_price_historical(fsym, tsym, limit=limit, aggregate=aggregate, exchange=exchange)
    if freq == 'daily':
        for fsym in symbols:
           series[fsym] = daily_price_historical(fsym, tsym, limit=limit, aggregate=aggregate, exchange=exchange)

    # Drop coins which do not return successfully
    series = {k:v for k, v in series.items() if type(v) == pd.core.frame.DataFrame}
    return series

def get_coin_features(sym, series):
    df = series[sym].set_index('timestamp').drop(['time'], axis=1)
    return df

def pad_time(datalist, filler=0):
    longest = max([d.shape for d in datalist], key=lambda x: x[1])
    padded_data = []
    for data in datalist:
        fill = np.full((data.shape[0], longest[1] - data.shape[1]), filler)
        padded = np.concatenate((fill, data), axis=1)
        padded_data.append(padded)
    return padded_data

def stack(datalist):
    tensor = np.stack(datalist, axis=2)
    tensor = np.swapaxes(tensor, 0, 2)
    return tensor

def get_dataset(series, name):
    datalist = pad_time([get_coin_features(sym, series).T for sym in series.keys()])
    tensor = stack(datalist)
    dims = ['coins', 'time', 'features']
    coords={'coins': list(series.keys()), 'time': series['LTC']['timestamp'], 'features': ['close','high','low','open','volumeto', 'volumefrom']}
    da = xr.DataArray(tensor, dims=dims, coords=coords, name=name)
    return da

def daily_price_historical(symbol, comparison_symbol, limit=1, aggregate=1, exchange='', allData='true'):
    url = 'https://min-api.cryptocompare.com/data/histoday?fsym={}&tsym={}&limit={}&aggregate={}&allData={}'\
            .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate, allData)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return df

def hourly_price_historical(symbol, comparison_symbol, limit, aggregate, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histohour?fsym={}&tsym={}&limit={}&aggregate={}'\
            .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    return df

def minute_price_historical(symbol, comparison_symbol, limit, aggregate, exchange=''):
    url = 'https://min-api.cryptocompare.com/data/histominute?fsym={}&tsym={}&limit={}&aggregate={}'\
            .format(symbol.upper(), comparison_symbol.upper(), limit, aggregate)
    if exchange:
        url += '&e={}'.format(exchange)
    page = requests.get(url)
    data = page.json()['Data']
    df = pd.DataFrame(data)
    try:
        df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
    except AttributeError:
        print("Skipping {0} because it has no time attribute.".format(symbol))
        return None
    return df

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

