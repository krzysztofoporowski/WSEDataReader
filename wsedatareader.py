# -*- coding: utf-8 -*-
'''
Created on  Jul 19 2020

@author: Krzysztof Oporowski
krzysztof.oporowski@gmail.com
'''

import os
import pickle
import requests
import datetime as dt
from pathlib import Path
#import requests_cache
import pandas_datareader.data as pdr
from pandas_datareader.yahoo.headers import DEFAULT_HEADERS
from datetime import datetime
from bs4 import BeautifulSoup
import time
import quandl
import pandas as pd
# zmiana 6.01.2023
import yfinance as yf

#yf.pdr_override()
# koniec zmiana 6.01.2023


def finviz_screener_tickers(filters, max_pages=5, pause=1.0):
    """
    filters: list of finviz filter codes, np. ['cap_largeover','fa_pe_u25']
    max_pages: ile stron pobrać (Finviz ~20 wyników na stronę)
    returns: list of tickers
    """
    base = "https://finviz.com/screener.ashx?v=111&f=" + ",".join(filters)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    tickers = []

    for page in range(1, max_pages + 1):
        if page == 1:
            url = base
        else:
            # Finviz paginacja: r param określa indeks pierwszego rekordu (1,21,41,...)
            start = (page - 1) * 20 + 1
            url = base + f"&r={start}"
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"HTTP {resp.status_code} dla {url}")
            break
            print(resp.status_code)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Finviz: tickery są w linkach z klasą 'screener-link-primary'
        anchors = soup.find_all("a", class_="screener-link-primary")
        if not anchors:
            break
        page_tickers = [a.text.strip() for a in anchors]
        # unikaj duplikatów
        for t in page_tickers:
            if t not in tickers:
                tickers.append(t)

        time.sleep(pause)  # nie obciążaj serwera
    return tickers

def create_directory(directory_name, verbose=False):
    '''
    Function to create directory for storing data

    Parameters
    ----------
    directory_name : string
        Directory name.

    Returns
    -------
    None.

    '''
    path_to_check = Path(directory_name)
    if path_to_check.exists():
        if verbose:
            print('Directory {} already exists'.format(directory_name))
        return True
    else:
        if verbose:
            print('Attempting to create path')
        try:
            path_to_check.mkdir()
            return True
        except FileExistsError:
            if verbose:
                print('Strange, directory {} cannot be created'.format(
                    directory_name))
            return False

def get_data_from_quandle(equity_name, quandl_api_token):
    '''
    Function read data of the signle quity using the Quandl. It requires the
    Quandl API to be provided, to make sure that more than 50 queries are
    allowed. Function returns the Pandas Panel data structure.
    Parameters:
    -----------
    equity_names:     String, used for polish stocks. On the Quandl
                      platform polish stocks are listed under the 'WSE/'
                      subfolder (Warsaw Stock Exchnage). Equity_names needs to
                      be the list of strings without the 'WSE/' (which is added
                      by the function).
    quandl_API_token: string, representing the Quandl API token. For more
                      details refer to the http://quandl.com
    Returns:
    --------
    Pandas DataFrame with one entitie's data
    '''
    todays_date = str(dt.date.today())
    file_name = 'Data/' + equity_name + '-' + todays_date + '.pickle'
    try:
        with open(file_name, 'rb') as opened_file:
            data = pickle.load(opened_file)
        # print('data from file {} used'.format(opened_file))
    except FileNotFoundError:
        quandl.ApiConfig.api_key = quandl_api_token
        # for equity_name in equity_names:
        quandl_query = 'WSE/' + equity_name
        data = quandl.get(quandl_query)
        data.drop(['%Change', '# of Trades', 'Turnover (1000)'],
                  axis=1, inplace=True)
        data.columns = ['open', 'high', 'low', 'close', 'volume']
        data.index.names = ['date']
        # data = data[equity_name].resample('1d').mean()
        data.fillna(method='ffill', inplace=True)
        # print('Data for {} collected'.format(quandl_query))
        # save data to avoid downloading again today
        if create_directory('Data'):
            with open(file_name, 'wb') as opened_file:
                pickle.dump(data, opened_file)
            # print('Data from Quandl downloaded')
    return data

def get_bossa_date(mode='string'):
    '''
    Checks what is the date of the file with stocks data listed in the text
    format on bossa.pl (file mstall.zip)
    Parameters:
    ----------
        mode - determines on how to return the value:
               'string' - as a string used in files obtained from bossa.pl
               'datetime' - as a pandas datetime value
    Returns:
    --------
        bossa_date - date in the pandas datetime or string format
    '''
    path = Path()
    home_path = path.home()
    wig20_path = str(home_path) + os.path.sep + 'python' + os.path.sep + 'data' 
    wig20_path = wig20_path + os.path.sep + 'WIG20.mst'
    try:
        wig20 = pd.read_csv(wig20_path, parse_dates=[1], usecols=[0, 1],
                    names=['ticker', 'date'])
    except:
        text = 'No WIG20.mst file found on path: {}. Download the WIG20 file '
        text = text + 'from the bossa.pl'
        print(text.format(wig20_path))
    last = wig20.tail(1)
    last_date = pd.to_datetime(last.date)
    year = int(last_date.dt.year)
    print(year)
    month = int(last_date.dt.month)
    day = int(last_date.dt.day)
    if mode == 'datetime':
        bossa_date = dt.date(year, month, day)
    elif mode == 'string':
        if month < 10:
            str_month = '0' + str(month)
        else:
            str_monht = str(month)
        if day < 10:
            str_day = '0' + str(day)
        else:
            str_day = str(day)
        bossa_date = str(year) + '-' + str_month + '-' + str_day
    else:
        print('Do not understand mode value, use "string" or "datetime" only')
    return bossa_date

def get_data_from_bossa(stooq_name, path_to_data):
    '''
    Parameters
    ----------
    stooq_name : TYPE
        DESCRIPTION.

    Returns
    -------
    data : TYPE
        DESCRIPTION.

    '''
    file_name = path_to_data + os.path.sep + stooq_name + '.mst'
    data = pd.read_csv(file_name,
                       usecols=[1, 2, 3, 4, 5, 6],
                       parse_dates=[0],
                       index_col=[0],
                       header=0,
                       names=['date',
                              'open',
                              'high',
                              'low',
                              'close',
                              'volume']
                       )
    return data

def get_date_only(row):
    '''
    To process index date/time value from Quandl to get only date, as a string
    '''
    date_time = row.name
    date_time = pd.to_datetime(date_time)
    date_only = date_time.date()
    return date_only

def get_data_yahoo(symbol, start, end):
    '''
    Returns data from Yahoo Finance. Data is cached for 8 hours.
    Columns: ['High', 'Low', 'Open', 'Close', 'Volume', 'AdjClose']
    '''
    ''' zmiana 7.06.2023
    expire_afer = dt.timedelta(hours=8)
    session = requests_cache.CachedSession(cache_name='cache',
                                            backend='sqlite',
                                            expire_afer=expire_afer)
    session.headers = DEFAULT_HEADERS
    '''
    try:
        ''' zmiana 6.01.2023
        data = pdr.DataReader(symbol,
                            'yahoo',
                            start=start,
                            end=end,
                            session=session)
        '''
        #data = pdr.get_data_yahoo(symbol, start=start, end=end, progress=False)
        data = yf.download(symbol, start=start, end=end, progress=False)
    except Exception as e:
        print('Whike reading {} data for period: {} - {} got exception: {}'.format(symbol,
                                                                                start,
                                                                                end,
                                                                                str(e)))

    return data

def get_yahoo_date():
    '''
    Function gets last quote date avaiable on Yahoo Finance by checking the
    ^SPX index values
    '''
    END = dt.date.today()
    START = END - dt.timedelta(days=14)
    SYMBOL = '^SPX'
    DATA=get_data_yahoo(SYMBOL, START, END)
    DATE = DATA.index[-1].date()

    return DATE

if __name__ == '__main__':
    print('This is a module, do not run it, import it!')
