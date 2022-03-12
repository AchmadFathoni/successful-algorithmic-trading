#!/usr/bin/python
# -*- coding: utf-8 -*-

# price_retrieval.py

from __future__ import print_function

import datetime
import warnings

import mariadb as mdb

import yfinance as yf

# Obtain a database connection to the MySQL instance
db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'password'
db_name = 'securities_master'


def obtain_list_of_db_tickers():
    """
    Obtains a list of the ticker symbols in the database.
    """
    with mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name) as con: 
        cur = con.cursor()
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1]) for d in data]


def get_daily_historic_data_yahoo(
        ticker, start_date="2000-1-1",
        end_date=datetime.datetime.today().strftime('%Y-%m-%d')
    ):
    """
    Obtains data from Yahoo Finance returns and a pandas data frame.

    ticker: Yahoo Finance ticker symbol, e.g. "GOOG" for Google, Inc.
    start_date: Start date in "YYYY-M-D" format
    end_date: End date in "YYYY-M-D" format
    """
    # Fetch history data from Yahoo Finance as return it as panda data frame
    yf_ticker = yf.Ticker(ticker)
    prices = yf_ticker.history(interval="1d", start=start_date, end=end_date,auto_adjust=False) 
    return prices


def insert_daily_data_into_db(
        data_vendor_id, symbol_id, daily_data
    ):
    """
    Takes a list of tuples of daily data and adds it to the
    mariadb database. Appends the vendor ID and symbol ID to the data.

    daily_data: pandas data frame that contain daily data with date as index
    """
    # Create the time now
    now = datetime.datetime.utcnow()

    # Amend the data to include the vendor ID and symbol ID
    db_daily_data = [
        (data_vendor_id, symbol_id, index.to_pydatetime(), now, now,
        row["Open"], row["High"], row["Low"], row["Close"], row["Volume"], row["Adj Close"]) 
        for index, row in daily_data.iterrows()
    ]

    # Create the insert strings
    column_str = """data_vendor_id, symbol_id, price_date, created_date, 
                 last_updated_date, open_price, high_price, low_price, 
                 close_price, volume, adj_close_price"""
    insert_str = ("%s, " * 11)[:-2]
    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % \
        (column_str, insert_str)

    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    with mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name) as con: 
        cur = con.cursor()
        cur.executemany(final_str, db_daily_data)
        con.commit()


if __name__ == "__main__":
    # This ignores the warnings regarding Data Truncation
    # from the Yahoo precision to Decimal(19,4) datatypes
    warnings.filterwarnings('ignore')

    # Loop over the tickers and insert the daily historical
    # data into the database
    tickers = obtain_list_of_db_tickers()
    lentickers = len(tickers)
    for i, t in enumerate(tickers):
        print(
            "Adding data for %s: %s out of %s" % 
            (t[1], i+1, lentickers)
        )
        yf_data = get_daily_historic_data_yahoo(t[1])
        insert_daily_data_into_db('1', t[0], yf_data)
    print("Successfully added Yahoo Finance pricing data to DB.")
