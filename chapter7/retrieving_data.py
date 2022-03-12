#!/usr/bin/python
# -*- coding: utf-8 -*-

# retrieving_data.py

from __future__ import print_function
import sys
import pandas as pd
import sqlalchemy


if __name__ == "__main__":
    try:
        ticker = sys.argv[1] 
    except IndexError:
        print('usage: python retrieving_data.py TICKER')
        sys.exit(1)
        
    # Connect to the MySQL instance
    engine = sqlalchemy.create_engine("mariadb+mariadbconnector://sec_user:password@localhost:3306/securities_master")

    # Select all of the historic Google adjusted close data
    sql = """SELECT dp.price_date, dp.adj_close_price
             FROM symbol AS sym
             INNER JOIN daily_price AS dp
             ON dp.symbol_id = sym.id
             WHERE sym.ticker = '%s'
             ORDER BY dp.price_date ASC;""" % ticker

    # Create a pandas dataframe from the SQL query
    goog = pd.read_sql_query(sql, con=engine, index_col='price_date')    

    # Output the dataframe tail
    print(goog.tail())
