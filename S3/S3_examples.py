#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import json
import pandas as pd
import s3fs
import pickle
import zlib

KEY_FILE = '../keys/s3.sec'

def get_dates_s3():
    with open(KEY_FILE, 'r') as f:
        parameters = json.load(f)
    
    key = parameters['AWS_ACCESS_KEY']
    secret = parameters['AWS_SECRET_ACCESS_KEY']
    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)

    dates_list_s3 = s3.ls(parameters["BASKET"])
    return [d[-10:] for d in dates_list_s3]

def get_date_rates_s3(d):
    with open(KEY_FILE, 'r') as f:
        parameters = json.load(f)
    
    key = parameters['AWS_ACCESS_KEY']
    secret = parameters['AWS_SECRET_ACCESS_KEY']
    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)
    
    date_string = d.strftime("%Y-%m-%d")

    try:
        with s3.open(parameters["BASKET"] + '/' + date_string + "/curr.csv", 'r') as f:
            return pd.read_csv(f, index_col=0, parse_dates=True)
    except:
        return None

def filename_from_curr(df, t, c):
    return df.loc[t, "file"] + " opt_" + c.lower()

def get_snaphot_s3(f_name):
    with open(KEY_FILE, 'r') as f:
        parameters = json.load(f)
    
    key = parameters['AWS_ACCESS_KEY']
    secret = parameters['AWS_SECRET_ACCESS_KEY']
    s3 = s3fs.S3FileSystem(anon=False, key=key, secret=secret)
    
    try:
        with s3.open(parameters["BASKET"] + '/' + f_name[:10] + '/' + f_name, "rb") as f:
            content_str = f.read()
            return pickle.loads(zlib.decompress(content_str[1:-1]))
    except:
        return None

def examples():
    dates_s3 = get_dates_s3()
    
    date_example = datetime.strptime(dates_s3[0], "%Y-%m-%d")
    df_rates = get_date_rates_s3(date_example)
    
    date_time_example = df_rates.index[10]

    f_name = filename_from_curr(df_rates, date_time_example, "BTC")
    snapshot_BTC = get_snaphot_s3(f_name)
    
    f_name = filename_from_curr(df_rates, date_time_example, "ETH")
    snapshot_ETH = get_snaphot_s3(f_name)

