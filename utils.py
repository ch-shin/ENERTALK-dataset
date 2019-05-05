from __future__ import unicode_literals
import matplotlib as mpl          
import matplotlib.pyplot as plt  
import matplotlib.dates as mdates
import pprint

import pandas as pd
import os

import datetime

import pandas as pd
import numpy as np
import datetime
import matplotlib as mpl
import matplotlib.pylab as plt
import matplotlib.dates as md
import pickle
import functools
from multiprocessing import Pool
from matplotlib import font_manager, rc
from pytz import timezone
from ipywidgets import IntProgress
from IPython.display import display



def get_one_day(base_path, house_num, date_str):
    """
    read one day data (total, appliances)
    
    input
    ----
        base_path: the path that has contains csv files
        house_number: house number (ex. '00', '12')
        date_str: date string (ex. '20170505')
    
    output
    ----
        target_sample: data frame that contains one day data (columns: ['timestamp', 'active_power', 'reactive_power', appliance'] 
    
    """ 
    target_path = base_path + house_num + '/' + date_str + '/'
    target_file_names = [f for f in os.listdir(target_path)]
    target_sample = [pd.read_parquet(target_path + '/' + f_name) for f_name in target_file_names]
    
    for sample_index in range(0, len(target_sample)):
        #target_sample[sample_index].columns = ['active_power', 'reactive_power', 'timestamp']
        target_sample[sample_index].columns = ['timestamp', 'active_power', 'reactive_power']
        target_sample[sample_index]['timestamp'] = pd.to_datetime(target_sample[sample_index]['timestamp'], unit = 'ms')
        target_sample[sample_index]['appliance_name'] = target_file_names[sample_index].split('_', 1)[1].split('.',1)[0]
    
    target_sample = pd.concat(target_sample)
    target_sample = target_sample.set_index(pd.DatetimeIndex(target_sample['timestamp']))
    print(set(target_sample['appliance_name']))
    
    return(target_sample)


def get_specific_duration(df, start_date, end_date):
    """
    filter data with timestamp constraints
    
    input
    ----
       df: data frame with columns ['timestamp', 'active_power', 'reactive_power', 'appliance']
       start_date: date (datetime format)
       end_date: date (datetime format)
       
    output
    ----
        app_data: data in given periods
    
    """ 
    app_data = df
    app_data = app_data.set_index(pd.DatetimeIndex(app_data['timestamp']))
    app_data = app_data.loc[(app_data['timestamp'] >= start_date) & (app_data['timestamp'] < end_date)]
  
    return app_data

def get_kr_oneday(base_path, house_num, date_str, app_target = True, app_name = 'total'):
    """
    get one day NILM data with timezone Asia/Seoul
    
    input
    ----
       base_path: the path that contains data
       house_num: house number (ex. 'A300466E')
       date_str: date string (ex. '20170506')
       app_target: flag about the specific appliance data or all
       app_name: target appliance name
       
    output
    ----
       df_kr_oneday:  targeted data with columns ['timestamp', 'active_power', 'reactive_power', 'appliance']
    
    """ 
    target_day_data = get_one_day(base_path, house_num, date_str)
    previous_day_data = get_one_day(base_path, house_num, previous_day(date_str))
    
    df = previous_day_data 
    df = df.append(target_day_data)
    df = get_df_abs(df)
    df_kr = convert2KRtime(df)
    
    start_date = str2datetime(date_str, isKR=True)
    end_date = str2datetime(next_day(date_str), isKR=True)
    
    if app_target:
        df_kr = get_target_app(df_kr, app_name)
        
    df_kr_oneday = get_specific_duration(df_kr, start_date, end_date)
    
    return df_kr_oneday


def get_kr_oneday_aligned(base_path, house_num, date):
    """
    get one day NILM data with timezone Asia/Seoul
    
    input
    ----
       base_path: the path that contains data
       house_num: house number (ex. '00', '12')
       date_str: date string (ex. '20170506')
       app_target: flag about the specific appliance data or all

    output
    ----
       df_kr_oneday: df with multiple appliances and aligned timestamp
    
    """ 
    df = get_kr_oneday(base_path, house_num, date, app_target = False)
    df_lst = []

    for app_name in set(df['appliance_name']):
        df_app = get_target_app(df, app_name)
        df_app = df_app[['active_power']]
        df_app.columns = [app_name]
        df_app = df_app.reset_index()
        df_lst.append(df_app)


    df_kr_oneday = functools.reduce(lambda left,right: pd.merge(left,right,on='timestamp'), df_lst)
    
    return df_kr_oneday

def get_app_name_from_fname(fname):
    """
    extract appliance name from file name
    
    input
    ----
    fname: file name, example: 02_rice-cooker.parquet.gzip
    
    output
    ----
    app_name: appliance name string
    """
    
    app_name = fname.split('_')[1].split('.')[0]
    
    return app_name

def draw_cum_plot(df_agg):
    """
    draw cumulative plot
    
    input
    ----
    df_agg: pandas dataframe that contains aligned data

    """
    hours = mdates.HourLocator(interval = 2, tz = timezone('Asia/Seoul'))
    h_fmt = mdates.DateFormatter('%H:%M', tz = timezone('Asia/Seoul'))
    app_name_lst = list(df_agg.columns)
    app_name_lst.remove('total')
    app_name_lst.remove('timestamp')
    df_agg['Standby power'] = np.min(df_agg['total'])

    timestamp = [ts.to_pydatetime().replace(tzinfo=timezone('Asia/Seoul')) for ts in df_agg['timestamp']]
    display(df_agg.head())
    cum = df_agg['Standby power']
    plt.figure(figsize = (20, 10))
    plt.fill_between(timestamp, np.zeros(df_agg.shape[0]), cum, label = 'Standby power')
    for app_name in app_name_lst:
        lower = cum
        cum = cum + df_agg[app_name]
        plt.fill_between(timestamp, lower, cum, label = get_pretty_name(app_name))
    plt.fill_between(timestamp, cum, df_agg['total'], label = 'Unknown')
    plt.legend()
    plt.grid()
    ax = plt.gca()
    ax.xaxis.set_major_locator(hours)
    ax.xaxis.set_major_formatter(h_fmt)
    plt.margins(x=0, tight = True)


def get_app_color(app_name):
    palette = sns.color_palette("tab20")
    if app_name == 'Standby Power':
        color = "dimgrey"
    elif app_name == 'TV':
        color = palette[10]
    elif app_name == 'fridge':
        color = palette[0]
    elif app_name == 'kimchi-fridge':
        color = palette[6]
    elif app_name == 'water-purifier':
        color = palette[19]
    elif app_name == 'washing-machine':
        color = palette[18]
    elif app_name == 'rice-cooker':
        color = palette[2]
    elif app_name == 'microwave':
        color = palette[7]
    elif app_name == 'unknown':
        color = 'silver'
    else:
        raise ('I dont know about ' + app_name)
    return color