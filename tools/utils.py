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
import seaborn as sns
import multiprocessing as mp
from multiprocessing import Pool
from matplotlib import font_manager, rc
from pytz import timezone
from ipywidgets import IntProgress
from IPython.display import display


def str2datetime(date_str, isKR = False):
    """
    convert date string to datetime format
    
    input
    ----
        date_str: date string that has the format '%Y%m%s' (ex. '20170607')
        isKR: the flag whether it apply KR timezone
    
    output
    ----
        date_datetime: datetime object of date_str
    
    """    
    date_datetime = datetime.datetime.strptime(date_str,'%Y%m%d')
    
    if isKR:
        date_datetime = date_datetime.replace(tzinfo=timezone('Asia/Seoul'))
    return date_datetime

def next_day(date_str):
    """
    get next day of datetime
    
    input
    ----
        date_str: date string that has the format '%Y%m%s' (ex. '20170607')
    
    output
    ----
        next_date_str: date string one day after
    
    """   
    next_date = str2datetime(date_str) + datetime.timedelta(days=1)
    next_date_str = datetime.datetime.strftime(next_date, '%Y%m%d')
    return next_date_str

def previous_day(date_str, isKR=False):
    """
    get previous day of datetime
    
    input
    ----
        date_str: date string that has the format '%Y%m%s' (ex. '20170607')
    
    output
    ----
        previous_date_str: date string one day ago
    
    """  
    previous_date = str2datetime(date_str) + datetime.timedelta(days=-1)
    previous_date_str = datetime.datetime.strftime(previous_date, '%Y%m%d')
    return previous_date_str


def convert2KRtime(df):
    """
    convert dateframe's unix timestamp into Asia/Seoul Timezone
    
    input
    ----
        df: dataframe (columns: timestamp, active_power, reactive_power, appliance)
    
    output
    ----
        df_kr: dataframe (columns: timestamp, active_power, reactive_power, appliance, KR timezone)
    """ 
    df_kr = df
    df_kr['timestamp'] = df_kr['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Seoul')
    df_kr = df_kr.set_index(pd.DatetimeIndex(df_kr['timestamp']))
    return df_kr

def align_timestamp(df):
    """
    align timestamp by discritizing ms into 15 mins
    
    input
    ---
        df: data frame with columns ['timestamp', 'active_power', 'reactive_power']
    
    output
    ---
        df: data frame with columns ['timestamp', 'active_power', 'reactive_power'], in which timestamp aligned. 
    """    
    
    df['timestamp_s'] = np.floor(df['timestamp']/1000)*1000
    df['timestamp_ms'] = (np.round((df['timestamp']%1000)*15/1000.0)/15.0)*1000
    df['timestamp'] = np.floor(df['timestamp_s'] + df['timestamp_ms'])
    df = df[['timestamp', 'active_power', 'reactive_power']]
    df = df.drop_duplicates(subset = 'timestamp')
    return df

def get_one_day(base_path, house_num, date_str, align=False):
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
    
    if align:
        target_sample = [align_timestamp(pd.read_parquet(target_path + '/' + f_name)) for f_name in target_file_names]
    else:
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



def get_df_abs(df):
    """
    convert active_power and reactive_power into absolute values.
    
    input
    ----
       df: data frame with columns ['timestamp', 'active_power', 'reactive_power', 'appliance']
       
    output
    ----
       df_abs: same with df except active_power and reactive_power is absolute values
    
    """ 
    
    df_abs = df
    df_abs['active_power'] = df['active_power'].abs()
    df_abs['reactive_power'] = df['reactive_power'].abs()
    return df

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

def get_target_app(df, app_name):
    """
    get the specific appliance data
    
    input
    ----
       df: dataframe with columns ['timestamp', 'active_power', 'reactive_power', 'appliance']
       app_name: target appliance name
    output
    ----
       df_target: targeted appliance data with columns ['timestamp', 'active_power', 'reactive_power', 'appliance']
    
    """ 
    
    return df[df['appliance_name'].isin([app_name])]

def get_kr_oneday(base_path, house_num, date_str, app_target = True,
                  app_name = 'total', align=False):
    """
    get one day NILM data with timezone Asia/Seoul
    
    input
    ----
       base_path: the path that contains data
       house_num: house number (ex. '00', '12')
       date_str: date string (ex. '20170506')
       app_target: flag about the specific appliance data or all
       app_name: target appliance name
       
    output
    ----
       df_kr_oneday:  targeted data with columns ['timestamp', 'active_power', 'reactive_power', 'appliance']
    
    """ 
    target_day_data = get_one_day(base_path, house_num, date_str, align=align)
    previous_day_data = get_one_day(base_path, house_num, previous_day(date_str), align=align)
    
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
    df = get_kr_oneday(base_path, house_num, date, app_target = False, align=True)
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

def filter_on_data(df, on_threshold = 15):
    """
    get appliance "on" state data, which has active_power over on_threshold
    input
    ----
       df: dataframe with columns ['timestamp', 'active_power', 'reactive_power'] 
       on_threshold: active power threshold that determine the 'on' state of appliances 
       
    output
    ----
       df_on: pandas dataframe with pretty application name index
    
    """
    df_on = df[df['active_power'] > on_threshold]
    
    return df_on
   
    

def convert2pretty_df(df):
    """
    convert raw application name indexes into pretty application name indexes 
    input
    ----
       df: pandas dataframe with raw application name index
       
    output
    ----
       df_pretty: pandas dataframe with pretty application name index
    
    """
    df_pretty = df
    df_pretty.rename(index=lambda x: get_pretty_name(x), inplace=True)
    return df_pretty


def get_pretty_name(app_name):
    """
    get pretty name from raw app name
    input
    ----
       app_name: application name string (e.g. 'fridge')
       
    output
    ----
       pretty_name:  pretty app name string (e.g. 'Refridgerator')
    
    """ 
    if app_name == 'fridge':
        pretty_name = "Refrigerator"
    elif app_name == 'kimchi-fridge':
        pretty_name = "Kimchi refrigerator"
    elif app_name == 'rice-cooker':
        pretty_name = "Rice cooker"
    elif app_name == 'washing-machine':
        pretty_name = "Washing machine"
    elif app_name == 'water-purifier':
        pretty_name = "Water purifier"
    elif app_name == 'TV' or app_name == 'tv' or app_name == 'Tv':
        pretty_name = "TV"
    elif app_name == 'air-conditioner':
        pretty_name = "Air conditioner"
    elif app_name == 'microwave':
        pretty_name = "Microwave"
    elif app_name == 'remainder':
        pretty_name = "Remainder"
    elif app_name == 'standby-power':
        pretty_name = "Standby power"
    elif app_name == 'total':
        pretty_name = 'Total'
    else:
        pretty_name = app_name
        print(app_name, ' does not change.')
        
        
    return pretty_name

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
    elif app_name == 'total':
        color = 'black'
    else:
        raise ('I dont know about ' + app_name)
    return color

def downsampling_with_first_sample(df, unit):
    """
    downsampling with given sampling rate(unit)
    input
    ----
       df: dataframe with columns ['timestamp', 'active_power', 'reactive_power'] 
       
    output
    ----
       df_pretty: pandas dataframe with pretty application name index
    
    """
    ## unit : second;S, 10minutes;10T
    df['timestamp_agg'] = df['timestamp'].dt.floor(unit)
    df_downsampled = df.groupby(['timestamp_agg'], as_index=False)['timestamp', 'active_power', 'reactive_power'].first().reset_index()
    
    return(df_downsampled)


def aggregate_by_hour(target_data):
    target_data['hour'] = target_data['timestamp'].dt.hour
    target_data_agg = target_data.groupby(['hour', 'appliance_name'], as_index=False)['active_power', 'reactive_power'].agg(['mean'])
    target_data_agg = target_data_agg.reset_index()
    return(target_data_agg)

def select_app_data(df, app_name):
    return df[df['appliance_name']==app_name]

def get_all_day_by_house(base_path, num_processes = mp.cpu_count()):
    target_file_dates = [f for f in os.listdir(base_path)]
    target_file_path = [base_path + '/' + f_name for f_name in target_file_dates]
    pool = Pool(processes=num_processes) # or whatever your hardware can support
    df_list = pool.map(preprocessing_one_day, target_file_path)
    # reduce the list of dataframes to a single dataframe
    result = pd.concat(df_list, ignore_index=True)
    return(result)


def preprocessing_one_day(target_path):
    target_file_names = [f for f in os.listdir(target_path)]
    target_sample = [pd.read_parquet(target_path + '/' + f_name) for f_name in target_file_names]
    
    for sample_index in range(0, len(target_sample)):
        target_sample[sample_index].columns = ['active_power', 'reactive_power', 'timestamp']
        target_sample[sample_index]['timestamp'] = pd.to_datetime(target_sample[sample_index]['timestamp'], unit = 'ms')
        target_sample[sample_index]['appliance_name'] = target_file_names[sample_index].split('_', 1)[1].split('.',1)[0]
    
    target_sample = pd.concat(target_sample)
    target_sample = target_sample.set_index(pd.DatetimeIndex(target_sample['timestamp']))
    print(set(target_sample['appliance_name']))
    
    return(target_sample)


def read_with_mp(path_lst, num_processes = mp.cpu_count()):
    """
    read csv files from path_lst with multipl
    
    input
    ----
    path_lst: file path lst to read
    num_processes: the number of multiprocessing units
    output
    ----
    df: pd.DataFrame that contains all data
    
    """
    pool = Pool(processes=num_processes) # or whatever your hardware can support
    
    # have your pool map the file names to dataframes
    df_list = pool.map(read_parquet, path_lst)


    # reduce the list of dataframes to a single dataframe
    df = pd.concat(df_list, ignore_index=True)
    
    pool.close()
    pool.join()
     
    return df

def filter_on_data(df, on_threshold = 15):
    """
    get appliance "on" state data, which has active_power over on_threshold
    input
    ----
       df: dataframe with columns ['timestamp', 'active_power', 'reactive_power'] 
       on_threshold: active power threshold that determine the 'on' state of appliances 
       
    output
    ----
       df_on: pandas dataframe with pretty application name index
    
    """
    df_on = df[df['active_power'] > on_threshold]
    
    return df_on
   
def get_dict(gap_count):
    parsed = {}
    trimmed = gap_count.replace('{','').replace('}','').replace(' ','')
    splited = trimmed.split(',')
    for matching in splited:
        sec = matching.split(':')[0]
        count = matching.split(':')[1]
        if int(sec) < 60:
            parsed[sec+'s'] = int(count)
        elif int(sec) < 3600:
            minutes = str(int(int(sec)/60))
            parsed[minutes+'m'] = int(count)
        else:
            hours = str(int(int(sec)/3600))
            parsed[hours+'h'] = int(count)
    return parsed

