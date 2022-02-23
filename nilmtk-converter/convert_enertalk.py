import pandas as pd
import pyarrow
from os.path import join
from os import listdir
import fnmatch
import re
from sys import stdout
from nilmtk.utils import get_datastore
from nilmtk.datastore import Key
from nilmtk.measurement import LEVEL_NAMES
from nilm_metadata import save_yaml_to_datastore
from tqdm import tqdm

def convert_enertalk(input_path, output_filename, format='HDF', tz='Asia/Seoul'):
    """
    Parameters
    ----------
    input_path : str
        The root path of ENEERTAK dataset.
    output_filename : str
        The destination filename (including path and suffix).
    format : str
        format of output. Either 'HDF' or 'CSV'. Defaults to 'HDF'
    tz : str
        Timezone e.g. 'Asia/Seoul'
    """
    
    # Open DataStore
    store = get_datastore(output_filename, format, mode='w')
    
    # convert raw data to DataStore
    _convert(input_path, store, tz=tz)
      
    # Add metadata
    save_yaml_to_datastore('metadata/',
                     store)
    store.close()
    

def _convert(input_path, store, tz):
    """
    Parameters
    ----------
    input_path : str
        The root path of the ENERTALK dataset.
    store : DataStore
        The NILMTK DataStore object.
    tz : str 
        Timezone e.g. 'Asia/Seoul'
    """
    house_list = [fname for fname in listdir(input_path) if not fname.startswith('.')]
    
    date_count = 0

    for house in house_list:
        date_list = sorted(listdir(join(input_path, house)))
        date_count += len(date_list)
        
    with tqdm(total = date_count) as pbar:
        for house in house_list:
            date_list = sorted(listdir(join(input_path, house)))
            for date in date_list:
                fname_list = sorted(listdir(join(input_path, house, date)))

                for fname in fname_list:
                    file_path = join(input_path, house, date, fname)
                    chan_df = _load_parquet(file_path)
                    house_id = int(house) + 1
                    chan_id = int(fname.split('_')[0]) + 1
                    key = Key(building=house_id, meter=chan_id)
                    chan_df.columns = pd.MultiIndex.from_tuples([('power', 'active'), ('power', 'reactive')])
                    chan_df.columns.set_names(LEVEL_NAMES, inplace=True)


                    if str(key) in store._keys():
                        store.append(str(key), chan_df)
                    else:
                        store.put(str(key), chan_df)
                pbar.update(1)
    
def _load_parquet(file_path, tz='Asia/Seoul'):
    """
    Parameters
    ----------
    file_path : str
          The file path to read
    
    tz : str e.g. 'Asia/Seoul'
    Returns
    -------
    dataframe
    """
    # load data
    df = pd.read_parquet(file_path)

    # Convert timestamp into timezon-aware datetime
    df['Unix'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df = df.drop('timestamp', axis='columns')
    df.set_index('Unix', inplace=True)
    df = df.tz_convert(tz)
    return df



if __name__=='__main__':
    input_path = '../enertalk-dataset'
    output_path = 'enertalk_converted/'
    output_filename = join(output_path, 'enertalk.h5')
    convert_enertalk(input_path, output_filename)