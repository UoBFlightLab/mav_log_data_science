from pymavlink import mavutil
import pandas as pd
from datetime import datetime, timedelta
import os

def get_all_log_paths(search_root = '.', file_ext = '.bin'):
    path_list = []
    for root, _, files in os.walk(search_root, topdown=False):
        for filename in files:
            if(filename.endswith(file_ext)):
                log_path = os.path.join(root, filename)
                path_list.append(log_path)
    return path_list

def get_time_from_filename(file_path):
    time_str = file_path[-23:-13]
    time_str+='T'
    time_str+=file_path[-12:-10]
    time_str+=':'
    time_str+=file_path[-9:-7]
    time_str+=':'
    time_str+=file_path[-6:-4]
    try:
        time_value = datetime.fromisoformat(time_str)
    except ValueError:
        time_value = None
    return time_value

def import_log(file_path, include_types = None, exclude_types = ['FMT','ISBD'], memory=False):
    reported_types = []
    drone_id = None
    log_df = pd.DataFrame({})
    df_index = 0
    full_path = os.path.abspath(file_path)
    mlog = mavutil.mavlink_connection(file_path)
    line_dict = {}
    while True:
        if memory:
            pass
        else:
            line_dict = {}
        try:
            if include_types:
                this_msg = mlog.recv_match(type=include_types)
            else:
                this_msg = mlog.recv_match()
            if this_msg is None:
                break
        except Exception:
            break
        #print(this_msg)
        msg_type = this_msg.get_type()
        if msg_type in exclude_types:
            if msg_type not in reported_types:
                print(f'Excluding {msg_type}')
                reported_types.append(msg_type)
            continue
        if msg_type not in reported_types:
            print(f'Including {msg_type}')
            reported_types.append(msg_type)
        if msg_type=='PARM':
            if this_msg.Name=='SYSID_THISMAV':
                drone_id = int(this_msg.Value)
        line_dict = {'Type': msg_type,
                     'DroneID': drone_id,
                     'FilePath': full_path,
                     }
        msg_fields = this_msg.get_fieldnames()
        if 'TimeUS' in msg_fields:
            line_dict['TimeUS'] = this_msg.TimeUS
        for fld in msg_fields:
            df_field_name = '_'.join([msg_type,
                                      fld])
            line_dict[df_field_name] = this_msg.__getattr__(fld)
        try:
            log_df = pd.concat([log_df,pd.DataFrame(line_dict,
                                                    index=[df_index])])
        except ValueError:
            print('Could not add record')
            print(this_msg)
            continue
        df_index += 1
        if df_index % 1000 == 0:
            print(f'Got {df_index} records from {full_path}')
    print(f'Got {df_index} records from {full_path}')
    return log_df

def get_log_start_us(df):
    return int(df.iloc[0].TimeUS)

def df_log_duration_seconds(df):
    duration_us = int(df.iloc[-1].TimeUS) - get_log_start_us(df)
    log_duration = timedelta(microseconds=duration_us).total_seconds()
    return log_duration

def add_real_time_from_filename(df):
    filename = df.iloc[0].FilePath
    filename_start_time = get_time_from_filename(filename)
    if filename_start_time:
        log_epoch = filename_start_time - timedelta(microseconds=get_log_start_us(df))
        df['TimeFilename'] = [log_epoch + timedelta(microseconds=int(t)) for t in df['TimeUS']]
    return df
    
def add_real_time_from_gps(df):
    gps_epoch = datetime.fromisoformat('1980-01-06')
    if 'GPS_GMS' in df.columns:
        first_gps_msg = df[df['Type']=='GPS'].iloc[0]
        first_gps_time = gps_epoch + timedelta(weeks=first_gps_msg.GPS_GWk, milliseconds=first_gps_msg.GPS_GMS)
        log_epoch = first_gps_time - timedelta(microseconds=int(first_gps_msg.TimeUS))
        df['TimeGPS'] = [log_epoch + timedelta(microseconds=int(t)) for t in df['TimeUS']]
    return df
    
def import_log_set(path_list, include_types = ['PARM','MODE','GPS','POS','MAVC','CMD'], exclude_types = ['FMT','ISBD'], memory=False):
    df_list = []
    for file_name in path_list:
        df = import_log(file_name,
                        include_types=include_types,
                        exclude_types=exclude_types,
                        memory=memory)
        df = add_real_time_from_filename(df)
        df = add_real_time_from_gps(df)
        df_list.append(df)
    all_df = pd.concat(df_list)
    return all_df

def get_drone_ids(df):
    drone_ids = set([id for id in df['DroneID'] if id is not None])
    return drone_ids

def get_files(df):
    files = set(df['FilePath'])
    return files

def get_flight_times(df):
    files = get_files(df)
    results = {}
    flight_stat_df = df[df['PARM_Name']=='STAT_FLTTIME']
    for f in files:
        file_stats_df = flight_stat_df[flight_stat_df['FilePath']==f]
        file_flight_time = max(file_stats_df['PARM_Value']) - min(file_stats_df['PARM_Value'])
        results[f] = file_flight_time
    return results

def get_total_flight_time(df):
    time_res = get_flight_times(df)
    return sum(time_res.values())

def by_msg_type(df,msg_type):
    return(df[df['Type']==msg_type])