from datetime import datetime, timedelta
import os
from argparse import ArgumentParser
import pandas as pd
from pymavlink import mavutil

def get_all_log_paths(search_root = '.', file_ext = '.bin'):
    path_list = []
    for root, _, files in os.walk(search_root, topdown=False):
        for filename in files:
            if filename.lower().endswith(file_ext.lower()):
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

def get_log_start_us(this_df):
    return int(this_df.iloc[0].TimeUS)

def df_log_duration_seconds(this_df):
    duration_us = int(this_df.iloc[-1].TimeUS) - get_log_start_us(this_df)
    log_duration = timedelta(microseconds=duration_us).total_seconds()
    return log_duration

def add_real_time_from_filename(this_df):
    filename = this_df.iloc[0].FilePath
    filename_start_time = get_time_from_filename(filename)
    if filename_start_time:
        log_epoch = filename_start_time - timedelta(microseconds=get_log_start_us(this_df))
        this_df['TimeFilename'] = [log_epoch + timedelta(microseconds=int(t)) for t in this_df['TimeUS']]
    return this_df

def add_real_time_from_gps(this_df):
    gps_epoch = datetime.fromisoformat('1980-01-06')
    if 'GPS_GMS' in this_df.columns:
        first_gps_msg = this_df[this_df['Type']=='GPS'].iloc[0]
        first_gps_time = gps_epoch + timedelta(weeks=first_gps_msg.GPS_GWk,
                                               milliseconds=first_gps_msg.GPS_GMS)
        log_epoch = first_gps_time - timedelta(microseconds=int(first_gps_msg.TimeUS))
        this_df['TimeGPS'] = [log_epoch + timedelta(microseconds=int(t)) for t in this_df['TimeUS']]
    return this_df

def import_log_set(path_list,
                   include_types = ['PARM','MODE','GPS','POS','MAVC','CMD'],
                   exclude_types = ['FMT','ISBD'], memory=False):
    df_list = []
    for file_name in path_list:
        this_df = import_log(file_name,
                             include_types=include_types,
                             exclude_types=exclude_types,
                             memory=memory)
        this_df = add_real_time_from_filename(this_df)
        this_df = add_real_time_from_gps(this_df)
        df_list.append(this_df)
    all_df = pd.concat(df_list)
    return all_df

def get_drone_ids(this_df):
    drone_ids = set([id for id in this_df['DroneID'] if id is not None])
    return drone_ids

def get_files(this_df):
    files = set(this_df['FilePath'])
    return files

def get_flight_times(this_df):
    files = get_files(this_df)
    results = {}
    flight_stat_df = this_df[this_df['PARM_Name']=='STAT_FLTTIME']
    for fn in files:
        file_stats_df = flight_stat_df[flight_stat_df['FilePath']==fn]
        file_flight_time = max(file_stats_df['PARM_Value']) - min(file_stats_df['PARM_Value'])
        results[fn] = file_flight_time
    return results

def get_total_flight_time(this_df):
    time_res = get_flight_times(this_df)
    return sum(time_res.values())

def by_msg_type(this_df,msg_type):
    return this_df[this_df['Type']==msg_type]

def main(search_path='.',
         input_file=None,
         summary_file='summary.csv',
         output_file=None,
         messages=['PARM']):
    num_files = 0
    if os.path.isfile(summary_file):
        summary_df = pd.read_csv(summary_file)
    else:
        summary_df = pd.DataFrame(columns=['file','flight_time','log_time','file_size'])
    if input_file:
        print(f'Loading {messages} from "{input_file}"')
        this_df = import_log(input_file, include_types=messages)
        if 'GPS' in messages:
            this_df = add_real_time_from_gps(this_df)
        num_files = 1
        summary_df = pd.concat([summary_df,
                                pd.DataFrame([{'file': input_file,
                                                'flight_time': get_total_flight_time(this_df),
                                                'log_time': df_log_duration_seconds(this_df),
                                                'file_size': os.stat(input_file).st_size}])])
        summary_df.to_csv(summary_file)
        if output_file:
            this_df.to_csv(output_file)
    elif search_path:
        print(f'Searching "{search_path}" for log files.')
        file_list = get_all_log_paths(search_root=search_path)
        for (ii,fn) in enumerate(file_list):
            if any(summary_df['file']==fn):
                print(f'Already done {fn}')
                continue
            print(f'Loading {messages} from "{fn}"')
            this_df = import_log(fn, include_types=messages)
            if 'GPS' in messages:
                this_df = add_real_time_from_gps(this_df)
            num_files += 1
            try:
                flight_time = get_total_flight_time(this_df)
            except ValueError:
                flight_time = 0.0
            summary_df = pd.concat([summary_df,
                                    pd.DataFrame([{'file': fn,
                                                    'flight_time': flight_time,
                                                    'log_time': df_log_duration_seconds(this_df),
                                                    'file_size': os.stat(fn).st_size}])])
            summary_df.to_csv(summary_file)
            if output_file:
                this_ofile = f'{output_file}_{ii:03d}.csv'
                this_df.to_csv(this_ofile)
    return num_files



if __name__=='__main__':
    parser = ArgumentParser(prog='mav_log_data_science',
                            description='Process MAV log file(s)')
    parser.add_argument('-p','--path',help='Search path',default='.')
    parser.add_argument('-i','--input',help='Input file',default=None)
    parser.add_argument('-s','--summary',help='Summary file',default='summary.csv')
    parser.add_argument('-o','--output',help='Output file',default=None)
    parser.add_argument('-m','--messages',help='Messages to import',default=['PARM'], nargs='*')
    args = parser.parse_args()
    main(args.path,
         args.input,
         args.summary,
         args.output,
         args.messages)
