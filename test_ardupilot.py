from log_utils import import_log, df_log_duration_seconds, add_real_time_from_filename, add_real_time_from_gps, get_total_flight_time

def test_ardupilot():
    df = import_log('examples/ardupilot/2023-07-06 10-12-18.bin', include_types=['PARM','MODE','GPS','POS','MAVC'])
    print(df.shape)
    assert(df.shape==(2498, 49))
    log_duration = df_log_duration_seconds(df)
    print(log_duration)
    assert(log_duration==97.937126)
    df = add_real_time_from_filename(df)
    assert(df.shape==(2498, 50))
    df = add_real_time_from_gps(df)
    assert(df.shape==(2498, 51))
    flight_time = get_total_flight_time(df)
    print(flight_time)
    assert(int(flight_time)==61)
    df.to_csv('example_ardupilot.csv')

if __name__=='__main__':
    test_ardupilot()