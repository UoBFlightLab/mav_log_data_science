from log_utils import get_all_log_paths, import_log_set, get_flight_times, get_total_flight_time

def test_multiple():
    path_list = get_all_log_paths()
    assert(len(path_list)==2)
    all_df = import_log_set(path_list)
    print(all_df.shape)
    assert(all_df.shape==(5132,63))
    flight_times = get_flight_times(all_df)
    print(flight_times)
    total_flight_time = get_total_flight_time(all_df)
    print(total_flight_time)
    assert(int(total_flight_time)==141)
    all_df.to_csv('example_multiple.csv')

if __name__=='__main__':
    test_multiple()