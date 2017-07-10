# Patrick Herring
# Zee.Aero
# April 25th, 2017
# Modified for use with identical test names being run on different channels
# Behavior is chosen to export only the most recent version of each test name
# 
import os
import pypyodbc
import pandas
import csv
import time
import numpy as np
import pickle
from sql_func_ch import *
from frame_func_ch import *

data_folder = os.path.normpath('C:/Data/')
path_converted_test_channels = 'converted_test_channels.pickle'
channel_delimiter = '_CH'
Excluded_tests = ['AB-CC_CV_5V5A_Cap-5V200F', 'AB-CC_CV_5V10A_Cap-5V200F',
                  'AB-AUXT_V', 'AB-CC_CV_5V2_5A_Cap-5V50F', 'AB-FUNSC_5V10A_Cap-5V200F',
                  '11', '22', '222', 'AB-CC_CV_5V2_5A_Cap-5V50F-04',
                  'AB-FUNSC_5V5A_Cap-5V200F', 'AB-FUNSC_5V5A_Cap-5V1300F', 'AB-CC_CV_5V2_5A_Cap-5V50F-02', 'Test',
                  'TEST', 'TEST_CAL', 'TEST_chan05', 'TEST_chan05_02', 'TEST2',
                  '111', 'AB-CC_CV_5V2_5A_Cap-5V50F-03', '00', 'Test2',
                  'Arbin_newA_chan8_test_02', 'Arbin_newA_chan8_test_03',
                  '20170509_4_8C-3_6V_test',
                  '20170509_6_0C-3_6V_test', '20170509_4C_1C_3_6V',
                  '20170510_testpolicies', '20170411-NP-a123fc-rev2',
                  '20170411-np-6C3C', '20170411-np-NP_overpotential', '2017-05-18_restingforaesthetics',
                  'GITT_aged_cells', '2017-06-30', '2017-06-29-final_policy_test', '20170628_Rest',
                  '2017-06-27_current_logging_test',
                  '2017-06-27_6C_test', '2017-06-27_6C_Rest_Test', '2017-06-26_6C_Rest_Test',
                  '2017-06-22_6C_50per_3C_1sTimeStep',
                  '2017-05-12_8C-35per_3_6C', '2017-05-12_8C-25per_3_6C', '2017-05-12_8C-15per_3_6C',
                  '2017-05-12_7C-40per_3C', '2017-05-12_7C-40per_3_6C', '2017-05-12_7C-30per_3_6C',
                  '2017-05-12_6C-60per_3C', '2017-05-12_6C-50per_3_6C', '2017-05-12_6C-50per_3C',
                  '2017-05-12_6C-40per_3_6C', '2017-05-12_6C-40per_3C', '2017-05-12_6C-30per_3_6C',
                  '2017-05-12_5_4C-80per_5_4C', '2017-05-12_5_4C-70per_3C', '2017-05-12_5_4C-60per_3_6C',
                  '2017-05-12_5_4C-60per_3C', '2017-05-12_5_4C-50per_3_6C', '2017-05-12_5_4C-50per_3C',
                  '2017-05-12_5_4C-40per_3_6C', '2017-05-12_4C-80per_4C', '2017-05-12_4_8C-80per_4_8C',
                  '2017-05-12_4_4C-80per_4_4C', '2017-05-12_3_6C-80per_3_6C']


def parser():
    conn = pypyodbc.connect('Driver={SQL Server};'
                            'Server=localhost\SQLEXPRESS;'
                            'Database=ArbinMasterData;'
                            'uid=sa;pwd=arbin')

    t0 = time.time()
    c = conn.cursor()

    test_names = Get_test_names(c)
    test_names = list(set(test_names) - set(Excluded_tests))

    test_name_chs = []
    for test in test_names:
        test_ids = Get_Test_IDs(c, test)
        test_id = test_ids[-1]  # just take the most recent test with this name
        channels = Get_Channel_ID(c, test_id)
        for chan in channels:
            test_name_chs.append([test, test_id, chan])

    try:
        converted_tests = pandas.read_pickle(path_converted_test_channels)
    except:
        converted_tests = pandas.DataFrame(columns=['converted_test_ch', 'lasttime', 'record_length'])

    for test_name_ch in test_name_chs:
        name = test_name_ch[0] + channel_delimiter + str(test_name_ch[2] + 1)  # +1 for liveware indexing
        if name in converted_tests.converted_test_ch.unique():
            test_fin_times = converted_tests.lasttime[converted_tests['converted_test_ch'] == name]
            test_lengths = converted_tests.record_length[converted_tests['converted_test_ch'] == name]
            test_fin_time = test_fin_times.max()
            test_length = test_lengths.max()
            print('Updating: ', name)
        else:
            test_fin_time = -1
            new_row = pandas.DataFrame([[name, test_fin_time]], columns=['converted_test_ch', 'lasttime'])
            converted_tests = converted_tests.append(new_row, ignore_index=True)
            test_length = 0
            print('New test:', name)

        metadata_frame = Get_Metadata(conn, test_name_ch[1], test_name_ch[2])
        test_frame, last_time, t2 = FullFrame(test_name_ch[1], test_name_ch[2], test_fin_time, conn, c)

        frame_length = test_frame['Cycle_Index'].count()

        if (last_time <= test_fin_time) & (last_time > 0):
            print('Already converted:', name)
        else:
            test_frame.to_csv(os.path.join(data_folder, name + '.csv'), sep=',')
            metadata_frame.to_csv(os.path.join(data_folder, name + '_Metadata' + '.csv'), sep=',')
            if frame_length == test_length:
                last_time = time.time()
        converted_tests.loc[converted_tests.converted_test_ch == name, 'lasttime'] = last_time
        converted_tests.loc[converted_tests.converted_test_ch == name, 'record_length'] = frame_length
    print(converted_tests)
    converted_tests.to_pickle(path_converted_test_channels)
    conn.close()

    t1 = time.time()
    print("Total run time:", t1 - t0)
    print("Query time:", t2 - t0)


def main():
    while True:
        parser()
        time.sleep(1500)

if __name__ == "__main__":
    main()