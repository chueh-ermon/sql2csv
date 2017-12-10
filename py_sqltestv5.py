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
while 1 == 1:
	host = 'SQLHOST' # connection information for the local SQL server
	user = 'sa'
	password = 'arbin'
	database = 'master'
	abandoned_time = 1209600
	datafolder = os.path.normpath('C:/Data/')
	convert_file = 'converted_test_channels.pickle'
	channel_delimiter = '_CH'
	Excluded_tests = ['AB-CC_CV_5V5A_Cap-5V200F','AB-CC_CV_5V10A_Cap-5V200F',
					  'AB-AUXT_V', 'AB-CC_CV_5V2_5A_Cap-5V50F', 'AB-FUNSC_5V10A_Cap-5V200F',
					  '11', '22', '222', 'AB-CC_CV_5V2_5A_Cap-5V50F-04',
	                                  'AB-FUNSC_5V5A_Cap-5V200F', 'AB-FUNSC_5V5A_Cap-5V1300F', 'AB-CC_CV_5V2_5A_Cap-5V50F-02', 'Test',
	                                  'TEST', 'TEST_CAL', 'TEST_chan05', 'TEST_chan05_02', 'TEST2',
	                                  '111', 'AB-CC_CV_5V2_5A_Cap-5V50F-03', '00', 'Test2',
	                                  'Arbin_newA_chan8_test_02', 'Arbin_newA_chan8_test_03',
	                                  '20170509_4_8C-3_6V_test',
	                                  '20170509_6_0C-3_6V_test','20170509_4C_1C_3_6V',
	                                  '20170510_testpolicies','2017-05-18_restingforaesthetics',
	                                  'GITT_aged_cells', '2017-06-30_CH14']

	conn = pypyodbc.connect('Driver={SQL Server};'
							'Server=localhost\SQLEXPRESS;'
							'Database=ArbinMasterData;'
							'uid=sa;pwd=arbin')

	t0 = time.time()
	c = conn.cursor()
	############# Uncomment for Production ###############
	testnames = Get_test_names(c)
	testnames = list(set(testnames)-set(Excluded_tests))
	############ Comment out for Production #############
	# testnames = ['2017-05-12_6C-50per_3_6C']
	######################################################

	testname_chs =[]
	for test in testnames:
		test_ids = Get_Test_IDs(c, test)
		testid = test_ids[-1] # just take the most recent test with this name
		chans = Get_Channel_ID(c, testid)
		for chan in chans:
			testname_chs.append([test, testid, chan])

	############### Comment out for Production ###########
	#print(testname_chs[0:6])
	#testname_chs = testname_chs[0:6]
	######################################################

	try: 
		converted_tests = pandas.read_pickle(convert_file)
	except:
		converted_tests = pandas.DataFrame(columns=['converted_test_ch', 'lasttime','record_length'])

	for testname_ch in testname_chs:
		name = testname_ch[0] + channel_delimiter + str(testname_ch[2] + 1) # +1 for liveware indexing
		if name in converted_tests.converted_test_ch.unique():
			test_fin_times = converted_tests.lasttime[converted_tests['converted_test_ch'] == name]
			test_lengths = converted_tests.record_length[converted_tests['converted_test_ch'] == name]
			test_fin_time = test_fin_times.max()
			test_length = test_lengths.max()
			print('Updating: ', name)
		else:
			test_fin_time = -1
			newrow = pandas.DataFrame([[name, test_fin_time]], columns=['converted_test_ch', 'lasttime'])
			converted_tests = converted_tests.append(newrow, ignore_index=True)
			test_length = 0
			print('New test:', name)

	# 	Test_ids = Get_Test_IDs(c, testname)
		MetadataFrame = Get_Metadata(conn, testname_ch[1], testname_ch[2])
		TestFrame, lasttime, t2 = FullFrame(testname_ch[1], testname_ch[2], test_fin_time, conn, c)

		framelength = TestFrame['Cycle_Index'].count()
		
		if (lasttime <= test_fin_time)&(lasttime > 0):
			print('Already converted:', name)
		else:
			# Test_summary = Frame_summary(TestFrame)
			# Test_summary.to_csv(os.path.join(datafolder,'CycSum_' + testname + '.csv'), sep=',')

			TestFrame.to_csv(os.path.join(datafolder, name + '.csv'), sep=',')
			MetadataFrame.to_csv(os.path.join(datafolder, name + '_Metadata' + '.csv'), sep=',')

			if framelength == test_length: 
				lasttime = time.time()

		converted_tests.loc[converted_tests.converted_test_ch == name, 'lasttime'] = lasttime
		converted_tests.loc[converted_tests.converted_test_ch == name, 'record_length'] = framelength
	print(converted_tests)
	converted_tests.to_pickle(convert_file)
	conn.close()

	t1 = time.time()
	print("Total run time:", t1-t0)
	print("Query time:", t2-t0)
	time.sleep(1500)
