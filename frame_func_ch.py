# Patrick Herring
# For more information contact me at
# pkherring@gmail.com
# April 25th, 2017
# Modified for use with identical test names being run on different channels
# Changed to take both the testid and chan_id as arguments
from os import getenv
import pypyodbc
import pandas
import time
import numpy as np
from sql_func_ch import *

def FullFrame(testid, chan_id, test_fin_time, conn, c):
	frames = []
	ivChs, starts, stops, databases = Get_startstop(c, testid, chan_id) #Get the first start time and last stop time
	if (test_fin_time >= max(stops))&(max(stops)>0): # if there is no newer data, and not running skip
		print(max(stops), test_fin_time)
		# continue
	# if max(stops) == max(starts): # if there is no newer data, and not running skip
	# 	print(max(stops), max(starts))
	# 	returnframe = pandas.DataFrame()
	# 	lasttime = max(stops)
	# 	t2 = time.time()
	# 	return returnframe, lasttime, t2
	listed = list(zip(ivChs, starts, stops, databases))
	for item in listed: # Go through all of the start and stop times to get data
		ivCh = item[0]
		start = item[1]
		stop = item[2]
		# print(item)
		if stop == 0:
			stop = time.time() #If there is no stop time take the current time
		if test_fin_time >= stop: # if there is no newer data skip
			continue
		ti1 = time.time()
		# chan_id = Get_Channel_ID(c, testid) #Get the channel for the test
		# if not chan_id:
		# 	continue

		for DB in item[3].split(',')[:-1]:
			connection, cur = Results_connect(DB)
			print('Get channel query time', time.time() - ti1)
			steps = Get_Steps(connection, chan_id, start*10000000, stop*10000000)
			print('Get steps add query time', time.time() - ti1)
			dataraw = Get_rawdata_fast(connection, chan_id, start*10000000, stop*10000000)
			print('Get raw data add query time', time.time() - ti1)
			dataaux = Get_auxdata(connection, chan_id, start*10000000, stop*10000000)
			t2 = time.time() #Determine how the long the SQL query took (likely to be the slowest process)
			if (dataraw.empty):
				continue
			dataframe = pandas.concat([dataraw, dataaux], axis=1, join='outer') #left join the 
			fullframe = pandas.concat([steps, dataframe], axis=1, join='outer')
			fullframe = fullframe.reset_index() #Creates index column (data point)
			fullframe['Test_Time'] = fullframe.date_time - fullframe.date_time[0]
			fullframe['Step_Time'] = fullframe['date_time']
			fullframe.ix[fullframe.Step_Index.isnull(), 'Step_Time'] = np.NaN
			fullframe.fillna(method='ffill', inplace=True)
			fullframe.fillna(method='bfill', inplace=True)
			fullframe.Step_Time = fullframe.apply(Fill_times, axis=1)
			fullframe.date_time = fullframe.date_time/10000000
			fullframe.Step_Time = fullframe.Step_Time/10000000
			fullframe.Test_Time = fullframe.Test_Time/10000000
			fullframe['AC_Impedance'] = 0
			fullframe['Is_FC_Data'] = 0
			fullframe['ACI_Phase_Angle'] = 0
			fullframe = fullframe[fullframe.Step_Time != 0] # delete the rows that were inserted by steps frame
			fullframe.index.name = 'Data_Point'
			fullframe = fullframe.rename(columns={'date_time': 'DateTime'})
			cols = ['Test_Time', 'DateTime', 'Step_Time', 'Step_Index', 'Cycle_Index',
					'Current', 'Voltage', 'Charge_Capacity', 'Discharge_Capacity', 'Charge_Energy',
					'Discharge_Energy', 'dV/dt','Internal_Resistance','Temperature']#,'Shelf_Temp', 'Shelf_Temp2']
					#cols = ['Test_Time', 'DateTime', 'Step_Time', 'Step_Index', 'Cycle_Index',
					#'Current', 'Voltage', 'Charge_Capacity', 'Discharge_Capacity', 'Charge_Energy',
					#'Discharge_Energy', 'dV/dt', 'Internal_Resistance', 'Is_FC_Data', 'AC_Impedance',
					#'ACI_Phase_Angle', 'Temperature', 'Aux_Voltage']
			frames.append(fullframe[cols])
			connection.close()

	lasttime = max(stops)

	if frames == []:
		t2 = time.time()
		return pandas.DataFrame(columns=['DateTime', 'Cycle_Index']), lasttime, t2

	returnframe = pandas.concat(frames, ignore_index=True)
	returnframe.index.name = 'Data_Point'

	return returnframe, lasttime, t2

def Frame_summary(fullframe):
	cycles = fullframe.Cycle_Index.drop_duplicates()
	summary_frame = pandas.DataFrame(index=cycles, columns=['Test_Time',
					'DateTime', 'Mean_Charge_Current', 'Mean_Discharge_Current',
					'Max_Voltage', 'Min_Voltage', 'Charge_Capacity',
					'Discharge_Capacity', 'Charge_IR', 'Discharge_IR',
					'Mean_Charge_Temperature', 
					'Mean_Discharge_Temperature'])
	for cycle in cycles:
		cycleframe = fullframe[fullframe['Cycle_Index'].isin([cycle])]
		chargecycleframe = cycleframe[cycleframe['Current'] > 0]
		dischargecycleframe = cycleframe[cycleframe['Current'] < 0]
		summary_frame.Charge_Capacity[cycle] = cycleframe.Charge_Capacity.max()
		summary_frame.Discharge_Capacity[cycle] = cycleframe.Discharge_Capacity.max()
		summary_frame.Charge_IR[cycle] = chargecycleframe.Internal_Resistance.mean()
		summary_frame.Discharge_IR[cycle] = dischargecycleframe.Internal_Resistance.mean()
		summary_frame.Test_Time[cycle] = cycleframe.Test_Time.max()
		summary_frame.DateTime[cycle] = cycleframe.DateTime.max()
		summary_frame.Min_Voltage[cycle] = cycleframe.Voltage.min()
		summary_frame.Max_Voltage[cycle] = cycleframe.Voltage.max()
		summary_frame.Mean_Charge_Current[cycle] = chargecycleframe.Current.mean()
		summary_frame.Mean_Discharge_Current[cycle] = dischargecycleframe.Current.mean()
		summary_frame.Mean_Discharge_Temperature[cycle] = dischargecycleframe.Temperature.mean()
		summary_frame.Mean_Charge_Temperature[cycle] = chargecycleframe.Temperature.mean()
	return summary_frame

