# Patrick Herring
# Zee.Aero
# April 25th, 2017
# Modified for use with identical test names being run on different channels
# Changed Get_startstop to only return for a specific channel 

import pypyodbc
import pandas
import numpy as np


def Results_connect(DB_name):
    connection = pypyodbc.connect('Driver={SQL Server};' +
                                  'Server=localhost\SQLEXPRESS;' +
                                  'Database={db};uid=sa;pwd=arbin'.format(db=DB_name))
    cur = connection.cursor()
    return connection, cur


def Get_test_names(c):
    SQLcmd = ("SELECT test_name FROM TestList_Table")
    c.execute(SQLcmd)
    temp = c.fetchall()
    return list(set(map(lambda x: x[0], temp)))


def Get_Test_IDs(c, testname):
    SQLcmd = ("SELECT Test_ID FROM TestList_Table WHERE test_name = ?\
			  ORDER BY First_Start_DateTime")
    Values = [testname]
    c.execute(SQLcmd, Values)
    temp = c.fetchall()
    return list(map(lambda x: x[0], temp))


def Get_Channel_ID(c, testid):
    SQLcmd = ("SELECT Channel_ID FROM Resume_Table WHERE test_id = ?")
    Values = [testid]
    c.execute(SQLcmd, Values)
    temp = c.fetchall()
    return list(map(lambda x: x[0], temp))


def Get_startstop(c, testid, chanid):
    SQLcmd = ("SELECT \
	 		  IV_Ch_ID, First_Start_DateTime, Last_End_DateTime, Databases \
	 		  FROM TestIVChList_Table WHERE test_id = ? AND  IV_Ch_ID = ?\
	 		  ORDER BY First_Start_DateTime, IV_Ch_ID")
    Values = [testid, chanid]
    c.execute(SQLcmd, Values)
    temp = c.fetchall()
    iv, starts, stops, databases = zip(*temp)
    l_iv, l_starts, l_stops, l_databases = list(iv), list(starts), list(stops), list(databases)

    # Check if there is a restart in the channel after the last stop, if so set to last Go-Stop
    # for max of the stops (channel is not actually stopped)
    db_latest = databases[0].split(',')[-2]  # comma at end of string databases
    connection, cur = Results_connect(db_latest)
    SQLcmd = ("SELECT Date_Time FROM Event_Table WHERE test_id = ?")
    Values = [testid]
    cur.execute(SQLcmd, Values)
    temp2 = cur.fetchall()
    try:
        laterevent = max(list(map(lambda x: x[0], temp2))) / 10000000
        if (laterevent > max(l_stops)) & (max(l_stops) > 0):
            l_stops[l_stops.index(max(l_stops))] = laterevent
        # print(laterevent)
    except:
        'no action'
    connection.close()
    return l_iv, l_starts, l_stops, l_databases


def Get_Steps(connection, channelid, mintime, maxtime):
    SQLcmd = ("SELECT date_time, New_Step_ID, New_Cycle_ID FROM Event_Table WHERE\
	 (channel_id=? AND date_time>? AND date_time<?)")  # date_time, New_Step_ID, New_Cycle_ID
    Values = [channelid, mintime, maxtime]
    stepframe = pandas.read_sql(SQLcmd, connection, params=Values, index_col=['date_time'])
    stepframe.columns = ['Step_Index', 'Cycle_Index']
    stepframe.drop_duplicates(inplace=True)
    return stepframe


def Get_rawdata(connection, channelid, mintime, maxtime):
    datatype = (22, 'Current')
    SQLcmd = ("SELECT date_time,data_value FROM Channel_RawData_Table WHERE\
	 (channel_id=? AND date_time>? AND date_time<? AND data_type=?)")
    Values = [channelid, mintime, maxtime, datatype[0]]
    finalframe = pandas.read_sql(SQLcmd, connection, params=Values, index_col=['date_time'])
    finalframe.columns = [datatype[1]]
    finalframe = finalframe[~finalframe.index.duplicated(keep='first')]
    types = [(21, 'Voltage'), (23, 'Charge_Capacity'), (24, 'Discharge_Capacity')
        , (25, 'Charge_Energy'), (26, 'Discharge_Energy'), (27, 'dV/dt'), (30, 'Internal_Resistance')]
    for datatype in types:
        Values = [channelid, mintime, maxtime, datatype[0]]
        df = pandas.read_sql(SQLcmd, connection, params=Values, index_col=['date_time'])
        df.columns = [datatype[1]]
        df = df[~df.index.duplicated(keep='first')]
        finalframe = pandas.concat([finalframe, df], axis=1, join='outer')
    return finalframe


def Get_rawdata_fast(connection, channelid, mintime, maxtime):
    datatype = (22, 'Current')
    SQLcmd = ("SELECT data_type,date_time,data_value FROM Channel_RawData_Table WHERE\
	 (channel_id=? AND date_time>? AND date_time<?)")
    Values = [channelid, mintime, maxtime]
    totalframe = pandas.read_sql(SQLcmd, connection, params=Values)
    if totalframe.empty:
        return totalframe
    totalframe.sort_values(by=['date_time'], inplace=True)
    datagroups = totalframe.groupby(['data_type'])
    print(datagroups.groups.keys())
    finalframe = datagroups.get_group(datatype[0]).copy()
    finalframe.set_index(keys=['date_time'], drop=True, inplace=True)
    finalframe.drop('data_type', axis=1, inplace=True)
    finalframe.columns = [datatype[1]]
    finalframe = finalframe[~finalframe.index.duplicated(keep='first')]
    types = [(21, 'Voltage'), (23, 'Charge_Capacity'), (24, 'Discharge_Capacity')
        , (25, 'Charge_Energy'), (26, 'Discharge_Energy'), (27, 'dV/dt'), (30, 'Internal_Resistance')]
    for datatype in types:
        # Values = [channelid, mintime, maxtime, datatype[0]]
        # df = pandas.read_sql(SQLcmd, connection, params = Values, index_col=['date_time'])
        if datatype[0] in datagroups.groups.keys():
            df = datagroups.get_group(datatype[0]).copy()
        else:
            blank_data = {'data_type': pandas.Series(datatype[0], index=[0]),
                          'date_time': pandas.Series(totalframe.date_time.min(), index=[0]),
                          'data_value': pandas.Series(0, index=[0])}
            df = pandas.DataFrame(blank_data)
        df.sort_values(by=['date_time'], inplace=True)
        df.set_index(keys=['date_time'], drop=True, inplace=True)
        df.drop('data_type', axis=1, inplace=True)
        df.columns = [datatype[1]]
        df = df[~df.index.duplicated(keep='first')]
        finalframe = pandas.concat([finalframe, df], axis=1, join='outer')
    return finalframe


def Get_auxdata(connection, channelid, mintime, maxtime):
    datatype = [(0, 'Aux_Voltage'), (1, 'Temperature'), (1, 'Shelf_Temp'), (1, 'Shelf_Temp2')]
    if channelid > 40:
        shelf_id_1 = 59
        shelf_id_2 = 60
    elif channelid > 32:
        shelf_id_1 = 57
        shelf_id_2 = 58
    elif channelid > 24:
        shelf_id_1 = 55
        shelf_id_2 = 56
    elif channelid > 16:
        shelf_id_1 = 53
        shelf_id_2 = 54
    elif channelid > 8:
        shelf_id_1 = 51
        shelf_id_2 = 52
    else:
        shelf_id_1 = 49
        shelf_id_2 = 50
    SQLcmd = ("SELECT date_time,Data_Value FROM Auxiliary_Table WHERE\
	 			(AuxCh_ID=? AND date_time>? AND date_time<? AND data_type=?)")

    Values = [channelid, mintime, maxtime, datatype[1][0]]
    AuxTframe = pandas.read_sql(SQLcmd, connection, params=Values, index_col=['date_time'])
    AuxTframe.columns = [datatype[1][1]]
    AuxTframe = AuxTframe[~AuxTframe.index.duplicated(keep='first')]

    Values = [shelf_id_1, mintime, maxtime, datatype[2][0]]
    shelfframe = pandas.read_sql(SQLcmd, connection, params=Values, index_col=['date_time'])
    shelfframe.columns = [datatype[2][1]]
    shelfframe = shelfframe[~shelfframe.index.duplicated(keep='first')]

    Values = [shelf_id_2, mintime, maxtime, datatype[3][0]]
    shelf2frame = pandas.read_sql(SQLcmd, connection, params=Values, index_col=['date_time'])
    shelf2frame.columns = [datatype[3][1]]
    shelf2frame = shelf2frame[~shelf2frame.index.duplicated(keep='first')]
    # print(shelf2frame[:,0])
    # time.sleep(15)
    # print(np.shape(AuxTframe))
    # idx1 = np.searchsorted(shelfframe[:,0], AuxTframe[:,0]) - 1
    # mask1 = idx1 >= 0
    # idx2 = np.searchsorted(shelf2frame[:,0], AuxTframe[:,0]) - 1
    # mask2 = idx2 >= 0
    finalframe = AuxTframe
    # finalframe = pandas.concat([AuxTframe, shelfframe, shelf2frame], axis=1, join='inner')
    # finalframe2= pandas.DataFrame([AuxTframe[mask1],shelfframe[idx1][mask1],shelf2frame[idx2][mask2]])
    # print(len(finalframe2))
    print(len(AuxTframe))
    return finalframe


def Fill_times(row):
    return row['date_time'] - row['Step_Time']


def Get_unknowndata(connection, channelid, mintime, maxtime):
    datatype = (30, 'Unknown')
    SQLcmd = ("SELECT date_time,data_value FROM Channel_RawData_Table WHERE\
	 (channel_id=? AND date_time>? AND date_time<? AND data_type=?)")
    Values = [channelid, mintime, maxtime, datatype[0]]
    finalframe = pandas.read_sql(SQLcmd, connection, params=Values, index_col=['date_time'])
    finalframe.columns = [datatype[1]]
    return finalframe


def Get_datatypes(cur, channelid, mintime, maxtime):
    SQLcmd = ("SELECT data_type FROM Channel_RawData_Table WHERE\
	 (channel_id=? AND date_time>? AND date_time<?)")
    Values = [channelid, mintime, maxtime]
    cur.execute(SQLcmd, Values)
    temp = cur.fetchall()
    return set(list(map(lambda x: x[0], temp)))


def Get_Metadata(conn, testid, chanid):
    SQLcmd = ("SELECT * FROM TestIVChList_Table WHERE\
	 			(test_id=? AND iv_ch_id=?)")
    Values = [testid, chanid]
    finalframe = pandas.read_sql(SQLcmd, conn, params=Values)
    return finalframe
