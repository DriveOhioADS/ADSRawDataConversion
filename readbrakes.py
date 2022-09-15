import pymongo
import json
import numpy as np
import matplotlib.pyplot as plt
import datetime as DT
import sys
import pandas as pd
import pickle
import itertools

def string_to_datetime(string):
    dv = [string]

    dv_series = pd.Series(dv)
    dv_to_datetime = pd.to_datetime(dv_series)
    dv_to_datetime_list = list(dv_to_datetime)

    return dv_to_datetime_list[0]
#
# date_time_obj = string_to_datetime('2021-11-16T20:36:04.137+00:00')


# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# mydb = myclient["rosbag"]
# mycol = mydb["rosbag"]
#
# metadID = mydb['metadata'].find_one({'experimentID': 3})
# topics = [
# '/ssc/brake_command_echo',
# '/ssc/brake_feedback',
# '/vehicle/brake_cmd',
# '/vehicle/brake_info_report',
# '/vehicle/brake_report'
# ]
topics = {
'/ssc/brake_command_echo':'brake_pedal',
'/ssc/brake_feedback':'brake_pedal',
'/vehicle/brake_cmd':'pedal_cmd',
#'/vehicle/brake_info_report':'pedal_cmd',
'/vehicle/brake_report':'pedal_cmd'
}
#
# dataset = {}
# for tname in topics:
#     query = {'metadataID': metadID['_id'], 'topic': tname}
#     data = []
#     if mycol.find_one(query) is not None:
#         cursor = mycol.find(query)
#         for row in cursor:
#             data.append(row)
#     dataset.update({tname: data})
#
#
# with open('query.pkl', 'wb') as f:
#     pickle.dump(dataset, f, protocol=pickle.HIGHEST_PROTOCOL)
# sys.exit(1)
with open('query.pkl', 'rb') as f:
    dataset = pickle.load(f)
# colors = itertools.cycle(["r", "b", "g"])



for key in topics:
    print(key, '->', topics[key])
    xdata = []
    ydata = []
    for msg in dataset[key]:
        xdata.append(string_to_datetime(msg['timeField']).value)
        ydata.append(msg[topics[key]])
    # xdata = np.subtract(xdata, xdata[0])
    plt.scatter(xdata, ydata, label=key+'->'+topics[key], s=0.1)

ax = plt.gca()
colormap = plt.cm.gist_ncar #nipy_spectral, Set1,Paired
colorst = [colormap(i) for i in np.linspace(0, 0.9, len(ax.collections))]
for t, j1 in enumerate(ax.collections):
    j1.set_color(colorst[t])
    # xdata = j1.axes.get_xdata()
# xdata = []
# ydata = []
# for msg in dataset['/ssc/brake_feedback']:
#     xdata.append(string_to_datetime(msg['timeField']).value)
#     ydata.append(msg['brake_pedal'])
# plt.scatter(xdata, ydata, label="brake_feedback", color='green', s=0.1)
# # xdata = []
# # ydata = []
# # # brake_torque_actual
# # # brake_torque_request
# # for msg in dataset['/vehicle/brake_cmd']:
# #     xdata.append(string_to_datetime(msg['timeField']).value)
# #     ydata.append(msg['pedal_cmd'])
# # plt.scatter(xdata, ydata, label="brake_cmd", color='red', s=0.1)
# xdata = []
# ydata = []
# for msg in dataset['/vehicle/brake_info_report']:
#     xdata.append(string_to_datetime(msg['timeField']).value)
#     ydata.append(msg['pedal_cmd'])
# plt.scatter(xdata, ydata, label="brake_info_report", color='yellow', s=0.1)
# xdata = []
# ydata = []
# # pedal_cmd
# # pedal_input
# for msg in dataset['/vehicle/brake_report']:
#     xdata.append(string_to_datetime(msg['timeField']).value)
#     ydata.append(msg['pedal_cmd'])
# plt.scatter(xdata, ydata, label="brake_report", color='black', s=0.1)
# plt.scatter(ena_time, 1.1*np.ones(len(ena_time)), label="Enable", color='green')
# plt.scatter(enage_time, np.ones(len(enage_time)), s=80, label='Engage', color='red')
# plt.scatter(sdata, 1.125*np.ones(len(sdata)), label='Positive Speed', color='Black')

lgnd = plt.legend(loc='lower right')
for i in range(0,len(lgnd.legendHandles)):
    lgnd.legendHandles[i]._sizes = [30]
# lgnd.legendHandles[0]._sizes = [30]
    # plt.scatter(lat, lon)
    # plt.scatter(maxbrakePos['latitude'], maxbrakePos['longitude'], s=80)
    #
    # plt.xlim(min(lat)-0.0001, max(lat)+0.0001)
# plt.ylim(0.95, 1.15)
# plt.xlabel('Time [us]')
plt.show()
fig = plt.gcf()
fig.set_size_inches(18.5, 10.5)
