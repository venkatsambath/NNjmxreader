import json
import os
import csv

directory = 'JMX'

GetGroupsNumOps = -1
GetGroupsAvgTime = 3.14
CallQueueLength = -1
RpcProcessingTimeAvgTime = 3.14
RpcQueueTimeAvgTime = 3.14
CollectionTime = -1
timestamp = 0

header = ['Timestamp', 'GetGroupsNumOps', 'GetGroupsAvgTime', 'CallQueueLength',
          'RpcProcessingTimeAvgTime', 'RpcQueueTimeAvgTime', 'GcTimeMillisParNew','ThreadsBlocked','ThreadsWaiting',
          'ThreadsTimedWaiting','GcTimeMillisConcurrentMarkSweep']

# Create an empty list to hold the data
data = [header]

# Append the header to the data list

for filename in os.listdir(directory):
    jmx_file = open(os.path.join(directory, filename))
    jmx_data = json.load(jmx_file)  # Store JSON data in a different variable
    for section in jmx_data["beans"]:
        if section["name"] == "Hadoop:service=NameNode,name=JvmMetrics":
            ThreadsBlocked = json.loads(str(section["ThreadsBlocked"]))
            GcTimeMillisParNew = json.loads(str(section["GcTimeMillisParNew"]))
            ThreadsWaiting = json.loads(str(section["ThreadsWaiting"]))
            ThreadsTimedWaiting = json.loads(str(section["ThreadsTimedWaiting"]))
            GcTimeMillisConcurrentMarkSweep = json.loads(str(section["GcTimeMillisConcurrentMarkSweep"]))
        if section["name"] == "Hadoop:service=NameNode,name=UgiMetrics":
            GetGroupsNumOps = json.loads(str(section["GetGroupsNumOps"]))
            GetGroupsAvgTime = json.loads(str(section["GetGroupsAvgTime"]))
        if section["name"] == "Hadoop:service=NameNode,name=FSNamesystemState":
            topusers = json.loads(section["TopUserOpCounts"])
            timestamp = topusers["timestamp"][0:19]
        if section["name"] == "Hadoop:service=NameNode,name=RpcActivityForPort8020":
            CallQueueLength = json.loads(str(section["CallQueueLength"]))
            RpcProcessingTimeAvgTime = json.loads(str(section["RpcProcessingTimeAvgTime"]))
            RpcQueueTimeAvgTime = json.loads(str(section["RpcQueueTimeAvgTime"]))
            print(timestamp, GetGroupsNumOps, GetGroupsAvgTime, CallQueueLength, RpcProcessingTimeAvgTime,
                  RpcQueueTimeAvgTime, GcTimeMillisParNew,ThreadsBlocked,ThreadsWaiting,ThreadsTimedWaiting,GcTimeMillisConcurrentMarkSweep)
            row = [timestamp, GetGroupsNumOps, GetGroupsAvgTime, CallQueueLength,
                   RpcProcessingTimeAvgTime, RpcQueueTimeAvgTime, GcTimeMillisParNew,ThreadsBlocked,ThreadsWaiting,ThreadsTimedWaiting,GcTimeMillisConcurrentMarkSweep]
            data.append(row)

filename = 'data.csv'
with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)
