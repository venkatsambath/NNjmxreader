import json
import os
import csv

directory = 'JMX2'

# GetGroupsNumOps = -1
# GetGroupsAvgTime = 3.14
# CallQueueLength = -1
# RpcProcessingTimeAvgTime = 3.14
# RpcQueueTimeAvgTime = 3.14
# CollectionTime = -1
# timestamp = 0

header = ['Timestamp', 'GetGroupsNumOps', 'GetGroupsAvgTime', 'CallQueueLength',
          'RpcProcessingTimeAvgTime', 'RpcQueueTimeAvgTime', 'GcTimeMillisParNew','ThreadsBlocked','ThreadsWaiting',
          'ThreadsTimedWaiting','GcTimeMillisConcurrentMarkSweep','Rename2AvgTime','RenameAvgTime','DeleteAvgTime','CreateAvgTime','GetFileInfoAvgTime','GetAclStatusAvgTime','GetListingAvgTime','GetContentSummaryAvgTime','GetBlockLocationsAvgTime','CompleteAvgTime','ListCachePoolsAvgTime','ListCacheDirectivesAvgTime','FsyncAvgTime','GetDatanodeReportAvgTime']

# Create an empty list to hold the data
data = [header]

# Append the header to the data list

timestamp = ''
GetGroupsNumOps = GetGroupsAvgTime = CallQueueLength = RpcProcessingTimeAvgTime = RpcQueueTimeAvgTime = -1
GcTimeMillisParNew = ThreadsBlocked = ThreadsWaiting = ThreadsTimedWaiting = GcTimeMillisConcurrentMarkSweep = -1
ThreadsBlocked = ThreadsWaiting = ThreadsTimedWaiting = GcTimeMillisConcurrentMarkSweep = -1
Rename2AvgTime = RenameAvgTime = DeleteAvgTime = CreateAvgTime = GetFileInfoAvgTime = -1
GetAclStatusAvgTime = GetListingAvgTime = GetContentSummaryAvgTime = GetBlockLocationsAvgTime = -1
CompleteAvgTime = ListCachePoolsAvgTime = ListCacheDirectivesAvgTime = FsyncAvgTime = GetDatanodeReportAvgTime = -1

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
        if section["name"] == "Hadoop:service=NameNode,name=RpcDetailedActivityForPort8020":
            Rename2AvgTime = json.loads(str(section["Rename2AvgTime"]))
            RenameAvgTime = json.loads(str(section["RenameAvgTime"]))
            DeleteAvgTime = json.loads(str(section["DeleteAvgTime"]))
            CreateAvgTime = json.loads(str(section["CreateAvgTime"]))
            GetFileInfoAvgTime = json.loads(str(section["GetFileInfoAvgTime"]))
            GetAclStatusAvgTime = json.loads(str(section["GetAclStatusAvgTime"]))
            IsFileClosedAvgTime = json.loads(str(section["IsFileClosedAvgTime"]))
            GetListingAvgTime = json.loads(str(section["GetListingAvgTime"]))
            GetContentSummaryAvgTime = json.loads(str(section["GetContentSummaryAvgTime"]))
            GetBlockLocationsAvgTime = json.loads(str(section["GetBlockLocationsAvgTime"]))
            CompleteAvgTime = json.loads(str(section["CompleteAvgTime"]))
            ListCachePoolsAvgTime = json.loads(str(section["ListCachePoolsAvgTime"]))
            ListCacheDirectivesAvgTime = json.loads(str(section["ListCacheDirectivesAvgTime"]))
            FsyncAvgTime = json.loads(str(section["FsyncAvgTime"]))
            GetDatanodeReportAvgTime = json.loads(str(section["GetDatanodeReportAvgTime"]))

            print(timestamp, GetGroupsNumOps, GetGroupsAvgTime, CallQueueLength, RpcProcessingTimeAvgTime,
                  RpcQueueTimeAvgTime, GcTimeMillisParNew,ThreadsBlocked,ThreadsWaiting,ThreadsTimedWaiting,GcTimeMillisConcurrentMarkSweep,Rename2AvgTime,RenameAvgTime,DeleteAvgTime,CreateAvgTime,GetFileInfoAvgTime,GetAclStatusAvgTime,GetListingAvgTime,GetContentSummaryAvgTime,GetBlockLocationsAvgTime,CompleteAvgTime,ListCachePoolsAvgTime,ListCacheDirectivesAvgTime,FsyncAvgTime,GetDatanodeReportAvgTime)

            row = [timestamp, GetGroupsNumOps, GetGroupsAvgTime, CallQueueLength, RpcProcessingTimeAvgTime,
                              RpcQueueTimeAvgTime, GcTimeMillisParNew,ThreadsBlocked,ThreadsWaiting,ThreadsTimedWaiting,GcTimeMillisConcurrentMarkSweep,Rename2AvgTime,RenameAvgTime,DeleteAvgTime,CreateAvgTime,GetFileInfoAvgTime,GetAclStatusAvgTime,GetListingAvgTime,GetContentSummaryAvgTime,GetBlockLocationsAvgTime,CompleteAvgTime,ListCachePoolsAvgTime,ListCacheDirectivesAvgTime,FsyncAvgTime,GetDatanodeReportAvgTime]
#            row = [timestamp, GetGroupsNumOps, GetGroupsAvgTime, CallQueueLength,
#                   RpcProcessingTimeAvgTime, RpcQueueTimeAvgTime, GcTimeMillisParNew,ThreadsBlocked,ThreadsWaiting,ThreadsTimedWaiting,GcTimeMillisConcurrentMarkSweep]
            if all(x != -1 for x in row[1:]):  # Exclude timestamp from the check
               data.append(row)
               print("Included:", row)
            else:
               print("Skipped (incomplete):", row)

filename = 'data.csv'
with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)
