import json
import os
import csv

directory = 'JMXL'

header = [
    'Timestamp',
    # UgiMetrics
    'GetGroupsNumOps', 'GetGroupsAvgTime',
    # JvmMetrics
    'ThreadsBlocked', 'ThreadsWaiting', 'ThreadsTimedWaiting',
    'GcTimeMillisParNew', 'GcTimeMillisConcurrentMarkSweep',
    # Placeholder/Other Metrics (as per your original header/row)
    'CallQueueLength', 'RpcProcessingTimeAvgTime', 'RpcQueueTimeAvgTime',

    # RpcDetailedActivityForPort9000 - File System Operations (Basic Read/Write/Metadata)
    'CreateNumOps', 'CreateAvgTime', 'MkdirsNumOps', 'MkdirsAvgTime',
    'DeleteNumOps', 'DeleteAvgTime', 'RenameNumOps', 'RenameAvgTime',
    'Rename2NumOps', 'Rename2AvgTime', 'CompleteNumOps', 'CompleteAvgTime',
    'GetFileInfoNumOps', 'GetFileInfoAvgTime', 'GetBlockLocationsNumOps', 'GetBlockLocationsAvgTime',
    'GetListingNumOps', 'GetListingAvgTime', 'GetContentSummaryNumOps', 'GetContentSummaryAvgTime',
    'FsyncNumOps', 'FsyncAvgTime', 'ConcatNumOps', 'ConcatAvgTime',

    # RpcDetailedActivityForPort9000 - Snapshots
    'CreateSnapshotNumOps', 'CreateSnapshotAvgTime', 'DeleteSnapshotNumOps', 'DeleteSnapshotAvgTime',
    'RenameSnapshotNumOps', 'RenameSnapshotAvgTime',
    'GetSnapshotDiffReportNumOps', 'GetSnapshotDiffReportAvgTime', 'GetSnapshotDiffReportListingNumOps', 'GetSnapshotDiffReportListingAvgTime',

    # RpcDetailedActivityForPort9000 - Datanode Management
    'GetDatanodeReportNumOps', 'GetDatanodeReportAvgTime', 'GetDatanodeStorageReportNumOps', 'GetDatanodeStorageReportAvgTime',
]
# Create an empty list to hold the data
data = [header]
print('\t'.join(header))

# Append the header to the data list

timestamp = ''
# Initialize all metrics to -1 using chained assignment

# JvmMetrics
ThreadsBlocked = ThreadsWaiting = ThreadsTimedWaiting = -1
GcTimeMillisParNew = GcTimeMillisConcurrentMarkSweep = -1

# UgiMetrics
GetGroupsNumOps = GetGroupsAvgTime = -1

# Placeholder/Other Metrics (as per your original header/row)
CallQueueLength = RpcProcessingTimeAvgTime = RpcQueueTimeAvgTime = -1

# RpcDetailedActivityForPort9000 - File System Operations (Basic Read/Write/Metadata)
CreateNumOps = CreateAvgTime = -1
MkdirsNumOps = MkdirsAvgTime = -1
DeleteNumOps = DeleteAvgTime = -1
RenameNumOps = RenameAvgTime = -1
Rename2NumOps = Rename2AvgTime = -1
CompleteNumOps = CompleteAvgTime = -1

GetFileInfoNumOps = GetFileInfoAvgTime = -1
GetBlockLocationsNumOps = GetBlockLocationsAvgTime = -1
GetListingNumOps = GetListingAvgTime = -1
GetContentSummaryNumOps = GetContentSummaryAvgTime = -1

FsyncNumOps = FsyncAvgTime = -1
ConcatNumOps = ConcatAvgTime = -1

# RpcDetailedActivityForPort9000 - Snapshots
CreateSnapshotNumOps = CreateSnapshotAvgTime = -1
DeleteSnapshotNumOps = DeleteSnapshotAvgTime = -1
RenameSnapshotNumOps = RenameSnapshotAvgTime = -1

GetSnapshotDiffReportNumOps = GetSnapshotDiffReportAvgTime = -1
GetSnapshotDiffReportListingNumOps = GetSnapshotDiffReportListingAvgTime = -1

# RpcDetailedActivityForPort9000 - Datanode Management
GetDatanodeReportNumOps = GetDatanodeReportAvgTime = -1
GetDatanodeStorageReportNumOps = GetDatanodeStorageReportAvgTime = -1


for filename in os.listdir(directory):
    jmx_file = open(os.path.join(directory, filename))
    jmx_data = json.load(jmx_file)  # Store JSON data in a different variable
    for section in jmx_data["beans"]:
        try:
            if section["name"] == "Hadoop:service=NameNode,name=JvmMetrics":
                ThreadsBlocked = json.loads(str(section["ThreadsBlocked"]))
                GcTimeMillisParNew = json.loads(str(section.get("GcTimeMillisParNew", -1)))
                ThreadsWaiting = json.loads(str(section["ThreadsWaiting"]))
                ThreadsTimedWaiting = json.loads(str(section["ThreadsTimedWaiting"]))
                GcTimeMillisConcurrentMarkSweep = json.loads(str(section.get("GcTimeMillisConcurrentMarkSweep", -1)))

            if section["name"] == "Hadoop:service=NameNode,name=UgiMetrics":
                GetGroupsNumOps = json.loads(str(section["GetGroupsNumOps"]))
                GetGroupsAvgTime = json.loads(str(section["GetGroupsAvgTime"]))


            if section["name"] == "Hadoop:service=NameNode,name=FSNamesystemState":
                topusers = json.loads(section["TopUserOpCounts"])
                timestamp = topusers["timestamp"][0:19]

            if section["name"] == "Hadoop:service=NameNode,name=RpcActivityForPort9000":
                CallQueueLength = json.loads(str(section["CallQueueLength"]))
                RpcProcessingTimeAvgTime = json.loads(str(section["RpcProcessingTimeAvgTime"]))
                RpcQueueTimeAvgTime = json.loads(str(section["RpcQueueTimeAvgTime"]))

            if section["name"] == "Hadoop:service=NameNode,name=RpcDetailedActivityForPort9000":
                # --- File System Operations (Basic Read/Write/Metadata) ---
                CreateNumOps = json.loads(str(section["CreateNumOps"]))
                CreateAvgTime = json.loads(str(section["CreateAvgTime"]))
                MkdirsNumOps = json.loads(str(section["MkdirsNumOps"]))
                MkdirsAvgTime = json.loads(str(section["MkdirsAvgTime"]))
                DeleteNumOps = json.loads(str(section["DeleteNumOps"]))
                DeleteAvgTime = json.loads(str(section["DeleteAvgTime"]))
                RenameNumOps = json.loads(str(section["RenameNumOps"]))
                RenameAvgTime = json.loads(str(section["RenameAvgTime"]))
                Rename2NumOps = json.loads(str(section["Rename2NumOps"]))
                Rename2AvgTime = json.loads(str(section["Rename2AvgTime"]))
                CompleteNumOps = json.loads(str(section["CompleteNumOps"]))
                CompleteAvgTime = json.loads(str(section["CompleteAvgTime"]))

                GetFileInfoNumOps = json.loads(str(section["GetFileInfoNumOps"]))
                GetFileInfoAvgTime = json.loads(str(section["GetFileInfoAvgTime"]))
                GetBlockLocationsNumOps = json.loads(str(section["GetBlockLocationsNumOps"]))
                GetBlockLocationsAvgTime = json.loads(str(section["GetBlockLocationsAvgTime"]))
                GetListingNumOps = json.loads(str(section["GetListingNumOps"]))
                GetListingAvgTime = json.loads(str(section["GetListingAvgTime"]))
                GetContentSummaryNumOps = json.loads(str(section["GetContentSummaryNumOps"]))
                GetContentSummaryAvgTime = json.loads(str(section["GetContentSummaryAvgTime"]))

                FsyncNumOps = json.loads(str(section["FsyncNumOps"]))
                FsyncAvgTime = json.loads(str(section["FsyncAvgTime"]))
                ConcatNumOps = json.loads(str(section["ConcatNumOps"]))
                ConcatAvgTime = json.loads(str(section["ConcatAvgTime"]))

                # --- Snapshots ---
                CreateSnapshotNumOps = json.loads(str(section["CreateSnapshotNumOps"]))
                CreateSnapshotAvgTime = json.loads(str(section["CreateSnapshotAvgTime"]))
                DeleteSnapshotNumOps = json.loads(str(section["DeleteSnapshotNumOps"]))
                DeleteSnapshotAvgTime = json.loads(str(section["DeleteSnapshotAvgTime"]))
                RenameSnapshotNumOps = json.loads(str(section["RenameSnapshotNumOps"]))
                RenameSnapshotAvgTime = json.loads(str(section["RenameSnapshotAvgTime"]))

                GetSnapshotDiffReportNumOps = json.loads(str(section["GetSnapshotDiffReportNumOps"]))
                GetSnapshotDiffReportAvgTime = json.loads(str(section["GetSnapshotDiffReportAvgTime"]))
                GetSnapshotDiffReportListingNumOps = json.loads(str(section["GetSnapshotDiffReportListingNumOps"]))
                GetSnapshotDiffReportListingAvgTime = json.loads(str(section["GetSnapshotDiffReportListingAvgTime"]))

                # --- Datanode Management ---
                GetDatanodeReportNumOps = json.loads(str(section["GetDatanodeReportNumOps"]))
                GetDatanodeReportAvgTime = json.loads(str(section["GetDatanodeReportAvgTime"]))
                GetDatanodeStorageReportNumOps = json.loads(str(section["GetDatanodeStorageReportNumOps"]))
                GetDatanodeStorageReportAvgTime = json.loads(str(section["GetDatanodeStorageReportAvgTime"]))

                row = [
                    timestamp,
                    # UgiMetrics
                    GetGroupsNumOps, GetGroupsAvgTime,
                    # JvmMetrics
                    ThreadsBlocked, ThreadsWaiting, ThreadsTimedWaiting,
                    GcTimeMillisParNew, GcTimeMillisConcurrentMarkSweep,
                    # Placeholder/Other Metrics
                    CallQueueLength, RpcProcessingTimeAvgTime, RpcQueueTimeAvgTime,

                    # RpcDetailedActivityForPort9000 - File System Operations (Basic Read/Write/Metadata)
                    CreateNumOps, CreateAvgTime, MkdirsNumOps, MkdirsAvgTime,
                    DeleteNumOps, DeleteAvgTime, RenameNumOps, RenameAvgTime,
                    Rename2NumOps, Rename2AvgTime, CompleteNumOps, CompleteAvgTime,
                    GetFileInfoNumOps, GetFileInfoAvgTime, GetBlockLocationsNumOps, GetBlockLocationsAvgTime,
                    GetListingNumOps, GetListingAvgTime, GetContentSummaryNumOps, GetContentSummaryAvgTime,
                    FsyncNumOps, FsyncAvgTime, ConcatNumOps, ConcatAvgTime,

                    # RpcDetailedActivityForPort9000 - Snapshots
                    CreateSnapshotNumOps, CreateSnapshotAvgTime, DeleteSnapshotNumOps, DeleteSnapshotAvgTime,
                    RenameSnapshotNumOps, RenameSnapshotAvgTime,
                    GetSnapshotDiffReportNumOps, GetSnapshotDiffReportAvgTime, GetSnapshotDiffReportListingNumOps, GetSnapshotDiffReportListingAvgTime,

                    # RpcDetailedActivityForPort9000 - Datanode Management
                    GetDatanodeReportNumOps, GetDatanodeReportAvgTime, GetDatanodeStorageReportNumOps, GetDatanodeStorageReportAvgTime,
                ]
                print('\t'.join(map(str, row)))

                if all(x != -1 for x in row[1:]):
                    data.append(row)
                    print("Included:", row)
                else:
                    print("Skipped (incomplete):", row)

        except KeyError as e:
            print(f"KeyError occurred: {e}")

filename = 'data.csv'

with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)