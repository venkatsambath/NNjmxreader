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
    'TruncateNumOps', 'TruncateAvgTime', 'AppendNumOps', 'AppendAvgTime',
    'GetFileInfoNumOps', 'GetFileInfoAvgTime', 'GetBlockLocationsNumOps', 'GetBlockLocationsAvgTime',
    'GetListingNumOps', 'GetListingAvgTime', 'GetContentSummaryNumOps', 'GetContentSummaryAvgTime',
    'IsFileClosedNumOps', 'IsFileClosedAvgTime',
    'AddBlockNumOps', 'AddBlockAvgTime', 'UpdateBlockForPipelineNumOps', 'UpdateBlockForPipelineAvgTime',
    'UpdatePipelineNumOps', 'UpdatePipelineAvgTime',
    'FsyncNumOps', 'FsyncAvgTime', 'ConcatNumOps', 'ConcatAvgTime', 'MsyncNumOps', 'MsyncAvgTime',

    # RpcDetailedActivityForPort9000 - Permissions & ACLs & Quotas & XAttrs
    'SetPermissionNumOps', 'SetPermissionAvgTime', 'SetOwnerNumOps', 'SetOwnerAvgTime',
    'GetAclStatusNumOps', 'GetAclStatusAvgTime', 'SetAclNumOps', 'SetAclAvgTime',
    'RemoveAclNumOps', 'RemoveAclAvgTime', 'RemoveDefaultAclNumOps', 'RemoveDefaultAclAvgTime',
    'RemoveAclEntriesNumOps', 'RemoveAclEntriesAvgTime', 'ModifyAclEntriesNumOps', 'ModifyAclEntriesAvgTime',
    'SetQuotaNumOps', 'SetQuotaAvgTime', 'GetQuotaUsageNumOps', 'GetQuotaUsageAvgTime',
    'SetXAttrNumOps', 'SetXAttrAvgTime', 'GetXAttrsNumOps', 'GetXAttrsAvgTime',
    'RemoveXAttrNumOps', 'RemoveXAttrAvgTime', 'ListXAttrsNumOps', 'ListXAttrsAvgTime',
    'CheckAccessNumOps', 'CheckAccessAvgTime',

    # RpcDetailedActivityForPort9000 - Snapshots
    'CreateSnapshotNumOps', 'CreateSnapshotAvgTime', 'DeleteSnapshotNumOps', 'DeleteSnapshotAvgTime',
    'RenameSnapshotNumOps', 'RenameSnapshotAvgTime', 'AllowSnapshotNumOps', 'AllowSnapshotAvgTime',
    'DisallowSnapshotNumOps', 'DisallowSnapshotAvgTime',
    'GetSnapshotListingNumOps', 'GetSnapshotListingAvgTime', 'GetSnapshottableDirListingNumOps', 'GetSnapshottableDirListingAvgTime',
    'GetSnapshotDiffReportNumOps', 'GetSnapshotDiffReportAvgTime', 'GetSnapshotDiffReportListingNumOps', 'GetSnapshotDiffReportListingAvgTime',
    'SnapshotExceptionNumOps', 'SnapshotExceptionAvgTime',

    # RpcDetailedActivityForPort9000 - Cache Management
    'ListCachePoolsNumOps', 'ListCachePoolsAvgTime', 'ListCacheDirectivesNumOps', 'ListCacheDirectivesAvgTime',
    'AddCachePoolNumOps', 'AddCachePoolAvgTime', 'RemoveCachePoolNumOps', 'RemoveCachePoolAvgTime',
    'ModifyCachePoolNumOps', 'ModifyCachePoolAvgTime', 'AddCacheDirectiveNumOps', 'AddCacheDirectiveAvgTime',
    'RemoveCacheDirectiveNumOps', 'RemoveCacheDirectiveAvgTime', 'ModifyCacheDirectiveNumOps', 'ModifyCacheDirectiveAvgTime',
    'CacheReportNumOps', 'CacheReportAvgTime',

    # RpcDetailedActivityForPort9000 - Encryption Zones
    'CreateEncryptionZoneNumOps', 'CreateEncryptionZoneAvgTime', 'ListEncryptionZonesNumOps', 'ListEncryptionZonesAvgTime',
    'GetEZForPathNumOps', 'GetEZForPathAvgTime', 'ReencryptEncryptionZoneNumOps', 'ReencryptEncryptionZoneAvgTime',
    'ListReencryptionStatusNumOps', 'ListReencryptionStatusAvgTime',

    # RpcDetailedActivityForPort9000 - Datanode Management
    'GetDatanodeReportNumOps', 'GetDatanodeReportAvgTime', 'GetDatanodeStorageReportNumOps', 'GetDatanodeStorageReportAvgTime',
    'ReportBadBlocksNumOps', 'ReportBadBlocksAvgTime', 'GetSlowDatanodeReportNumOps', 'GetSlowDatanodeReportAvgTime',
    'RegisterDatanodeNumOps', 'RegisterDatanodeAvgTime', 'RefreshNodesNumOps', 'RefreshNodesAvgTime',
    'BlockReportNumOps', 'BlockReportAvgTime', 'BlockReceivedAndDeletedNumOps', 'BlockReceivedAndDeletedAvgTime',
    'GetAdditionalDatanodeNumOps', 'GetAdditionalDatanodeAvgTime',
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
TruncateNumOps = TruncateAvgTime = -1
AppendNumOps = AppendAvgTime = -1

GetFileInfoNumOps = GetFileInfoAvgTime = -1
GetBlockLocationsNumOps = GetBlockLocationsAvgTime = -1
GetListingNumOps = GetListingAvgTime = -1
GetContentSummaryNumOps = GetContentSummaryAvgTime = -1
IsFileClosedNumOps = IsFileClosedAvgTime = -1

AddBlockNumOps = AddBlockAvgTime = -1
UpdateBlockForPipelineNumOps = UpdateBlockForPipelineAvgTime = -1
UpdatePipelineNumOps = UpdatePipelineAvgTime = -1

FsyncNumOps = FsyncAvgTime = -1
ConcatNumOps = ConcatAvgTime = -1
MsyncNumOps = MsyncAvgTime = -1

# RpcDetailedActivityForPort9000 - Permissions & ACLs & Quotas & XAttrs
SetPermissionNumOps = SetPermissionAvgTime = -1
SetOwnerNumOps = SetOwnerAvgTime = -1
GetAclStatusNumOps = GetAclStatusAvgTime = -1
SetAclNumOps = SetAclAvgTime = -1
RemoveAclNumOps = RemoveAclAvgTime = -1
RemoveDefaultAclNumOps = RemoveDefaultAclAvgTime = -1
RemoveAclEntriesNumOps = RemoveAclEntriesAvgTime = -1
ModifyAclEntriesNumOps = ModifyAclEntriesAvgTime = -1

SetQuotaNumOps = SetQuotaAvgTime = -1
GetQuotaUsageNumOps = GetQuotaUsageAvgTime = -1

SetXAttrNumOps = SetXAttrAvgTime = -1
GetXAttrsNumOps = GetXAttrsAvgTime = -1
RemoveXAttrNumOps = RemoveXAttrAvgTime = -1
ListXAttrsNumOps = ListXAttrsAvgTime = -1

CheckAccessNumOps = CheckAccessAvgTime = -1

# RpcDetailedActivityForPort9000 - Snapshots
CreateSnapshotNumOps = CreateSnapshotAvgTime = -1
DeleteSnapshotNumOps = DeleteSnapshotAvgTime = -1
RenameSnapshotNumOps = RenameSnapshotAvgTime = -1
AllowSnapshotNumOps = AllowSnapshotAvgTime = -1
DisallowSnapshotNumOps = DisallowSnapshotAvgTime = -1

GetSnapshotListingNumOps = GetSnapshotListingAvgTime = -1
GetSnapshottableDirListingNumOps = GetSnapshottableDirListingAvgTime = -1
GetSnapshotDiffReportNumOps = GetSnapshotDiffReportAvgTime = -1
GetSnapshotDiffReportListingNumOps = GetSnapshotDiffReportListingAvgTime = -1

SnapshotExceptionNumOps = SnapshotExceptionAvgTime = -1

# RpcDetailedActivityForPort9000 - Cache Management
ListCachePoolsNumOps = ListCachePoolsAvgTime = -1
ListCacheDirectivesNumOps = ListCacheDirectivesAvgTime = -1
AddCachePoolNumOps = AddCachePoolAvgTime = -1
RemoveCachePoolNumOps = RemoveCachePoolAvgTime = -1
ModifyCachePoolNumOps = ModifyCachePoolAvgTime = -1
AddCacheDirectiveNumOps = AddCacheDirectiveAvgTime = -1
RemoveCacheDirectiveNumOps = RemoveCacheDirectiveAvgTime = -1
ModifyCacheDirectiveNumOps = ModifyCacheDirectiveAvgTime = -1
CacheReportNumOps = CacheReportAvgTime = -1

# RpcDetailedActivityForPort9000 - Encryption Zones
CreateEncryptionZoneNumOps = CreateEncryptionZoneAvgTime = -1
ListEncryptionZonesNumOps = ListEncryptionZonesAvgTime = -1
GetEZForPathNumOps = GetEZForPathAvgTime = -1
ReencryptEncryptionZoneNumOps = ReencryptEncryptionZoneAvgTime = -1
ListReencryptionStatusNumOps = ListReencryptionStatusAvgTime = -1

# RpcDetailedActivityForPort9000 - Datanode Management
GetDatanodeReportNumOps = GetDatanodeReportAvgTime = -1
GetDatanodeStorageReportNumOps = GetDatanodeStorageReportAvgTime = -1
ReportBadBlocksNumOps = ReportBadBlocksAvgTime = -1
GetSlowDatanodeReportNumOps = GetSlowDatanodeReportAvgTime = -1
RegisterDatanodeNumOps = RegisterDatanodeAvgTime = -1
RefreshNodesNumOps = RefreshNodesAvgTime = -1

BlockReportNumOps = BlockReportAvgTime = -1
BlockReceivedAndDeletedNumOps = BlockReceivedAndDeletedAvgTime = -1
GetAdditionalDatanodeNumOps = GetAdditionalDatanodeAvgTime = -1


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
                TruncateNumOps = json.loads(str(section["TruncateNumOps"]))
                TruncateAvgTime = json.loads(str(section["TruncateAvgTime"]))
                AppendNumOps = json.loads(str(section["AppendNumOps"]))
                AppendAvgTime = json.loads(str(section["AppendAvgTime"]))

                GetFileInfoNumOps = json.loads(str(section["GetFileInfoNumOps"]))
                GetFileInfoAvgTime = json.loads(str(section["GetFileInfoAvgTime"]))
                GetBlockLocationsNumOps = json.loads(str(section["GetBlockLocationsNumOps"]))
                GetBlockLocationsAvgTime = json.loads(str(section["GetBlockLocationsAvgTime"]))
                GetListingNumOps = json.loads(str(section["GetListingNumOps"]))
                GetListingAvgTime = json.loads(str(section["GetListingAvgTime"]))
                GetContentSummaryNumOps = json.loads(str(section["GetContentSummaryNumOps"]))
                GetContentSummaryAvgTime = json.loads(str(section["GetContentSummaryAvgTime"]))
                IsFileClosedNumOps = json.loads(str(section["IsFileClosedNumOps"]))
                IsFileClosedAvgTime = json.loads(str(section["IsFileClosedAvgTime"]))

                AddBlockNumOps = json.loads(str(section["AddBlockNumOps"]))
                AddBlockAvgTime = json.loads(str(section["AddBlockAvgTime"]))
                UpdateBlockForPipelineNumOps = json.loads(str(section["UpdateBlockForPipelineNumOps"]))
                UpdateBlockForPipelineAvgTime = json.loads(str(section["UpdateBlockForPipelineAvgTime"]))
                UpdatePipelineNumOps = json.loads(str(section["UpdatePipelineNumOps"]))
                UpdatePipelineAvgTime = json.loads(str(section["UpdatePipelineAvgTime"]))

                FsyncNumOps = json.loads(str(section["FsyncNumOps"]))
                FsyncAvgTime = json.loads(str(section["FsyncAvgTime"]))
                ConcatNumOps = json.loads(str(section["ConcatNumOps"]))
                ConcatAvgTime = json.loads(str(section["ConcatAvgTime"]))
                MsyncNumOps = json.loads(str(section["MsyncNumOps"]))
                MsyncAvgTime = json.loads(str(section["MsyncAvgTime"]))


                # --- Permissions & ACLs & Quotas & XAttrs ---
                SetPermissionNumOps = json.loads(str(section["SetPermissionNumOps"]))
                SetPermissionAvgTime = json.loads(str(section["SetPermissionAvgTime"]))
                SetOwnerNumOps = json.loads(str(section["SetOwnerNumOps"]))
                SetOwnerAvgTime = json.loads(str(section["SetOwnerAvgTime"]))
                GetAclStatusNumOps = json.loads(str(section["GetAclStatusNumOps"]))
                GetAclStatusAvgTime = json.loads(str(section["GetAclStatusAvgTime"]))
                SetAclNumOps = json.loads(str(section["SetAclNumOps"]))
                SetAclAvgTime = json.loads(str(section["SetAclAvgTime"]))
                RemoveAclNumOps = json.loads(str(section["RemoveAclNumOps"]))
                RemoveAclAvgTime = json.loads(str(section["RemoveAclAvgTime"]))
                RemoveDefaultAclNumOps = json.loads(str(section["RemoveDefaultAclNumOps"]))
                RemoveDefaultAclAvgTime = json.loads(str(section["RemoveDefaultAclAvgTime"]))
                RemoveAclEntriesNumOps = json.loads(str(section["RemoveAclEntriesNumOps"]))
                RemoveAclEntriesAvgTime = json.loads(str(section["RemoveAclEntriesAvgTime"]))
                ModifyAclEntriesNumOps = json.loads(str(section["ModifyAclEntriesNumOps"]))
                ModifyAclEntriesAvgTime = json.loads(str(section["ModifyAclEntriesAvgTime"]))

                SetQuotaNumOps = json.loads(str(section["SetQuotaNumOps"]))
                SetQuotaAvgTime = json.loads(str(section["SetQuotaAvgTime"]))
                GetQuotaUsageNumOps = json.loads(str(section["GetQuotaUsageNumOps"]))
                GetQuotaUsageAvgTime = json.loads(str(section["GetQuotaUsageAvgTime"]))

                SetXAttrNumOps = json.loads(str(section["SetXAttrNumOps"]))
                SetXAttrAvgTime = json.loads(str(section["SetXAttrAvgTime"]))
                GetXAttrsNumOps = json.loads(str(section["GetXAttrsNumOps"]))
                GetXAttrsAvgTime = json.loads(str(section["GetXAttrsAvgTime"]))
                RemoveXAttrNumOps = json.loads(str(section["RemoveXAttrNumOps"]))
                RemoveXAttrAvgTime = json.loads(str(section["RemoveXAttrAvgTime"]))
                ListXAttrsNumOps = json.loads(str(section["ListXAttrsNumOps"]))
                ListXAttrsAvgTime = json.loads(str(section["ListXAttrsAvgTime"]))

                CheckAccessNumOps = json.loads(str(section["CheckAccessNumOps"]))
                CheckAccessAvgTime = json.loads(str(section["CheckAccessAvgTime"]))


                # --- Snapshots ---
                CreateSnapshotNumOps = json.loads(str(section["CreateSnapshotNumOps"]))
                CreateSnapshotAvgTime = json.loads(str(section["CreateSnapshotAvgTime"]))
                DeleteSnapshotNumOps = json.loads(str(section["DeleteSnapshotNumOps"]))
                DeleteSnapshotAvgTime = json.loads(str(section["DeleteSnapshotAvgTime"]))
                RenameSnapshotNumOps = json.loads(str(section["RenameSnapshotNumOps"]))
                RenameSnapshotAvgTime = json.loads(str(section["RenameSnapshotAvgTime"]))
                AllowSnapshotNumOps = json.loads(str(section["AllowSnapshotNumOps"]))
                AllowSnapshotAvgTime = json.loads(str(section["AllowSnapshotAvgTime"]))
                DisallowSnapshotNumOps = json.loads(str(section["DisallowSnapshotNumOps"]))
                DisallowSnapshotAvgTime = json.loads(str(section["DisallowSnapshotAvgTime"]))

                GetSnapshotListingNumOps = json.loads(str(section["GetSnapshotListingNumOps"]))
                GetSnapshotListingAvgTime = json.loads(str(section["GetSnapshotListingAvgTime"]))
                GetSnapshottableDirListingNumOps = json.loads(str(section["GetSnapshottableDirListingNumOps"]))
                GetSnapshottableDirListingAvgTime = json.loads(str(section["GetSnapshottableDirListingAvgTime"]))
                GetSnapshotDiffReportNumOps = json.loads(str(section["GetSnapshotDiffReportNumOps"]))
                GetSnapshotDiffReportAvgTime = json.loads(str(section["GetSnapshotDiffReportAvgTime"]))
                GetSnapshotDiffReportListingNumOps = json.loads(str(section["GetSnapshotDiffReportListingNumOps"]))
                GetSnapshotDiffReportListingAvgTime = json.loads(str(section["GetSnapshotDiffReportListingAvgTime"]))

                SnapshotExceptionNumOps = json.loads(str(section["SnapshotExceptionNumOps"]))
                SnapshotExceptionAvgTime = json.loads(str(section["SnapshotExceptionAvgTime"]))


                # --- Cache Management ---
                ListCachePoolsNumOps = json.loads(str(section["ListCachePoolsNumOps"]))
                ListCachePoolsAvgTime = json.loads(str(section["ListCachePoolsAvgTime"]))
                ListCacheDirectivesNumOps = json.loads(str(section["ListCacheDirectivesNumOps"]))
                ListCacheDirectivesAvgTime = json.loads(str(section["ListCacheDirectivesAvgTime"]))
                AddCachePoolNumOps = json.loads(str(section["AddCachePoolNumOps"]))
                AddCachePoolAvgTime = json.loads(str(section["AddCachePoolAvgTime"]))
                RemoveCachePoolNumOps = json.loads(str(section["RemoveCachePoolNumOps"]))
                RemoveCachePoolAvgTime = json.loads(str(section["RemoveCachePoolAvgTime"]))
                ModifyCachePoolNumOps = json.loads(str(section["ModifyCachePoolNumOps"]))
                ModifyCachePoolAvgTime = json.loads(str(section["ModifyCachePoolAvgTime"]))
                AddCacheDirectiveNumOps = json.loads(str(section["AddCacheDirectiveNumOps"]))
                AddCacheDirectiveAvgTime = json.loads(str(section["AddCacheDirectiveAvgTime"]))
                RemoveCacheDirectiveNumOps = json.loads(str(section["RemoveCacheDirectiveNumOps"]))
                RemoveCacheDirectiveAvgTime = json.loads(str(section["RemoveCacheDirectiveAvgTime"]))
                ModifyCacheDirectiveNumOps = json.loads(str(section["ModifyCacheDirectiveNumOps"]))
                ModifyCacheDirectiveAvgTime = json.loads(str(section["ModifyCacheDirectiveAvgTime"]))
                CacheReportNumOps = json.loads(str(section["CacheReportNumOps"]))
                CacheReportAvgTime = json.loads(str(section["CacheReportAvgTime"]))


                # --- Encryption Zones ---
                CreateEncryptionZoneNumOps = json.loads(str(section["CreateEncryptionZoneNumOps"]))
                CreateEncryptionZoneAvgTime = json.loads(str(section["CreateEncryptionZoneAvgTime"]))
                ListEncryptionZonesNumOps = json.loads(str(section["ListEncryptionZonesNumOps"]))
                ListEncryptionZonesAvgTime = json.loads(str(section["ListEncryptionZonesAvgTime"]))
                GetEZForPathNumOps = json.loads(str(section["GetEZForPathNumOps"]))
                GetEZForPathAvgTime = json.loads(str(section["GetEZForPathAvgTime"]))
                ReencryptEncryptionZoneNumOps = json.loads(str(section["ReencryptEncryptionZoneNumOps"]))
                ReencryptEncryptionZoneAvgTime = json.loads(str(section["ReencryptEncryptionZoneAvgTime"]))
                ListReencryptionStatusNumOps = json.loads(str(section["ListReencryptionStatusNumOps"]))
                ListReencryptionStatusAvgTime = json.loads(str(section["ListReencryptionStatusAvgTime"]))


                # --- Datanode Management ---
                GetDatanodeReportNumOps = json.loads(str(section["GetDatanodeReportNumOps"]))
                GetDatanodeReportAvgTime = json.loads(str(section["GetDatanodeReportAvgTime"]))
                GetDatanodeStorageReportNumOps = json.loads(str(section["GetDatanodeStorageReportNumOps"]))
                GetDatanodeStorageReportAvgTime = json.loads(str(section["GetDatanodeStorageReportAvgTime"]))
                ReportBadBlocksNumOps = json.loads(str(section["ReportBadBlocksNumOps"]))
                ReportBadBlocksAvgTime = json.loads(str(section["ReportBadBlocksAvgTime"]))
                GetSlowDatanodeReportNumOps = json.loads(str(section["GetSlowDatanodeReportNumOps"]))
                GetSlowDatanodeReportAvgTime = json.loads(str(section["GetSlowDatanodeReportAvgTime"]))
                RegisterDatanodeNumOps = json.loads(str(section["RegisterDatanodeNumOps"]))
                RegisterDatanodeAvgTime = json.loads(str(section["RegisterDatanodeAvgTime"]))
                RefreshNodesNumOps = json.loads(str(section["RefreshNodesNumOps"]))
                RefreshNodesAvgTime = json.loads(str(section["RefreshNodesAvgTime"]))

                BlockReportNumOps = json.loads(str(section["BlockReportNumOps"]))
                BlockReportAvgTime = json.loads(str(section["BlockReportAvgTime"]))
                BlockReceivedAndDeletedNumOps = json.loads(str(section["BlockReceivedAndDeletedNumOps"]))
                BlockReceivedAndDeletedAvgTime = json.loads(str(section["BlockReceivedAndDeletedAvgTime"]))
                GetAdditionalDatanodeNumOps = json.loads(str(section["GetAdditionalDatanodeNumOps"]))
                GetAdditionalDatanodeAvgTime = json.loads(str(section["GetAdditionalDatanodeAvgTime"]))

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
                    TruncateNumOps, TruncateAvgTime, AppendNumOps, AppendAvgTime,
                    GetFileInfoNumOps, GetFileInfoAvgTime, GetBlockLocationsNumOps, GetBlockLocationsAvgTime,
                    GetListingNumOps, GetListingAvgTime, GetContentSummaryNumOps, GetContentSummaryAvgTime,
                    IsFileClosedNumOps, IsFileClosedAvgTime,
                    AddBlockNumOps, AddBlockAvgTime, UpdateBlockForPipelineNumOps, UpdateBlockForPipelineAvgTime,
                    UpdatePipelineNumOps, UpdatePipelineAvgTime,
                    FsyncNumOps, FsyncAvgTime, ConcatNumOps, ConcatAvgTime, MsyncNumOps, MsyncAvgTime,

                    # RpcDetailedActivityForPort9000 - Permissions & ACLs & Quotas & XAttrs
                    SetPermissionNumOps, SetPermissionAvgTime, SetOwnerNumOps, SetOwnerAvgTime,
                    GetAclStatusNumOps, GetAclStatusAvgTime, SetAclNumOps, SetAclAvgTime,
                    RemoveAclNumOps, RemoveAclAvgTime, RemoveDefaultAclNumOps, RemoveDefaultAclAvgTime,
                    RemoveAclEntriesNumOps, RemoveAclEntriesAvgTime, ModifyAclEntriesNumOps, ModifyAclEntriesAvgTime,
                    SetQuotaNumOps, SetQuotaAvgTime, GetQuotaUsageNumOps, GetQuotaUsageAvgTime,
                    SetXAttrNumOps, SetXAttrAvgTime, GetXAttrsNumOps, GetXAttrsAvgTime,
                    RemoveXAttrNumOps, RemoveXAttrAvgTime, ListXAttrsNumOps, ListXAttrsAvgTime,
                    CheckAccessNumOps, CheckAccessAvgTime,

                    # RpcDetailedActivityForPort9000 - Snapshots
                    CreateSnapshotNumOps, CreateSnapshotAvgTime, DeleteSnapshotNumOps, DeleteSnapshotAvgTime,
                    RenameSnapshotNumOps, RenameSnapshotAvgTime, AllowSnapshotNumOps, AllowSnapshotAvgTime,
                    DisallowSnapshotNumOps, DisallowSnapshotAvgTime,
                    GetSnapshotListingNumOps, GetSnapshotListingAvgTime, GetSnapshottableDirListingNumOps, GetSnapshottableDirListingAvgTime,
                    GetSnapshotDiffReportNumOps, GetSnapshotDiffReportAvgTime, GetSnapshotDiffReportListingNumOps, GetSnapshotDiffReportListingAvgTime,
                    SnapshotExceptionNumOps, SnapshotExceptionAvgTime,

                    # RpcDetailedActivityForPort9000 - Cache Management
                    ListCachePoolsNumOps, ListCachePoolsAvgTime, ListCacheDirectivesNumOps, ListCacheDirectivesAvgTime,
                    AddCachePoolNumOps, AddCachePoolAvgTime, RemoveCachePoolNumOps, RemoveCachePoolAvgTime,
                    ModifyCachePoolNumOps, ModifyCachePoolAvgTime, AddCacheDirectiveNumOps, AddCacheDirectiveAvgTime,
                    RemoveCacheDirectiveNumOps, RemoveCacheDirectiveAvgTime, ModifyCacheDirectiveNumOps, ModifyCacheDirectiveAvgTime,
                    CacheReportNumOps, CacheReportAvgTime,

                    # RpcDetailedActivityForPort9000 - Encryption Zones
                    CreateEncryptionZoneNumOps, CreateEncryptionZoneAvgTime, ListEncryptionZonesNumOps, ListEncryptionZonesAvgTime,
                    GetEZForPathNumOps, GetEZForPathAvgTime, ReencryptEncryptionZoneNumOps, ReencryptEncryptionZoneAvgTime,
                    ListReencryptionStatusNumOps, ListReencryptionStatusAvgTime,

                    # RpcDetailedActivityForPort9000 - Datanode Management
                    GetDatanodeReportNumOps, GetDatanodeReportAvgTime, GetDatanodeStorageReportNumOps, GetDatanodeStorageReportAvgTime,
                    ReportBadBlocksNumOps, ReportBadBlocksAvgTime, GetSlowDatanodeReportNumOps, GetSlowDatanodeReportAvgTime,
                    RegisterDatanodeNumOps, RegisterDatanodeAvgTime, RefreshNodesNumOps, RefreshNodesAvgTime,
                    BlockReportNumOps, BlockReportAvgTime, BlockReceivedAndDeletedNumOps, BlockReceivedAndDeletedAvgTime,
                    GetAdditionalDatanodeNumOps, GetAdditionalDatanodeAvgTime,
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
