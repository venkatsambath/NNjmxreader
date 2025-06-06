# NNjmxreader

This script reads through a directory containing jmx files. Reads each jmx file and finds TopUserOpCounts bean and inside 1 min window, it lists topusers for each type of rpc in 1 min window

First user in the list will be the top user
https://github.infra.cloudera.com/CDH/hadoop/blob/5bd7b592e5fbe4d448fd127c15d29f3121b8a833/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/top/window/RollingWindowManager.java#L203-L208

Lets say you want to find top users of getfileinfo rpc in each minute, then you can run the command like below

python3 script.py | sort -t, -k1 | grep "getfileinfo"

Note in each 1 min window there can be multiple top users listed for getfileinfo - For simplicity purpose, I am just printing the first topuser alone.

Example result looks like this

[svenkataramanasam@casefiles 883131]$ python3 script.py | sort -t, -k1 | grep "getfile" | head -100
2022-06-28T20:33:32,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,69037
2022-06-28T20:34:32,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,70479
2022-06-28T20:35:33,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,77705
2022-06-28T20:36:33,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,73775
2022-06-28T20:37:33,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,89094
2022-06-28T20:38:34,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,98314
2022-06-28T20:39:34,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,120099
2022-06-28T20:41:52,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,124827
2022-06-28T20:42:53,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,123247
2022-06-28T20:43:53,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,115530
2022-06-28T20:44:53,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,122609
2022-06-28T20:45:54,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,126021
2022-06-28T20:46:54,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,126122
2022-06-28T20:47:55,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,134628
2022-06-28T20:48:55,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,145863
2022-06-28T20:49:56,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,123435
2022-06-28T20:50:56,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,113205
2022-06-28T20:51:56,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,118928
2022-06-28T20:52:57,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,122075
2022-06-28T20:53:57,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,102452
2022-06-28T20:54:58,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,118713
2022-06-28T20:55:58,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,108840
2022-06-28T20:56:58,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,115828
2022-06-28T20:57:59,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,127815
2022-06-28T20:58:59,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,119847
2022-06-28T21:00:00,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,132409
2022-06-28T21:01:00,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,126348
2022-06-28T21:02:01,getfileinfo,xxxxxx/testhost004x03h5test-host@domain.com,155433

[svenkataramanasam@casefiles 883131]$ python3 script.py | sort -t, -k1 | awk -F, '{print $2}' | sort | uniq
*
append
cancelDelegationToken
concat
contentSummary
create
datanodeReport
delete
getAclStatus
getDelegationToken
getEZForPath
getfileinfo
listCacheDirectives
listCachePools
listEncryptionZones
listSnapshottableDirectory
listStatus
mkdirs
open
rename
rename (options=[OVERWRITE])
rename (options=[TO_TRASH])
renewDelegationToken
rollEditLog
safemode_get
setAcl
setErasureCodingPolicy
setOwner
setPermission
setReplication
setStoragePolicy
setTimes

Handy commands:
To list type of RPCs triggered across JMX files:
python3 script.py | sort -t, -k1 | awk -F, '{print $2}' | sort | uniq

To list top user for each rpc type on a minute window
python3 script.py | sort -t, -k1

To list top user for getfileinfo rpc type on a minute window
python3 script.py | sort -t, -k1 | grep getfileinfo

To list top user for listStatus rpc type on a minute window
python3 script.py | sort -t, -k1 | grep listStatus

To list top user for listStatus rpc type on a minute window
python3 script.py | sort -t, -k1 | grep listStatus

To list top user for rename rpc type on a minute window
python3 script.py | sort -t, -k1 | grep rename

To list top user for contentSummary rpc type on a minute window
python3 script.py | sort -t, -k1 | grep contentSummary

Just download the file and change
directory = '<path to your jmx dump folder>' in the script
