# NNjmxreader

This script reads through a directory containing jmx files. Reads each jmx file and finds TopUserOpCounts bean and inside 1 min window, it lists topusers for each type of rpc in 1 min window.

Lets say you want to find top users of getfileinfo rpc in each minute, then you can run the command like below

python3 script.py | sort -t, -k1 | grep "getfileinfo"

Note in each 1 min window there can be multiple top users listed for getfileinfo - For simplicity purpose, I am just printing the first topuser alone.
