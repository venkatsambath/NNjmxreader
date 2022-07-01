import json
import os
directory = 'JMX'

for filename in os.listdir(directory):
    jmx_file = open(os.path.join(directory, filename))
    data = json.load(jmx_file)
    for section in data["beans"]:
        if section["name"] == "Hadoop:service=NameNode,name=FSNamesystemState":
            topusers = json.loads(section["TopUserOpCounts"])
            for window in topusers["windows"]:
                if window["windowLenMs"] == 60000:
                    for optype in window['ops']:
                        print("{},{},{},{}".format(topusers["timestamp"][0:19], optype['opType'], optype['topUsers'][0]['user'], optype['topUsers'][0]['count']))
