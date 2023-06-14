import json
import os

directory = 'JMX'
for filename in os.listdir(directory):
    jmx_file = open(os.path.join(directory, filename))
    data = json.load(jmx_file)
    for section in data["beans"]:
        if section["name"] == "Hadoop:service=NameNode,name=FSNamesystemState":
            topusers = json.loads(section["TopUserOpCounts"])
            print("{}".format(topusers["timestamp"][0:19]))




