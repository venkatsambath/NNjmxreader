import json
import requests
import time

while True:
    url = 'http://citidse-cdh62101.novalocal:9870/jmx'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
    for section in data["beans"]:
        if section["name"] == "Hadoop:service=NameNode,name=FSNamesystemState":
            topusers = json.loads(section["TopUserOpCounts"])
            for window in topusers["windows"]:
                if window["windowLenMs"] == 60000:
                    for optype in window['ops']:
                        with open('data.csv', 'a') as f:
                            f.write("{},{},{},{}".format(topusers["timestamp"][0:19], optype['opType'], optype['topUsers'][0]['user'], optype['topUsers'][0]['count'])+'\n')
                        print("{},{},{},{}".format(topusers["timestamp"][0:19], optype['opType'], optype['topUsers'][0]['user'], optype['topUsers'][0]['count']))
    time.sleep(10)