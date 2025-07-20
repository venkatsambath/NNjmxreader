while (true); do curl -k http://localhost:9870/jmx >> JMXL/logs_NN_`hostname -f`_`date +%Y-%m-%d.%H:%M:%S`; sleep 10s; done

