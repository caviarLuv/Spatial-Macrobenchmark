
Credit Links
-----
http://wiki.github.com/brianfrankcooper/YCSB/  
https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/
ycsb-users@yahoogroups.com  

Building from source
--------------------

To build the full distribution, with all database bindings:

    mvn clean package -DskipTests

To build a single database binding:

    mvn -pl com.yahoo.ycsb:mongodb-binding -am clean package -DskipTests
    
Memcached
--------------------

Ensure memcached is running before running GeoYCSB.

    memcached -m 1024

Executing Benchmark
--------------------

1. MongoDB

./bin/ycsb load mongodb -P workloads/geo/workloadgm1 -p mongodb.url="mongodb://<public IP here>:27017/grafittiDB?w=1" -p mongodb.auth="true"

./bin/ycsb run mongodb -P workloads/geo/workloadgm1 -p mongodb.url="mongodb://<public IP here>:27017/grafittiDB?w=1" -p mongodb.auth="true"
    
Yahoo! Cloud System Benchmark (YCSB)
====================================
[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)
