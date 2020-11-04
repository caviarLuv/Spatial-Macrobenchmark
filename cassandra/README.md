# Cassandra GeoYCSB with GeoMesa

## Getting Started
Cassandra GeoYCSB client is CassandraGeomesaClient. To get started, install Cassandra on your machine. You may download from https://cassandra.apache.org/download/.

## Build

    mvn -pl cassandra -am clean package install -DskipTests
    
## Benchmark
To benchmark Cassandra using GeoYCSB, first create a keyspace in Cassandra.
Example:

    CREATE KEYSPACE grafittiDB WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

Run memcache before executing the benchmark.

    memcached -m 1024
    
### Microbenchmark
    ./bin/ycsb load cassandra-geomesa -P workloads/geo/workloadga -p host="127.0.0.1" -p cassandra.keyspace="grafittiDB" > outputLoad.txt

    ./bin/ycsb rum cassandra-geomesa -P workloads/geo/workloadga -p host="127.0.0.1" -p cassandra.keyspace="grafittiDB" > outputRun.txt

There are several parameter fields you could specify options:
```
host=<Cassandra server IP>                              DEFAULT: localhost
port=<Cassandra server connection port>                 DEFAULT: 9042
keyspace=<name of keyspace pre-created for GeoYCSB>     DEFAULT: grafittiDB
```

### Macrobenchmark
(to be added)

