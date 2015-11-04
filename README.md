# AsyncCassandra 

AsyncCassandra is a shim for using Cassandra as a backend for OpenTSDB instead of
the default HBase backend. Because you can configure Cassandra to operate similar
to HBase, albeit without the consistency guarantees, swapping out the backend is
pretty easy.

Enforcing the TSD schema on Cassandra requires using the Thrift interface and this
library uses Netflix's Astyanax library for a higher level API. Also note that
because this is a shim and not a low level client, performance may not match that
of AsyncHBase. Additionally, because Cassandra doesn't support atomic mutations, 
locking for atomic increments is implemented by writing a special lock column and
checking the timestamps to see who won with retries if acquisition failed. That means
UID assignments will be messy.

To use TSDB with Cassandra, your cluster must use the ByteOrderedPartitioner:
````
partitioner:  org.apache.cassandra.dht.ByteOrderedPartitioner
````

## Build instructions

Maven has been adopted as the building tool for this project. To produce jar file, run 

    mvn clean package

For OpenTSDB, make sure you're using version 2.3 from GitHub (as of 11/2/2015 the "put" branch) and compile with

```
sh build-cassandra.sh
````

## Configuration

In your opentsdb.conf file, set ``asynccassandra.seeds`` to the proper value for your Cassandra cluster. E.g. ``asynccassandra.seeds=127.0.0.1:9160``
Also set ``tsd.storage.hbase.uid_table = tsdbuid`` and ``tsd.storage.hbase.data_table = tsdb``