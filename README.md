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

