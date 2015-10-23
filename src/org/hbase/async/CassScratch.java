package org.hbase.async;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ColumnMap;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.recipes.locks.OneStepDistributedRowLock;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassScratch {

  public static void main(String[] args) {
    AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
    .forCluster("localhost")
    .forKeyspace("tsdb")
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
    )
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
        .setPort(9160)
        .setMaxConnsPerHost(1)
        .setSeeds("127.0.0.1:9160")
    )
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance());

    context.start();
    Keyspace keyspace = context.getClient();
    
    
    System.out.println("May have started up with keyspace: " + keyspace.getKeyspaceName());
    
    ColumnFamily<byte[], byte[]> T =
        new ColumnFamily<byte[], byte[]>(
          "t",              // Column Family Name
          BytesArraySerializer.get(),   // Key Serializer
          BytesArraySerializer.get());  // Column Serializer
    
    MutationBatch m = keyspace.prepareMutationBatch();
    
    m.withRow(T, "abc".getBytes())
      .putColumn(new byte[] { 0, 0 }, new byte[] { 0, 1 });
    
    try {
      OperationResult<Void> result = m.execute();
      System.out.println("Result: " + result);
    } catch (ConnectionException e) {
      e.printStackTrace();
    }
    
    OperationResult<ColumnList<byte[]>> result;
    try {
      result = keyspace.prepareQuery(T)
        .getKey("abc".getBytes())
        .execute();
      
      ColumnList<byte[]> columns = result.getResult();
      
      final Iterator<Column<byte[]>> it = columns.iterator();
      while (it.hasNext()) {
        final Column<byte[]> column = it.next();
        
        System.out.println("Q: " + Arrays.toString(column.getName()) + " V: " 
            + Arrays.toString(column.getByteArrayValue()));
      }
    } catch (ConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    
    ColumnFamily<byte[], String> CTRS =
        new ColumnFamily<byte[], String>(
          "ctrs",              // Column Family Name
          BytesArraySerializer.get(),   // Key Serializer
          StringSerializer.get());  // Column Serializer
    
    // fun with locking homes
    // THIS one works for some reason. The other doesn't yet.
    ColumnPrefixDistributedRowLock<byte[]> lock = 
        new ColumnPrefixDistributedRowLock<byte[]>(keyspace, CTRS, new byte[] { 0 })
            .expireLockAfter(1, TimeUnit.SECONDS);
    
        try {
          // Take the lock while reading ALL the columns in the row
          ColumnMap<String> columns = lock.acquireLockAndReadRow();
                  
          // Modify a value and add it to a batch mutation
          long value = 1;
          //if (columns.get("metrics") != null) {
          if (columns.get(new String(new byte[] { 0, 0, 1 })) != null) {
            value = columns.get("metrics").getLongValue() + 1;
          }
          System.out.println("COUNTER VALUE: " + value);
          m = keyspace.prepareMutationBatch();
          m.withRow(CTRS, new byte[] { 0 })
              //.putColumn("metrics", value, null);
          .putColumn(new String(new byte[] { 0, 0, 1 }), value, null);
          
          // Write data AND release the lock
          lock.releaseWithMutation(m);
      }
      catch (Exception e) {
          try {
            lock.release();
          } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
          e.printStackTrace();
      }
        
        
        ColumnFamily<byte[], byte[]> CTRS2 =
            new ColumnFamily<byte[], byte[]>(
              "ctrs",              // Column Family Name
              BytesArraySerializer.get(),   // Key Serializer
              BytesArraySerializer.get());  // Column Serializer
        
            // fun with locking homes
            OneStepDistributedRowLock<byte[], byte[]> lock2 = 
            new OneStepDistributedRowLock<byte[], byte[]>(keyspace, CTRS2, new byte[] { 1 })
                .expireLockAfter(1, TimeUnit.SECONDS)
                .withDataColumns(true)
                .withColumnStrategy(new ByteRowLockStrategy(new byte[] { 89 }));
        
            try {
              // Take the lock while reading ALL the columns in the row
              ColumnMap<byte[]> columns = lock2.acquireLockAndReadRow();
                      
              // Modify a value and add it to a batch mutation
              long value = 1;
              if (columns.get("metrics".getBytes()) != null) {
                value = columns.get("metrics".getBytes()).getLongValue() + 1;
              }
              System.out.println("COUNTER VALUE: " + value);
              m = keyspace.prepareMutationBatch();
              m.withRow(CTRS2, new byte[] { 0 })
                  .putColumn("metrics".getBytes(), value, null);
              
              // Write data AND release the lock
              lock.releaseWithMutation(m);
          }
          catch (Exception e) {
              try {
                lock.release();
              } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
              }
              e.printStackTrace();
          }
    context.shutdown();
    
    System.out.println("\nall done I hope...");
  }


  /**
   * May need to create keyspaces each time till I can persist the sucker
   * 
   * CREATE KEYSPACE tsdb;
   * USE tsdb;
   * create column family t with comparator = BytesType;
   * 
   * CREATE KEYSPACE tsdbuid;
   * USE tsdbuid;
   * create column family id with comparator = BytesType;
   * create column family name with comparator = BytesType;
   */
}
