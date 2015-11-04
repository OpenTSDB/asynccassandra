package org.hbase.async;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.hbase.async.Bytes.ByteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
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
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class HBaseClient {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);
  
  public static final byte[] EMPTY_ARRAY = new byte[0];
  
  /** A byte array containing a single zero byte.  */
  static final byte[] ZERO_ARRAY = new byte[] { 0 };
  
  public static final ColumnFamily<byte[], byte[]> TSDB_T = new ColumnFamily<byte[], byte[]>(
      "t",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer

  public static final ColumnFamily<byte[], byte[]> TSDB_UID_NAME = new ColumnFamily<byte[], byte[]>(
      "name",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer
  
  public static final ColumnFamily<byte[], byte[]> TSDB_UID_ID = new ColumnFamily<byte[], byte[]>(
      "id",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      BytesArraySerializer.get());  // Column Serializer
  
  public static final ColumnFamily<byte[], String> TSDB_UID_NAME_CAS = 
      new ColumnFamily<byte[], String>(
      "name",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      StringSerializer.get());  // Column Serializer
  
  public static final ColumnFamily<byte[], String> TSDB_UID_ID_CAS = 
      new ColumnFamily<byte[], String>(
      "id",              // Column Family Name
      BytesArraySerializer.get(),   // Key Serializer
      StringSerializer.get());  // Column Serializer
  
  final Config config;
  final ExecutorService executor = Executors.newFixedThreadPool(25);

  final ByteMap<AstyanaxContext<Keyspace>> contexts = 
      new ByteMap<AstyanaxContext<Keyspace>>();
  final ByteMap<Keyspace> keyspaces = new ByteMap<Keyspace>();
  final ByteMap<ColumnFamily<byte[], byte[]>> column_family_schemas = 
      new ByteMap<ColumnFamily<byte[], byte[]>>();
  
  final AstyanaxConfigurationImpl ast_config;
  final ConnectionPoolConfigurationImpl pool;
  final CountingConnectionPoolMonitor monitor;
  
  final byte[] tsdb_table;
  final byte[] tsdb_uid_table;
  final int lock_timeout = 5000;
  
  //------------------------ //
  // Client usage statistics. //
  // ------------------------ //
  
  /** Number of calls to {@link #flush}.  */
  private final AtomicLong num_flushes = new AtomicLong();
  
  /** Number of calls to {@link #get}.  */
  private final AtomicLong num_gets = new AtomicLong();
  
  /** Number of calls to {@link #openScanner}.  */
  private final AtomicLong num_scanners_opened = new AtomicLong();
  
  /** Number of calls to {@link #scanNextRows}.  */
  private final AtomicLong num_scans = new AtomicLong();
  
  /** Number calls to {@link #put}.  */
  private final AtomicLong num_puts = new AtomicLong();
   
  /** Number calls to {@link #lockRow}.  */
  private final AtomicLong num_row_locks = new AtomicLong();
   
  /** Number calls to {@link #delete}.  */
  private final AtomicLong num_deletes = new AtomicLong();
  
  /** Number of {@link AtomicIncrementRequest} sent.  */
  private final AtomicLong num_atomic_increments = new AtomicLong();

  public HBaseClient(final Config config) {
    this.config = config;
    if (config.getString("asynccassandra.seeds") == null || 
        config.getString("asynccassandra.seeds").isEmpty()) {
      throw new IllegalArgumentException(
          "Missing required config 'asynccassandra.seeds'");
    }
    
    ast_config = new AstyanaxConfigurationImpl()      
      .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE);
    pool = new ConnectionPoolConfigurationImpl("MyConnectionPool")
      .setPort(config.getInt("assynccassandra.port"))
      .setMaxConnsPerHost(1)
      .setSeeds(config.getString("asynccassandra.seeds"));
    monitor = new CountingConnectionPoolMonitor();
    
    tsdb_table = config.getString("tsd.storage.hbase.data_table").getBytes();
    tsdb_uid_table = config.getString("tsd.storage.hbase.uid_table").getBytes();
    
    column_family_schemas.put("t".getBytes(), TSDB_T);
    column_family_schemas.put("name".getBytes(), TSDB_UID_NAME);
    column_family_schemas.put("id".getBytes(), TSDB_UID_ID);
  }
  
  ByteMap<ColumnFamily<byte[], byte[]>> getColumnFamilySchemas() {
    return column_family_schemas;
  }
  
  public Deferred<ArrayList<KeyValue>> get(final GetRequest request) {
    num_gets.incrementAndGet();
    final Keyspace keyspace = getContext(request.table);
    if (request.family() == null) {
      throw new UnsupportedOperationException(
          "Can't scan cassandra without a column family: " + request);
    }
    
    final Deferred<ArrayList<KeyValue>> deferred = 
        new Deferred<ArrayList<KeyValue>>();
    
    class ResponseCB implements Runnable {
      final ListenableFuture<OperationResult<ColumnList<byte[]>>> future;
      public ResponseCB(final ListenableFuture<OperationResult<ColumnList<byte[]>>> future2) {
        this.future = future2;
      }
      @Override
      public void run() {
        try {
          // TODO - can track stats here
          final ColumnList<byte[]> columns = future.get().getResult();
          final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(columns.size());
          final Iterator<Column<byte[]>> it = columns.iterator();
          while (it.hasNext()) {
            final Column<byte[]> column = it.next();
            final KeyValue kv = new KeyValue(request.key, request.family(), 
                column.getName(), column.getTimestamp() / 1000, // micro to ms 
                column.getByteArrayValue());
            kvs.add(kv);
          }
          deferred.callback(kvs);
        } catch (InterruptedException e) {
          deferred.callback(e);
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          deferred.callback(e);
        }
      }
    }
    
    // Sucks, have to have a family I guess
    try {
      final ListenableFuture<OperationResult<ColumnList<byte[]>>> future; 
      final RowQuery<byte[], byte[]> query = keyspace.prepareQuery(
          column_family_schemas.get(request.family()))
          .getKey(request.key);
      if (request.qualifiers() == null || request.qualifiers().length < 1) {
        future = query.executeAsync();
      } else {
        future = query.withColumnSlice(
            Arrays.asList(request.qualifiers())).executeAsync();
      }      
      future.addListener(new ResponseCB(future), executor);
    } catch (ConnectionException e) {
      deferred.callback(e);
    }
    
    return deferred;
  }
  
  public Deferred<Object> put(final PutRequest request) {
    num_puts.incrementAndGet();
    // TODO how do we batch?
    final Keyspace keyspace = getContext(request.table);
    final Deferred<Object> deferred = new Deferred<Object>();
    final MutationBatch mutation = keyspace.prepareMutationBatch();
    
    // TODO - all quals and values 
    mutation.withRow(column_family_schemas.get(request.family), request.key)
      .putColumn(request.qualifier(), request.value());
    try {
      final ListenableFuture<OperationResult<Void>> future = mutation.executeAsync();
      
      class ResponseCB implements Runnable {
        @Override
        public void run() {
          try {
            future.get().getResult();
            deferred.callback(null);
          } catch (InterruptedException e) {
            deferred.callback(e);
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            deferred.callback(e);
          }
        }
      }
      future.addListener(new ResponseCB(), executor);
    } catch (ConnectionException e) {
      deferred.callback(e);
    }

    return deferred;
  }
  
  public Deferred<Object> append(final AppendRequest request) {
    return Deferred.fromError(
        new UnsupportedOperationException("Not implemented yet"));
  }
  
  public Deferred<Object> delete(final DeleteRequest request) {
    num_deletes.incrementAndGet();
    // TODO how do we batch?
    final Keyspace keyspace = getContext(request.table);
    final Deferred<Object> deferred = new Deferred<Object>();
    final MutationBatch mutation = keyspace.prepareMutationBatch();
    
    // TODO - all quals
    final ColumnListMutation<byte[]> clm = mutation
        .withRow(column_family_schemas.get(request.family), request.key);
    for (final byte[] qualifier : request.qualifiers()) {
      clm.deleteColumn(qualifier);
    }
    try {
      final ListenableFuture<OperationResult<Void>> future = mutation.executeAsync();
      
      class ResponseCB implements Runnable {
        @Override
        public void run() {
          try {
            future.get().getResult();
            deferred.callback(null);
          } catch (InterruptedException e) {
            deferred.callback(e);
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            deferred.callback(e);
          }
        }
      }
      future.addListener(new ResponseCB(), executor);
    } catch (ConnectionException e) {
      deferred.callback(e);
    }

    return deferred;
  }
  
  public Deferred<Boolean> compareAndSet(final PutRequest edit,
      final byte[] expected) {
    
    if (Bytes.memcmp(tsdb_uid_table, edit.table) != 0) {
      return Deferred.fromError(new UnsupportedOperationException(
          "Increments are not supported on other tables yet"));
    }
    
    final Keyspace keyspace = getContext(edit.table);
    final ColumnFamily<byte[], String> cf = 
        Bytes.memcmp("id".getBytes(), edit.family) == 0 ? 
            TSDB_UID_ID_CAS : TSDB_UID_NAME_CAS;
    ColumnPrefixDistributedRowLock<byte[]> lock = 
        new ColumnPrefixDistributedRowLock<byte[]>(keyspace, cf,
            edit.key)
            .withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
            .expireLockAfter(lock_timeout, TimeUnit.MILLISECONDS);
    try {
      num_row_locks.incrementAndGet();
      final ColumnMap<String> columns = lock.acquireLockAndReadRow();
      final String qualifier = new String(edit.qualifier());
      final MutationBatch mutation = keyspace.prepareMutationBatch();
      mutation.withRow(cf, edit.key)
        .putColumn(qualifier, edit.value(), null);
      
      if (columns.get(qualifier) == null && (expected == null || expected.length < 1)) {
        lock.releaseWithMutation(mutation);
        return Deferred.fromResult(true);
      } else if (expected != null && columns.get(qualifier) != null &&
          Bytes.memcmpMaybeNull(columns.get(qualifier).getByteArrayValue(), 
              expected) == 0) {
        lock.releaseWithMutation(mutation);
        return Deferred.fromResult(true);
      }
      
      try {
        lock.release();
      } catch (Exception e) {
        LOG.error("Error releasing lock post exception for request: " + edit, e);
      }
      return Deferred.fromResult(false);
    } catch (Exception e) {
      try {
        lock.release();
      } catch (Exception e1) {
        LOG.error("Error releasing lock post exception for request: " + edit, e1);
      }
      return Deferred.fromError(e);
    }
  }
  
  // TODO - async me!
  public Deferred<Long> atomicIncrement(final AtomicIncrementRequest request) {
    num_atomic_increments.incrementAndGet();
    if (Bytes.memcmp(tsdb_uid_table, request.table) != 0) {
      return Deferred.fromError(new UnsupportedOperationException(
          "Increments are not supported on other tables yet"));
    }
    
    final Keyspace keyspace = getContext(request.table);
    ColumnPrefixDistributedRowLock<byte[]> lock = 
        new ColumnPrefixDistributedRowLock<byte[]>(keyspace, 
            TSDB_UID_ID_CAS, request.key)
            .withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
            .expireLockAfter(lock_timeout, TimeUnit.MILLISECONDS);
    try {
      num_row_locks.incrementAndGet();
      final ColumnMap<String> columns = lock.acquireLockAndReadRow();
              
      // Modify a value and add it to a batch mutation
      final String qualifier = new String(request.qualifier());
      long value = 1;
      if (columns.get(qualifier) != null) {
        value = columns.get(qualifier).getLongValue() + 1;
      }
      final MutationBatch mutation = keyspace.prepareMutationBatch();
      mutation.withRow(TSDB_UID_ID_CAS, request.key)
        .putColumn(qualifier, value, null);
      lock.releaseWithMutation(mutation);
      return Deferred.fromResult(value);
    } catch (Exception e) {
      try {
        lock.release();
      } catch (Exception e1) {
        LOG.error("Error releasing lock post exception for request: " + request, e1);
      }
      
      return Deferred.fromError(e);
    }
  }
  
  // TODO - buffer!
  public Deferred<Long> bufferAtomicIncrement(final AtomicIncrementRequest request) {
    return atomicIncrement(request);
  }
  
  public Deferred<Object> ensureTableExists(final byte[] table) {
    return ensureTableFamilyExists(table, EMPTY_ARRAY);
  }
  
  public Deferred<Object> ensureTableFamilyExists(final byte[] table,
      final byte[] family) {
    // Just "fault in" the first region of the table.  Not the most optimal or
    // useful thing to do but gets the job done for now.  TODO(tsuna): Improve.
    final GetRequest dummy;
    if (family == EMPTY_ARRAY) {
      // figure it out from the table name
      if (Bytes.memcmp(tsdb_table, table) == 0) {
        dummy = GetRequest.exists(table, probeKey(ZERO_ARRAY), 
            TSDB_T.getName().getBytes());
      } else if (Bytes.memcmp(tsdb_uid_table, table) == 0) {
        dummy = GetRequest.exists(table, probeKey(ZERO_ARRAY), 
            TSDB_UID_ID.getName().getBytes());
      } else {
       throw new IllegalArgumentException("Unrecognized table " + Bytes.pretty(table)); 
      }
    } else {
      dummy = GetRequest.exists(table, probeKey(ZERO_ARRAY), family);
    }
    
    class CB implements Callback<Deferred<Object>, ArrayList<KeyValue>> {

      @Override
      public Deferred<Object> call(ArrayList<KeyValue> arg0) throws Exception {
        return Deferred.fromResult(null);
      }
      
    }
    return get(dummy).addCallbackDeferring(new CB());
  }
  
  public Deferred<Object> ensureTableExists(final String table) {
    return ensureTableFamilyExists(table.getBytes(), EMPTY_ARRAY);
  }
  
  /**
   * UNUSED at this time. Eventually we may store BigTable stats in these
   * objects. It is here for backwards compatability with AsyncHBase.
   * @return An empty list.
   */
  public List<RegionClientStats> regionStats() {
    return Collections.emptyList();
  }
  
  public Scanner newScanner(final byte[] table) {
    num_scanners_opened.incrementAndGet();
    return new Scanner(this, executor, table, getContext(table));
  }
  
  public Scanner newScanner(final String table) {
    return newScanner(table.getBytes());
  }
  
  public short setFlushInterval(final short flush_interval) {
    // Note: if we have buffered increments, they'll pick up the new flush
    // interval next time the current timer fires.
    if (flush_interval < 0) {
      throw new IllegalArgumentException("Negative: " + flush_interval);
    }
    final short prev = config.flushInterval();
    config.overrideConfig("asynchbase.rpcs.buffered_flush_interval", 
        Short.toString(flush_interval));
    return prev;
  }
  
  public short getFlushInterval() {
    return config.flushInterval();
  }
  
  public Deferred<Object> shutdown() {
    try {
      // TODO - flag to prevent rpcs while shutting down
      for (final AstyanaxContext<Keyspace> context : contexts.values()) {
        context.shutdown();
      }
      executor.shutdown();
    } catch (Exception e) {
      LOG.error("failed to close the contexts", e);
    }
    return Deferred.fromResult(null);
  }
  
  public ClientStats stats() {
    return new ClientStats(0, 0, 0, 0, 
        num_flushes.get(), 
        0, 0, 0, 
        num_gets.get(), 
        num_scanners_opened.get(), 
        num_scans.get(), 
        num_puts.get(), 
        0, 
        num_row_locks.get(), 
        num_deletes.get(), 
        num_atomic_increments.get(), 
        null);
  }
  
  public Deferred<Object> flush() {
    num_flushes.incrementAndGet();
    return Deferred.fromResult(null);
  }

  public void overRideSocketTimeout(final int timeout) {
    // dunna care
  }
  
  public void logNSREBuffer(final boolean val) {
    // dunna care
  }
  
  public void logInflightBuffer(final boolean val) {
    // dunna care
  }
  
  public ByteMap<Integer> getNsreCounts() {
    return new ByteMap<Integer>();
  }
  
  /**
   * A no-op for BigTable clients
   * @param table Ignored
   * @return A deferred with a null result immediately.
   * @deprecated
   */
  public Deferred<Object> prefetchMeta(final String table) {
    return Deferred.fromResult(null);
  }
  
  /**
   * A no-op for BigTable clients
   * @param table Ignored
   * @param start Ignored
   * @param stop Ignored
   * @return A deferred with a null result immediately.
   * @deprecated
   */
  public Deferred<Object> prefetchMeta(final String table,
      final String start,
      final String stop) {
    return Deferred.fromResult(null);
  }
  
  /**
   * A no-op for BigTable clients
   * @param table Ignored
   * @return A deferred with a null result immediately.
   * @deprecated
   */
  public Deferred<Object> prefetchMeta(final byte[] table) {
    return Deferred.fromResult(null);
  }
  
  /**
   * A no-op for BigTable clients
   * @param table Ignored
   * @param start Ignored
   * @param stop Ignored
   * @return A deferred with a null result immediately.
   * @deprecated
   */
  public Deferred<Object> prefetchMeta(final byte[] table,
      final byte[] start,
      final byte[] stop) {
    return Deferred.fromResult(null);
  }
  
  
  private Keyspace getContext(final byte[] table) {
    Keyspace keyspace = keyspaces.get(table);
    if (keyspace == null) {
      synchronized (keyspaces) {
        // avoid race conditions where another thread put the client
        keyspace = keyspaces.get(table);
        AstyanaxContext<Keyspace> context = contexts.get(table);
        if (context != null) {
          LOG.warn("Context wasn't null for new keyspace " + Bytes.pretty(table));
        }
        context = new AstyanaxContext.Builder()
          .forCluster("localhost")
          .forKeyspace(new String(table))
          .withAstyanaxConfiguration(ast_config)
          .withConnectionPoolConfiguration(pool)
          .withConnectionPoolMonitor(monitor)
          .buildKeyspace(ThriftFamilyFactory.getInstance());
        contexts.put(table, context);
        context.start();
        
        keyspace = context.getClient();
        keyspaces.put(table, keyspace);
      }
    }
    return keyspace;
  }

  /**
   * Some arbitrary junk that is unlikely to appear in a real row key.
   * @see probeKey
   */
  static byte[] PROBE_SUFFIX = {
    ':', 'A', 's', 'y', 'n', 'c', 'H', 'B', 'a', 's', 'e',
    '~', 'p', 'r', 'o', 'b', 'e', '~', '<', ';', '_', '<',
  };
  
  /**
   * Returns a newly allocated key to probe, to check a region is online.
   * Sometimes we need to "poke" HBase to see if a region is online or a table
   * exists.  Given a key, we prepend some unique suffix to make it a lot less
   * likely that we hit a real key with our probe, as doing so might have some
   * implications on the RegionServer's memory usage.  Yes, some people with
   * very large keys were experiencing OOM's in their RegionServers due to
   * AsyncHBase probes.
   */
  private static byte[] probeKey(final byte[] key) {
    final byte[] testKey = new byte[key.length + 64];
    System.arraycopy(key, 0, testKey, 0, key.length);
    System.arraycopy(PROBE_SUFFIX, 0,
                     testKey, testKey.length - PROBE_SUFFIX.length,
                     PROBE_SUFFIX.length);
    return testKey;
  }
}
