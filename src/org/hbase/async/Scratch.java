package org.hbase.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class Scratch {
  
  static byte[] TABLE = "tsdb".getBytes();
  static byte[] CF = "t".getBytes();

  static public void main(String[] args) {
    final Config conf = new Config();
    conf.overrideConfig("tsd.storage.hbase.data_table", "tsdb");
    conf.overrideConfig("tsd.storage.hbase.uid_table", "tsdbuid");
    final HBaseClient client = new HBaseClient(conf);
    
    try {
      //put(client);
      //get(client);
      //exists(client);
      //increment(client);
      //cas(client);
      scanner(client);
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    try {
      client.shutdown().join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("\n-----------------\nComplete");
  }
  
  static void put(final HBaseClient client) throws InterruptedException, Exception {
    final PutRequest put = new PutRequest(TABLE,
        new byte[] { 0, 0, 1 }, CF,
        "woot".getBytes(), "diffval".getBytes());
    client.put(put).join();
  }
  
  static void get(final HBaseClient client) throws InterruptedException, Exception {
    final GetRequest get = new GetRequest(TABLE, new byte[] { 0, 0, 1 }, CF, "woot".getBytes());
    ArrayList<KeyValue> row = client.get(get).join();
    System.out.println("Got: " + row);
  }
  
  static void exists(final HBaseClient client) throws InterruptedException, Exception {
    System.out.println("Exists: " + client.ensureTableExists(TABLE).join());
  }
  
  static void increment(final HBaseClient client) throws InterruptedException, Exception {
    final AtomicIncrementRequest air = new AtomicIncrementRequest(
        "tsdbuid".getBytes(), 
        new byte[] { 0 }, "id".getBytes(), "metrics".getBytes());
    System.out.println("ID: " + client.atomicIncrement(air).join());
  }
  
  static void cas(final HBaseClient client) throws InterruptedException, Exception {
    PutRequest put = new PutRequest("tsdbuid".getBytes(), 
        new byte[] { 1 }, "id".getBytes(), "metrics".getBytes(), new byte[] { 42 });
    
    System.out.println("First Put: " + client.compareAndSet(put, null).join());
    
    System.out.println("Second Put: " + client.compareAndSet(put, null).join());
    
    put = new PutRequest("tsdbuid".getBytes(), 
        new byte[] { 1 }, "id".getBytes(), "metrics".getBytes(), new byte[] { 24 });
    System.out.println("Third Put: " + client.compareAndSet(put, new byte[] { 42 }).join());
  }
  
  static void scanner(final HBaseClient client) throws InterruptedException, Exception {
    final Scanner scanner = client.newScanner(TABLE);
    scanner.setFamily(CF);
    scanner.setStartKey(new byte[] { 0 });
    scanner.setStopKey(new byte[] { (byte) 0xFF });
    
    ArrayList<ArrayList<KeyValue>> rows = scanner.nextRows().join();
    while(rows != null) {
      for (ArrayList<KeyValue> row : rows) {
        System.out.println("ROW: " + row);
      }
      rows = scanner.nextRows().join();
    }
    System.out.println("Done with the scan!");
  }
}
