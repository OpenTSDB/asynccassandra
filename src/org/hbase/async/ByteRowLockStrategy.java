package org.hbase.async;

import java.util.Arrays;

import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.recipes.locks.LockColumnStrategy;
import com.netflix.astyanax.util.RangeBuilder;

public class ByteRowLockStrategy implements LockColumnStrategy<byte[]> {

  public static final byte[] DEFAULT_LOCK_PREFIX = "_LOCK_".getBytes();

  private final byte[] lock_id;
  private byte[] prefix = DEFAULT_LOCK_PREFIX;
  
  public ByteRowLockStrategy(final byte[] lock_id) {
    this.lock_id = lock_id;
  }
  
  @Override
  public byte[] generateLockColumn() {
    final byte[] concatenated = new byte[prefix.length + lock_id.length];
    System.arraycopy(prefix, 0, concatenated, 0, prefix.length);
    System.arraycopy(lock_id, 0, concatenated, prefix.length, lock_id.length);
    System.out.println("Gerating lock: " + Arrays.toString(concatenated));
    return concatenated;
  }

  @Override
  public ByteBufferRange getLockColumnRange() {
    final byte[] start = new byte[prefix.length + 1];
    System.arraycopy(prefix, 0, start, 0, prefix.length); // last byte be zero!
    final byte[] end = new byte[prefix.length + 1];
    System.arraycopy(prefix, 0, start, 0, prefix.length);
    end[end.length-1] = (byte)0xFF; // max out the last byte
    
    return new RangeBuilder()
      .setStart(start)
      .setEnd(end)
      .build();
  }

  @Override
  public boolean isLockColumn(final byte[] column) {
    System.out.println("Read column: " + Arrays.toString(column));
    if (column == null || column.length < prefix.length) {
      System.out.println("Diff lengths");
      return false;
    }
    System.out.println("COMPARE: " + Bytes.memcmp(column, prefix, 0, prefix.length));
    return Bytes.memcmp(column, prefix, 0, prefix.length) == 0;
  }

  public byte[] getLockId() {
    return lock_id;
  }
  
  public byte[] getPrefix() {
    return prefix;
  }
  
  public void setPrefix(final byte[] prefix) {
    this.prefix = prefix;
  }
}
