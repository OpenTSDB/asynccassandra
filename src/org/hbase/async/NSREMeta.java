/*
 * Copyright (C) 2011-2015  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import java.util.Arrays;

import com.google.common.base.Objects;

/**
 * Utility class to store information about an NSRE thrown by HBase.
 * @since 1.6
 */
final class NSREMeta {
  // UNIX timestamp in msec when NSRE first detected.
  private final long time;

  // HBase region server that owned region.
  private final String remote_address;

  // HBase key at which region started.
  private final byte[] start_key;

  // HBase key at which region ended.
  private final byte[] stop_key;

  /**
   * Create a new instance of the NSREMeta class.
   * @param time The time, in milliseconds, when the NSRE was first detected.
   * @param remote_address The hostname of the server on which the region lived.
   * @param start_key Where the region started.
   * @param stop_key Where the region ended.
   * @throws IllegalArgumentException if time negative
   * @throws IllegalArgumentException if remote_address null or empty
   * @throws IllegalArgumentException if start_key null
   * @throws IllegalArgumentException if stop_key null
   */
  NSREMeta(final long time, final String remote_address, final byte[] start_key,
      final byte[] stop_key) {
    // No one in this world can you trust. Not men, not women, not beasts...
    // Not even other AsyncHBase programmers.
    if (time < 0L) {
      throw new IllegalArgumentException("timestamp can't be negative");
    }
    if (null == remote_address || remote_address.isEmpty()) {
      throw new IllegalArgumentException("remote address must not be null or" +
        " empty");
    }
    if (null == start_key) {
      throw new IllegalArgumentException("start key can't be null");
    }
    if (null == stop_key) {
      throw new IllegalArgumentException("stop key can't be null");
    }

    this.time = time;
    this.remote_address = remote_address;
    this.start_key = Arrays.copyOf(start_key, start_key.length); // This you can trust.
    this.stop_key = Arrays.copyOf(stop_key, stop_key.length);
  }

  /** @return the time in milliseconds at which the NSRE was first detected. */
  long getTime() {
    return time;
  }

  /** @return the hostname of the region server that owned the region. */
  String getRemoteAddress() {
    return remote_address;
  }

  /** @return the HBase key at which the region started. */
  byte[] getStartKey() {
    return start_key;
  }

  /** @return the HBase key at which the region ended. */
  byte[] getStopKey() {
    return stop_key;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getTime(), getRemoteAddress(),
      Arrays.hashCode(getStartKey()), Arrays.hashCode(getStopKey()));
  }

  @Override
  public boolean equals(final Object other) {
    if (null == other) {
      return false;
    }
    if (this == other) {
      return true;
    }
    if (getClass() != other.getClass()) {
      return false;
    }

    final NSREMeta meta = (NSREMeta)other;
    return getTime() == meta.getTime() &&
           getRemoteAddress().equals(meta.getRemoteAddress()) &&
           Arrays.equals(getStartKey(), meta.getStartKey()) &&
           Arrays.equals(getStopKey(), meta.getStopKey());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("time", getTime())
      .add("remoteAddress", getRemoteAddress())
      .add("startKey", Arrays.toString(getStartKey()))
      .add("stopKey", Arrays.toString(getStopKey()))
      .toString();
  }
}

