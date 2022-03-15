// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Each compaction will create a new {@link AbstractCompactionFilter}
 * allowing the application to know about different compactions
 *
 */
public abstract class AbstractValueExtractorFactory
    extends RocksObject {

  public AbstractValueExtractorFactory(final long nativeHandle) {
    super(nativeHandle);
  }


  /**
   * A name which identifies this compaction filter
   *
   * The name will be printed to the LOG file on start up for diagnosis
   */
  public abstract String name();
  
  // private native void disposeInternal(final long handle);
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }
}
