// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for TERARKDB_NAMESPACE::OptimisticTransactionOptions.

#include <jni.h>

#include "include/org_terarkdb_OptimisticTransactionOptions.h"

#include "rocksdb/comparator.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"

/*
 * Class:     org_terarkdb_OptimisticTransactionOptions
 * Method:    newOptimisticTransactionOptions
 * Signature: ()J
 */
jlong Java_org_terarkdb_OptimisticTransactionOptions_newOptimisticTransactionOptions(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  TERARKDB_NAMESPACE::OptimisticTransactionOptions* opts =
      new TERARKDB_NAMESPACE::OptimisticTransactionOptions();
  return reinterpret_cast<jlong>(opts);
}

/*
 * Class:     org_terarkdb_OptimisticTransactionOptions
 * Method:    isSetSnapshot
 * Signature: (J)Z
 */
jboolean Java_org_terarkdb_OptimisticTransactionOptions_isSetSnapshot(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* opts =
      reinterpret_cast<TERARKDB_NAMESPACE::OptimisticTransactionOptions*>(jhandle);
  return opts->set_snapshot;
}

/*
 * Class:     org_terarkdb_OptimisticTransactionOptions
 * Method:    setSetSnapshot
 * Signature: (JZ)V
 */
void Java_org_terarkdb_OptimisticTransactionOptions_setSetSnapshot(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jboolean jset_snapshot) {
  auto* opts =
      reinterpret_cast<TERARKDB_NAMESPACE::OptimisticTransactionOptions*>(jhandle);
  opts->set_snapshot = jset_snapshot;
}

/*
 * Class:     org_terarkdb_OptimisticTransactionOptions
 * Method:    setComparator
 * Signature: (JJ)V
 */
void Java_org_terarkdb_OptimisticTransactionOptions_setComparator(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jcomparator_handle) {
  auto* opts =
      reinterpret_cast<TERARKDB_NAMESPACE::OptimisticTransactionOptions*>(jhandle);
  opts->cmp = reinterpret_cast<TERARKDB_NAMESPACE::Comparator*>(jcomparator_handle);
}

/*
 * Class:     org_terarkdb_OptimisticTransactionOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_terarkdb_OptimisticTransactionOptions_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  delete reinterpret_cast<TERARKDB_NAMESPACE::OptimisticTransactionOptions*>(jhandle);
}
