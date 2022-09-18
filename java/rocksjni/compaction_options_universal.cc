// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// TERARKDB_NAMESPACE::CompactionOptionsUniversal.

#include <jni.h>

#include "include/org_terarkdb_CompactionOptionsUniversal.h"
#include "rocksdb/advanced_options.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    newCompactionOptionsUniversal
 * Signature: ()J
 */
jlong Java_org_terarkdb_CompactionOptionsUniversal_newCompactionOptionsUniversal(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  const auto* opt = new TERARKDB_NAMESPACE::CompactionOptionsUniversal();
  return reinterpret_cast<jlong>(opt);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    setSizeRatio
 * Signature: (JI)V
 */
void Java_org_terarkdb_CompactionOptionsUniversal_setSizeRatio(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jint jsize_ratio) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->size_ratio = static_cast<unsigned int>(jsize_ratio);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    sizeRatio
 * Signature: (J)I
 */
jint Java_org_terarkdb_CompactionOptionsUniversal_sizeRatio(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->size_ratio);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    setMinMergeWidth
 * Signature: (JI)V
 */
void Java_org_terarkdb_CompactionOptionsUniversal_setMinMergeWidth(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jint jmin_merge_width) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->min_merge_width = static_cast<unsigned int>(jmin_merge_width);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    minMergeWidth
 * Signature: (J)I
 */
jint Java_org_terarkdb_CompactionOptionsUniversal_minMergeWidth(JNIEnv* /*env*/,
                                                               jobject /*jobj*/,
                                                               jlong jhandle) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->min_merge_width);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    setMaxMergeWidth
 * Signature: (JI)V
 */
void Java_org_terarkdb_CompactionOptionsUniversal_setMaxMergeWidth(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jint jmax_merge_width) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->max_merge_width = static_cast<unsigned int>(jmax_merge_width);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    maxMergeWidth
 * Signature: (J)I
 */
jint Java_org_terarkdb_CompactionOptionsUniversal_maxMergeWidth(JNIEnv* /*env*/,
                                                               jobject /*jobj*/,
                                                               jlong jhandle) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->max_merge_width);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    setMaxSizeAmplificationPercent
 * Signature: (JI)V
 */
void Java_org_terarkdb_CompactionOptionsUniversal_setMaxSizeAmplificationPercent(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jint jmax_size_amplification_percent) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->max_size_amplification_percent =
      static_cast<unsigned int>(jmax_size_amplification_percent);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    maxSizeAmplificationPercent
 * Signature: (J)I
 */
jint Java_org_terarkdb_CompactionOptionsUniversal_maxSizeAmplificationPercent(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->max_size_amplification_percent);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    setCompressionSizePercent
 * Signature: (JI)V
 */
void Java_org_terarkdb_CompactionOptionsUniversal_setCompressionSizePercent(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jint jcompression_size_percent) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->compression_size_percent =
      static_cast<unsigned int>(jcompression_size_percent);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    compressionSizePercent
 * Signature: (J)I
 */
jint Java_org_terarkdb_CompactionOptionsUniversal_compressionSizePercent(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->compression_size_percent);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    setStopStyle
 * Signature: (JB)V
 */
void Java_org_terarkdb_CompactionOptionsUniversal_setStopStyle(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jbyte jstop_style_value) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->stop_style = TERARKDB_NAMESPACE::CompactionStopStyleJni::toCppCompactionStopStyle(
      jstop_style_value);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    stopStyle
 * Signature: (J)B
 */
jbyte Java_org_terarkdb_CompactionOptionsUniversal_stopStyle(JNIEnv* /*env*/,
                                                            jobject /*jobj*/,
                                                            jlong jhandle) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return TERARKDB_NAMESPACE::CompactionStopStyleJni::toJavaCompactionStopStyle(
      opt->stop_style);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    setAllowTrivialMove
 * Signature: (JZ)V
 */
void Java_org_terarkdb_CompactionOptionsUniversal_setAllowTrivialMove(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jboolean jallow_trivial_move) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->allow_trivial_move = static_cast<bool>(jallow_trivial_move);
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    allowTrivialMove
 * Signature: (J)Z
 */
jboolean Java_org_terarkdb_CompactionOptionsUniversal_allowTrivialMove(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* opt = reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return opt->allow_trivial_move;
}

/*
 * Class:     org_terarkdb_CompactionOptionsUniversal
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_terarkdb_CompactionOptionsUniversal_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  delete reinterpret_cast<TERARKDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
}
