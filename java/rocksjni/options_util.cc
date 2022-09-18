// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ TERARKDB_NAMESPACE::OptionsUtil methods from Java side.

#include <jni.h>

#include "include/org_terarkdb_OptionsUtil.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksjni/portal.h"

void build_column_family_descriptor_list(
    JNIEnv* env, jobject jcfds,
    std::vector<TERARKDB_NAMESPACE::ColumnFamilyDescriptor>& cf_descs) {
  jmethodID add_mid = TERARKDB_NAMESPACE::ListJni::getListAddMethodId(env);
  if (add_mid == nullptr) {
    // exception occurred accessing method
    return;
  }

  // Column family descriptor
  for (TERARKDB_NAMESPACE::ColumnFamilyDescriptor& cfd : cf_descs) {
    // Construct a ColumnFamilyDescriptor java object
    jobject jcfd = TERARKDB_NAMESPACE::ColumnFamilyDescriptorJni::construct(env, &cfd);
    if (env->ExceptionCheck()) {
      // exception occurred constructing object
      if (jcfd != nullptr) {
        env->DeleteLocalRef(jcfd);
      }
      return;
    }

    // Add the object to java list.
    jboolean rs = env->CallBooleanMethod(jcfds, add_mid, jcfd);
    if (env->ExceptionCheck() || rs == JNI_FALSE) {
      // exception occurred calling method, or could not add
      if (jcfd != nullptr) {
        env->DeleteLocalRef(jcfd);
      }
      return;
    }
  }
}

/*
 * Class:     org_terarkdb_OptionsUtil
 * Method:    loadLatestOptions
 * Signature: (Ljava/lang/String;JLjava/util/List;Z)V
 */
void Java_org_terarkdb_OptionsUtil_loadLatestOptions(
    JNIEnv* env, jclass /*jcls*/, jstring jdbpath, jlong jenv_handle,
    jlong jdb_opts_handle, jobject jcfds, jboolean ignore_unknown_options) {
  const char* db_path = env->GetStringUTFChars(jdbpath, nullptr);
  std::vector<TERARKDB_NAMESPACE::ColumnFamilyDescriptor> cf_descs;
  TERARKDB_NAMESPACE::Status s = TERARKDB_NAMESPACE::LoadLatestOptions(
      db_path, reinterpret_cast<TERARKDB_NAMESPACE::Env*>(jenv_handle),
      reinterpret_cast<TERARKDB_NAMESPACE::DBOptions*>(jdb_opts_handle), &cf_descs,
      ignore_unknown_options);
  env->ReleaseStringUTFChars(jdbpath, db_path);

  if (!s.ok()) {
    TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }

  build_column_family_descriptor_list(env, jcfds, cf_descs);
}

/*
 * Class:     org_terarkdb_OptionsUtil
 * Method:    loadOptionsFromFile
 * Signature: (Ljava/lang/String;JJLjava/util/List;Z)V
 */
void Java_org_terarkdb_OptionsUtil_loadOptionsFromFile(
    JNIEnv* env, jclass /*jcls*/, jstring jopts_file_name, jlong jenv_handle,
    jlong jdb_opts_handle, jobject jcfds, jboolean ignore_unknown_options) {
  const char* opts_file_name = env->GetStringUTFChars(jopts_file_name, nullptr);
  std::vector<TERARKDB_NAMESPACE::ColumnFamilyDescriptor> cf_descs;
  TERARKDB_NAMESPACE::Status s = TERARKDB_NAMESPACE::LoadOptionsFromFile(
      opts_file_name, reinterpret_cast<TERARKDB_NAMESPACE::Env*>(jenv_handle),
      reinterpret_cast<TERARKDB_NAMESPACE::DBOptions*>(jdb_opts_handle), &cf_descs,
      ignore_unknown_options);
  env->ReleaseStringUTFChars(jopts_file_name, opts_file_name);

  if (!s.ok()) {
    TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }

  build_column_family_descriptor_list(env, jcfds, cf_descs);
}

/*
 * Class:     org_terarkdb_OptionsUtil
 * Method:    getLatestOptionsFileName
 * Signature: (Ljava/lang/String;J)Ljava/lang/String;
 */
jstring Java_org_terarkdb_OptionsUtil_getLatestOptionsFileName(
    JNIEnv* env, jclass /*jcls*/, jstring jdbpath, jlong jenv_handle) {
  const char* db_path = env->GetStringUTFChars(jdbpath, nullptr);
  std::string options_file_name;
  if (db_path != nullptr) {
    TERARKDB_NAMESPACE::GetLatestOptionsFileName(
        db_path, reinterpret_cast<TERARKDB_NAMESPACE::Env*>(jenv_handle),
        &options_file_name);
  }
  env->ReleaseStringUTFChars(jdbpath, db_path);

  return env->NewStringUTF(options_file_name.c_str());
}
