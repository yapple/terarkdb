// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "include/org_terarkdb_MemoryUtil.h"

#include "rocksjni/portal.h"

#include "rocksdb/utilities/memory_util.h"


/*
 * Class:     org_terarkdb_MemoryUtil
 * Method:    getApproximateMemoryUsageByType
 * Signature: ([J[J)Ljava/util/Map;
 */
jobject Java_org_terarkdb_MemoryUtil_getApproximateMemoryUsageByType(
    JNIEnv *env, jclass /*jclazz*/, jlongArray jdb_handles, jlongArray jcache_handles) {

  std::vector<TERARKDB_NAMESPACE::DB*> dbs;
  jsize db_handle_count = env->GetArrayLength(jdb_handles);
  if(db_handle_count > 0) {
    jlong *ptr_jdb_handles = env->GetLongArrayElements(jdb_handles, nullptr);
    if (ptr_jdb_handles == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    for (jsize i = 0; i < db_handle_count; i++) {
      dbs.push_back(reinterpret_cast<TERARKDB_NAMESPACE::DB *>(ptr_jdb_handles[i]));
    }
    env->ReleaseLongArrayElements(jdb_handles, ptr_jdb_handles, JNI_ABORT);
  }

  std::unordered_set<const TERARKDB_NAMESPACE::Cache*> cache_set;
  jsize cache_handle_count = env->GetArrayLength(jcache_handles);
  if(cache_handle_count > 0) {
    jlong *ptr_jcache_handles = env->GetLongArrayElements(jcache_handles, nullptr);
    if (ptr_jcache_handles == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    for (jsize i = 0; i < cache_handle_count; i++) {
      auto *cache_ptr =
          reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Cache> *>(ptr_jcache_handles[i]);
      cache_set.insert(cache_ptr->get());
    }
    env->ReleaseLongArrayElements(jcache_handles, ptr_jcache_handles, JNI_ABORT);
  }

  std::map<TERARKDB_NAMESPACE::MemoryUtil::UsageType, uint64_t> usage_by_type;
  if(TERARKDB_NAMESPACE::MemoryUtil::GetApproximateMemoryUsageByType(dbs, cache_set, &usage_by_type) != TERARKDB_NAMESPACE::Status::OK()) {
    // Non-OK status
    return nullptr;
  }

  jobject jusage_by_type = TERARKDB_NAMESPACE::HashMapJni::construct(
      env, static_cast<uint32_t>(usage_by_type.size()));
  if (jusage_by_type == nullptr) {
    // exception occurred
    return nullptr;
  }
  const TERARKDB_NAMESPACE::HashMapJni::FnMapKV<const TERARKDB_NAMESPACE::MemoryUtil::UsageType, const uint64_t>
      fn_map_kv =
      [env](const std::pair<TERARKDB_NAMESPACE::MemoryUtil::UsageType, uint64_t>& pair) {
        // Construct key
        const jobject jusage_type =
            TERARKDB_NAMESPACE::ByteJni::valueOf(env, TERARKDB_NAMESPACE::MemoryUsageTypeJni::toJavaMemoryUsageType(pair.first));
        if (jusage_type == nullptr) {
          // an error occurred
          return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
        }
        // Construct value
        const jobject jusage_value =
            TERARKDB_NAMESPACE::LongJni::valueOf(env, pair.second);
        if (jusage_value == nullptr) {
          // an error occurred
          return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
        }
        // Construct and return pointer to pair of jobjects
        return std::unique_ptr<std::pair<jobject, jobject>>(
            new std::pair<jobject, jobject>(jusage_type,
                                            jusage_value));
      };

  if (!TERARKDB_NAMESPACE::HashMapJni::putAll(env, jusage_by_type, usage_by_type.begin(),
                                   usage_by_type.end(), fn_map_kv)) {
    // exception occcurred
    jusage_by_type = nullptr;
  }

  return jusage_by_type;

}
