// Copyright (c) 2020-present, Bytedance Inc.  All rights reserved.
// This source code is licensed under Apache 2.0 License.

#include "terark_zip_memtable.h"

#include <inttypes.h>

#include <atomic>
#include <chrono>
#include <memory>

#include "db/dbformat.h"
#include "gtest/gtest.h"

namespace rocksdb {

class TerarkZipMemtableTest : public testing::Test {};

TEST_F(TerarkZipMemtableTest, SimpleTest) {
  std::shared_ptr<MemTable> mem_;
  Options options;
  options.memtable_factory =
      std::shared_ptr<MemTableRepFactory>(NewPatriciaTrieRepFactory());

  InternalKeyComparator cmp(BytewiseComparator());
  ImmutableCFOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);

  mem_ = std::shared_ptr<MemTable>(
      new MemTable(cmp, ioptions, MutableCFOptions(options),
                   /* needs_dup_key_check */ true, &wb, kMaxSequenceNumber,
                   0 /* column_family_id */));

  // Run some basic tests
  SequenceNumber seq = 123;
  bool res;
  res = mem_->Add(seq, kTypeValue, "key", "value2");
  ASSERT_TRUE(res);
  res = mem_->Add(seq, kTypeValue, "key", "value2");
  ASSERT_FALSE(res);
  // Changing the type should still cause the duplicatae key
  res = mem_->Add(seq, kTypeMerge, "key", "value2");
  ASSERT_FALSE(res);
  // Changing the seq number will make the key fresh
  res = mem_->Add(seq + 1, kTypeMerge, "key", "value2");
  ASSERT_TRUE(res);
  // Test with different types for duplicate keys
  res = mem_->Add(seq, kTypeDeletion, "key", "");
  ASSERT_FALSE(res);
  res = mem_->Add(seq, kTypeSingleDeletion, "key", "");
  ASSERT_FALSE(res);
}

// Test multi-threading insertion
TEST_F(TerarkZipMemtableTest, MultiThreadingTest) {
  MemTable* mem_;
  Options options;
  options.memtable_factory =
      std::shared_ptr<MemTableRepFactory>(NewPatriciaTrieRepFactory());

  InternalKeyComparator cmp(BytewiseComparator());
  ImmutableCFOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);

  mem_ = new MemTable(cmp, ioptions, MutableCFOptions(options),
                      /* needs_dup_key_check */ true, &wb, kMaxSequenceNumber,
                      0 /* column_family_id */);

  size_t records = 1 << 20;  // 1M records
  SequenceNumber seq = 0;
  // Single Thread QPS
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < records; ++i) {
    Slice key("key " + std::to_string(i));
    Slice value("value " + std::to_string(i));
    auto ret = mem_->Add(seq, kTypeValue, key, value);
    seq++;
  }
  auto end = std::chrono::system_clock::now();
  auto dur =
      std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

  printf("Single-Thread Time Cost: %" PRId64 ", mem_->size = %" PRId64 "\n",
         dur, mem_->num_entries());

  delete mem_;

  // Multi Thread QPS
  mem_ = new MemTable(cmp, ioptions, MutableCFOptions(options),
                      /* needs_dup_key_check */ true, &wb, kMaxSequenceNumber,
                      0 /* column_family_id */);

  std::vector<std::thread> threads;
  int thread_cnt = 2;
  std::atomic<SequenceNumber> atomic_seq{0};
  start = std::chrono::system_clock::now();

  for (int t = 0; t < thread_cnt; ++t) {
    threads.emplace_back(std::thread([&, t]() {
      int start = (records / thread_cnt) * t;
      int end = (records / thread_cnt) * (t + 1);
      printf("thread kv range: [%d, %d)\n", start, end);
      for (size_t i = start; i < end; ++i) {
        Slice key("key " + std::to_string(i));
        Slice value("value " + std::to_string(i));
        auto ret = mem_->Add(atomic_seq, kTypeValue, key, value);
        atomic_seq++;
      }
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  end = std::chrono::system_clock::now();
  dur = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

  printf("Multi-Thread Time Cost: %" PRId64 ", mem_->size = %" PRId64 "\n", dur,
         mem_->num_entries());
  delete mem_;
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
