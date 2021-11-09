#include <functional>

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/terark_namespace.h"
#include "util/testutil.h"

namespace TERARKDB_NAMESPACE {

#ifndef ROCKSDB_LITE
class ExternalSSTFileBasicTest : public DBTestBase,
                                 public ::testing::WithParamInterface<bool> {
 public:
  ExternalSSTFileBasicTest() : DBTestBase("/external_sst_file_test") {
    sst_files_dir_ = dbname_ + "/sst_files/";
    DestroyAndRecreateExternalSSTFilesDir();
  }

  void DestroyAndRecreateExternalSSTFilesDir() {
    test::DestroyDir(env_, sst_files_dir_);
    env_->CreateDir(sst_files_dir_);
  }

  Status DeprecatedAddFile(const std::vector<std::string>& files,
                           bool move_files = false,
                           bool skip_snapshot_check = false,
                           bool quick_ingest = false) {
    IngestExternalFileOptions opts;
    opts.move_files = move_files;
    opts.quick_ingest = quick_ingest;
    opts.snapshot_consistency = !skip_snapshot_check;
    opts.allow_global_seqno = false;
    opts.allow_blocking_flush = false;
    return db_->IngestExternalFile(files, opts);
  }
  ~ExternalSSTFileBasicTest() { test::DestroyDir(env_, sst_files_dir_); }

 protected:
  std::string sst_files_dir_;
};

TEST_F(ExternalSSTFileBasicTest, FlushConflict) {
  bool flush_conflict = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFile:ProcessFlush",
      [&](void* arg) { flush_conflict = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options = CurrentOptions();

  SstFileWriter sst_file_writer(EnvOptions(), options);

  // Current file size should be 0 after sst_file_writer init and before open a
  // file.
  ASSERT_EQ(sst_file_writer.FileSize(), 0);

  // file1.sst (0 => 99)
  std::string file1 = sst_files_dir_ + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(sst_file_writer.Put(Key(k), Key(k) + "_val"));
  }
  ExternalSstFileInfo file1_info;
  Status s = sst_file_writer.Finish(&file1_info);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Current file size should be non-zero after success write.
  ASSERT_GT(sst_file_writer.FileSize(), 0);

  ASSERT_EQ(file1_info.file_path, file1);
  ASSERT_EQ(file1_info.num_entries, 100);
  ASSERT_EQ(file1_info.smallest_key, Key(0));
  ASSERT_EQ(file1_info.largest_key, Key(99));
  ASSERT_EQ(file1_info.num_range_del_entries, 0);
  ASSERT_EQ(file1_info.smallest_range_del_key, "");
  ASSERT_EQ(file1_info.largest_range_del_key, "");
  // sst_file_writer already finished, cannot add this value
  s = sst_file_writer.Put(Key(100), "bad_val");
  ASSERT_FALSE(s.ok()) << s.ToString();
  s = sst_file_writer.DeleteRange(Key(100), Key(200));
  ASSERT_FALSE(s.ok()) << s.ToString();

  DestroyAndReopen(options);
  // Add file using file path
  s = DeprecatedAddFile({file1});
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);
  for (int k = 0; k < 100; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }
  for (int k = 0; k < 100; k++) {
    ASSERT_OK(db_->Delete(WriteOptions(), Key(k)));
  }

  s = DeprecatedAddFile({file1}, false, false, true);

  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(flush_conflict);

  for (int k = 0; k < 100; k++) {
    ASSERT_EQ(Get(Key(k)), Key(k) + "_val");
  }
  DestroyAndRecreateExternalSSTFilesDir();
}

#endif
}  // namespace TERARKDB_NAMESPACE
int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}