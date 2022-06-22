//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdint.h>

#include <algorithm>
#include <string>

#include "db/db_impl.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/terark_namespace.h"
#include "util/file_util.h"
#include "util/filename.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"

namespace TERARKDB_NAMESPACE {

Status DBImpl::DisableFileDeletions() {
  InstrumentedMutexLock l(&mutex_);
  ++disable_delete_obsolete_files_;
  if (disable_delete_obsolete_files_ == 1) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "File Deletions Disabled");
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "File Deletions Disabled, but already disabled. Counter: %d",
                   disable_delete_obsolete_files_);
  }
  return Status::OK();
}

Status DBImpl::EnableFileDeletions(bool force) {
  // Job id == 0 means that this is not our background process, but rather
  // user thread
  JobContext job_context(0);
  bool file_deletion_enabled = false;
  {
    InstrumentedMutexLock l(&mutex_);
    if (force) {
      // if force, we need to enable file deletions right away
      disable_delete_obsolete_files_ = 0;
    } else if (disable_delete_obsolete_files_ > 0) {
      --disable_delete_obsolete_files_;
    }
    if (disable_delete_obsolete_files_ == 0) {
      file_deletion_enabled = true;
      FindObsoleteFiles(&job_context, true);
      bg_cv_.SignalAll();
    }
  }
  if (file_deletion_enabled) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "File Deletions Enabled");
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "File Deletions Enable, but not really enabled. Counter: %d",
                   disable_delete_obsolete_files_);
  }
  job_context.Clean(&mutex_);
  LogFlush(immutable_db_options_.info_log);
  return Status::OK();
}

int DBImpl::IsFileDeletionsEnabled() const {
  return !disable_delete_obsolete_files_;
}

Status DBImpl::UndoFakeFlush() {
  mutex_.Lock();
  autovector<ColumnFamilyData*> cfds;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    cfd->Ref();
    cfds.push_back(cfd);
  }
  mutex_.Unlock();
  Status status;
  for (auto cfd : cfds) {
    auto iter = version_edits_.find(cfd->GetID());
    if (iter == version_edits_.end()) continue;
    VersionEdit* edit = &iter->second;
    VersionEdit edit_del;
    for (auto f : edit->GetNewFiles()) {
      edit_del.DeleteFile(0, f.second.fd.GetNumber());
    }
    edit_del.set_check_point(true);
    mutex_.Lock();
    status = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                                    &edit_del, &mutex_);
    mutex_.Unlock();
    if (!status.ok()) {
      break;
    }
  }

  mutex_.Lock();
  for (auto cfd : cfds) {
    cfd->Unref();
  }
  mutex_.Unlock();

  return status;
}

Status DBImpl::FakeFlush(std::vector<std::string>& ret) {
  Status status;
  mutex_.Lock();
  autovector<ColumnFamilyData*> cfds;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    cfd->Ref();
    cfds.push_back(cfd);
  }
  mutex_.Unlock();
  version_edits_.clear();

  for (auto cfd : cfds) {
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    version_edits_.insert({cfd->GetID(), edit});
  }
  for (auto cfd : cfds) {
    auto iter = version_edits_.find(cfd->GetID());
    int job_id = next_job_id_.fetch_add(1);
    VersionEdit* edit = &iter->second;
    status = WriteLevel0TableForRecovery(job_id, cfd, cfd->mem(), edit);
    edit->set_check_point(true);
    if (status.ok()) {
      mutex_.Lock();
      status = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                                      edit, &mutex_);
      mutex_.Unlock();
      if (!status.ok()) {
        break;
      }
    }
  }
  mutex_.Lock();
  for (auto cfd : cfds) {
    cfd->Unref();
  }
  mutex_.Unlock();

  if (status.ok()) {
    for (auto iter : version_edits_) {
      VersionEdit* edit = &iter.second;
      int cf_id = iter.first;
      for (auto f : edit->GetNewFiles()) {
        ret.push_back(MakeTableFileName("", f.second.fd.GetNumber()));
      }
    }
  }
  return status;
}
Status DBImpl::GetLiveFiles(std::vector<std::string>& ret,
                            uint64_t* manifest_file_size, bool flush_memtable) {
  *manifest_file_size = 0;

  mutex_.Lock();

  if (flush_memtable) {
    // flush all dirty data to disk.
    autovector<ColumnFamilyData*> cfds;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      cfd->Ref();
      cfds.push_back(cfd);
    }
    mutex_.Unlock();
    Status status =
        FlushMemTable(cfds, FlushOptions(), FlushReason::kGetLiveFiles);
    TEST_SYNC_POINT("DBImpl::GetLiveFiles:1");
    TEST_SYNC_POINT("DBImpl::GetLiveFiles:2");
    mutex_.Lock();
    for (auto cfd : cfds) {
      cfd->Unref();
    }
    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();

    if (!status.ok()) {
      mutex_.Unlock();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log, "Cannot Flush data %s\n",
                      status.ToString().c_str());
      return status;
    }
  }

  // Make a set of all of the live *.sst files
  std::vector<FileDescriptor> live;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    cfd->current()->AddLiveFiles(&live);
  }

  ret.clear();
  ret.reserve(live.size() + 3);  // *.sst + CURRENT + MANIFEST + OPTIONS

  // create names of the live files. The names are not absolute
  // paths, instead they are relative to dbname_;
  for (const auto& live_file : live) {
    ret.push_back(MakeTableFileName("", live_file.GetNumber()));
  }

  ret.push_back(CurrentFileName(""));
  ret.push_back(DescriptorFileName("", versions_->manifest_file_number()));
  ret.push_back(OptionsFileName("", versions_->options_file_number()));

  // find length of manifest file while holding the mutex lock
  *manifest_file_size = versions_->manifest_file_size();

  mutex_.Unlock();
  return Status::OK();
}

Status DBImpl::GetSortedWalFiles(VectorLogPtr& files) {
  {
    // If caller disabled deletions, this function should return files that are
    // guaranteed not to be deleted until deletions are re-enabled. We need to
    // wait for pending purges to finish since WalManager doesn't know which
    // files are going to be purged. Additional purges won't be scheduled as
    // long as deletions are disabled (so the below loop must terminate).
    InstrumentedMutexLock l(&mutex_);
    while (disable_delete_obsolete_files_ > 0 &&
           pending_purge_obsolete_files_ > 0) {
      bg_cv_.Wait();
    }
  }
  return wal_manager_.GetSortedWalFiles(files);
}

}  // namespace TERARKDB_NAMESPACE

#endif  // ROCKSDB_LITE
