//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <stdint.h>
#ifdef OS_SOLARIS
#include <alloca.h>
#endif

#include <algorithm>
#include <cinttypes>
#include <iostream>
#include <map>
#include <numeric>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/db_info_dumper.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/forward_iterator.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/malloc_stats.h"
#include "db/map_builder.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/periodic_work_scheduler.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "monitoring/in_memory_stats_history.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/persistent_stats_history.h"
#include "monitoring/thread_status_util.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/stats_history.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/terark_namespace.h"
#include "table/block_based_table_factory.h"
#include "table/merging_iterator.h"
#include "table/two_level_iterator.h"
#include "util/autovector.h"
#include "util/build_version.h"
#include "util/c_style_callback.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/file_util.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/logging.h"
#include "util/sst_file_manager_impl.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "utilities/trace/bytedance_metrics_reporter.h"

#ifdef WITH_ZENFS
#include "utilities/trace/zbd_stat.h"
#endif

#if !defined(_MSC_VER) && !defined(__APPLE__)
#include <sys/unistd.h>

#endif
#include "utilities/util/valvec.hpp"

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#ifdef WITH_TERARK_ZIP
#include <table/terark_zip_table.h>

#include <terark/thread/fiber_yield.hpp>
#include <terark/util/fiber_pool.hpp>
#endif

#ifdef WITH_BOOSTLIB
#include <boost/fiber/all.hpp>
#endif

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

namespace TERARKDB_NAMESPACE {

#ifndef ROCKSDB_LITE

class TablePropertiesCollectionIteratorImpl
    : public TablePropertiesCollectionIterator {
 public:
  TablePropertiesCollectionIteratorImpl(DBImpl* impl, ColumnFamilyData* cfd)
      : impl_(impl), cfd_(cfd), version_(nullptr), iter_(file_meta_.end()) {
    impl_->mutex()->Lock();
    cfd->Ref();
    version_ = cfd->current();
    version_->Ref();
    prefix_extractor_ = cfd->GetLatestMutableCFOptions()->prefix_extractor;
    impl_->mutex()->Unlock();
    for (auto pair : version_->storage_info()->dependence_map()) {
      file_meta_.emplace(pair.second);
    }
  }
  virtual ~TablePropertiesCollectionIteratorImpl() {
    JobContext job_context(0);
    bool need_purge = false;
    impl_->mutex()->Lock();
    if (version_->Unref()) {
      need_purge = true;
    }
    if (cfd_->Unref()) {
      need_purge = true;
      delete cfd_;
    }
    if (need_purge) {
      impl_->FindObsoleteFiles(&job_context, false, true);
    }
    impl_->mutex()->Unlock();

    if (job_context.HaveSomethingToDelete()) {
      bool background_purge =
          impl_->immutable_db_options().avoid_unnecessary_blocking_io;
      impl_->PurgeObsoleteFiles(job_context, background_purge);
    }
    job_context.Clean(impl_->mutex());
  }

  void SeekToFirst() override { Update(file_meta_.begin()); }
  void Next() override { Update(std::next(iter_)); }

  size_t size() const override { return file_meta_.size(); }

  const std::string& filename() const override {
    assert(Valid());
    return filename_;
  }
  const std::shared_ptr<const TableProperties>& properties() const override {
    assert(Valid());
    return properties_;
  }

  bool Valid() const override { return iter_ != file_meta_.end(); }
  Status status() const override { return status_; }

 private:
  void Update(chash_set<FileMetaData*>::const_iterator where) {
    if (where != file_meta_.end()) {
      FileMetaData* f = *where;
      filename_ = TableFileName(cfd_->ioptions()->cf_paths, f->fd.GetNumber(),
                                f->fd.GetPathId());
      status_ = cfd_->table_cache()->GetTableProperties(
          impl_->env_options(), cfd_->internal_comparator(), *f, &properties_,
          prefix_extractor_.get(), false);
    }
    if (status_.ok()) {
      iter_ = where;
    } else {
      iter_ = file_meta_.end();
    }
  }

  DBImpl* impl_;
  ColumnFamilyData* cfd_;
  Version* version_;
  std::shared_ptr<const SliceTransform> prefix_extractor_;
  chash_set<FileMetaData*> file_meta_;
  chash_set<FileMetaData*>::const_iterator iter_;
  Status status_;
  std::string filename_;
  std::shared_ptr<const TableProperties> properties_;
};

#endif  // ROCKSDB_LITE

const std::string kDefaultColumnFamilyName("default");
const uint64_t kDumpStatsWaitMicroseconds = 10000;
const std::string kPersistentStatsColumnFamilyName(
    "___rocksdb_stats_history___");
void DumpRocksDBBuildVersion(Logger* log);

CompressionType GetCompressionFlush(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options) {
  // Compressing memtable flushes might not help unless the sequential load
  // optimization is used for leveled compaction. Otherwise the CPU and
  // latency overhead is not offset by saving much space.
  if (ioptions.compaction_style == kCompactionStyleUniversal) {
    if (mutable_cf_options.compaction_options_universal
            .compression_size_percent < 0) {
      return mutable_cf_options.compression;
    } else {
      return kNoCompression;
    }
  } else if (!ioptions.compression_per_level.empty()) {
    // For leveled compress when min_level_to_compress != 0.
    return ioptions.compression_per_level[0];
  } else {
    return mutable_cf_options.compression;
  }
}

namespace {
void DumpSupportInfo(Logger* logger) {
  ROCKS_LOG_HEADER(logger, "Compression algorithms supported:");
  for (auto& compression : OptionsHelper::compression_type_string_map) {
    if (compression.second != kNoCompression &&
        compression.second != kDisableCompressionOption) {
      ROCKS_LOG_HEADER(logger, "\t%s supported: %d", compression.first.c_str(),
                       CompressionTypeSupported(compression.second));
    }
  }
  ROCKS_LOG_HEADER(logger, "Fast CRC32 supported: %s",
                   crc32c::IsFastCrc32Supported().c_str());
}

int64_t kDefaultLowPriThrottledRate = 2 * 1024 * 1024;
}  // namespace

static std::string write_qps_metric_name = "dbimpl_writeimpl_qps";
static std::string read_qps_metric_name = "dbimpl_getimpl_qps";
static std::string newiterator_qps_metric_name = "dbimpl_newiterator_qps";
static std::string seek_qps_metric_name = "dbiter_seek_qps";
static std::string next_qps_metric_name = "dbiter_next_qps";
static std::string seekforprev_qps_metric_name = "dbiter_seekforprev_qps";
static std::string prev_qps_metric_name = "dbiter_prev_qps";

static std::string write_latency_metric_name = "dbimpl_writeimpl_latency";
static std::string read_latency_metric_name = "dbimpl_getimpl_latency";
static std::string newiterator_latency_metric_name =
    "dbimpl_newiterator_latency";
static std::string seek_latency_metric_name = "dbiter_seek_latency";
static std::string next_latency_metric_name = "dbiter_next_latency";
static std::string seekforprev_latency_metric_name =
    "dbiter_seekforprev_latency";
static std::string prev_latency_metric_name = "dbiter_prev_latency";

static std::string zenfs_get_snapshot_latency_metric_name =
    "dbimpl_zenfs_get_snapshot_latency";

static std::string write_throughput_metric_name = "dbimpl_writeimpl_throughput";
static std::string write_batch_size_metric_name = "dbimpl_writeimpl_batch_size";
#ifdef WITH_ZENFS
std::string MetricsTag(Env* env);
#endif

DBImpl::DBImpl(const DBOptions& options, const std::string& dbname,
               const bool seq_per_batch, const bool batch_per_txn)
    : env_(options.env),
      dbname_(dbname),
      own_info_log_(options.info_log == nullptr),
      initial_db_options_(SanitizeOptions(dbname, options)),
      immutable_db_options_(initial_db_options_),
      mutable_db_options_(initial_db_options_),
      stats_(immutable_db_options_.statistics.get()),
      db_lock_(nullptr),
      mutex_(stats_, env_, DB_MUTEX_WAIT_MICROS,
             immutable_db_options_.use_adaptive_mutex),
      shutting_down_(false),
      bg_cv_(&mutex_),
      logfile_number_(0),
      log_writer_pool_state_(kLogWriterPoolIdle),
      memtable_info_queue_lock_(false),
      log_dir_synced_(false),
      log_empty_(true),
      default_cf_handle_(nullptr),
      persist_stats_cf_handle_(nullptr),
      log_sync_cv_(&mutex_),
      total_log_size_(0),
      max_total_in_memory_state_(0),
      is_snapshot_supported_(true),
      write_buffer_manager_(immutable_db_options_.write_buffer_manager.get()),
      write_thread_(immutable_db_options_),
      nonmem_write_thread_(immutable_db_options_),
      write_controller_(mutable_db_options_.delayed_write_rate),
      // Use delayed_write_rate as a base line to determine the initial
      // low pri write rate limit. It may be adjusted later.
      low_pri_write_rate_limiter_(NewGenericRateLimiter(std::min(
          static_cast<int64_t>(mutable_db_options_.delayed_write_rate / 8),
          kDefaultLowPriThrottledRate))),
      last_batch_group_size_(0),
      unscheduled_flushes_(0),
      unscheduled_compactions_(0),
      unscheduled_garbage_collections_(0),
      bg_bottom_compaction_scheduled_(0),
      bg_compaction_scheduled_(0),
      bg_garbage_collection_scheduled_(0),
      num_running_compactions_(0),
      num_running_garbage_collections_(0),
      bg_flush_scheduled_(0),
      num_running_flushes_(0),
      bg_purge_scheduled_(0),
      disable_delete_obsolete_files_(0),
      pending_purge_obsolete_files_(0),
      delete_obsolete_files_last_run_(env_->NowMicros()),
      last_stats_dump_time_microsec_(0),
      next_job_id_(1),
      has_unpersisted_data_(false),
      delete_obsolete_files_lock_(false),
      unable_to_release_oldest_log_(false),
      env_options_(BuildDBOptions(immutable_db_options_, mutable_db_options_)),
      env_options_for_compaction_(env_->OptimizeForCompactionTableWrite(
          env_options_, immutable_db_options_)),
      num_running_ingest_file_(0),
#ifndef ROCKSDB_LITE
      wal_manager_(immutable_db_options_, env_options_, seq_per_batch),
#endif  // ROCKSDB_LITE
      event_logger_(immutable_db_options_.info_log.get()),
      bg_work_paused_(0),
      bg_compaction_paused_(0),
      refitting_level_(false),
      opened_successfully_(false),
#ifndef ROCKSDB_LITE
      periodic_work_scheduler_(nullptr),
#endif  // ROCKSDB_LITE
      two_write_queues_(options.two_write_queues),
      manual_wal_flush_(options.manual_wal_flush),
      seq_per_batch_(seq_per_batch),
      batch_per_txn_(batch_per_txn),
      // last_sequencee_ is always maintained by the main queue that also writes
      // to the memtable. When two_write_queues_ is disabled last seq in
      // memtable is the same as last seq published to the readers. When it is
      // enabled but seq_per_batch_ is disabled, last seq in memtable still
      // indicates last published seq since wal-only writes that go to the 2nd
      // queue do not consume a sequence number. Otherwise writes performed by
      // the 2nd queue could change what is visible to the readers. In this
      // cases, last_seq_same_as_publish_seq_==false, the 2nd queue maintains a
      // separate variable to indicate the last published sequence.
      last_seq_same_as_publish_seq_(
          !(seq_per_batch && options.two_write_queues)),
      // Since seq_per_batch_ is currently set only by WritePreparedTxn which
      // requires a custom gc for compaction, we use that to set use_custom_gc_
      // as well.
      use_custom_gc_(seq_per_batch),
      shutdown_initiated_(false),
      own_sfm_(options.sst_file_manager == nullptr),
      preserve_deletes_(options.preserve_deletes),
      closed_(false),
      error_handler_(this, immutable_db_options_, &mutex_),
      atomic_flush_install_cv_(&mutex_),
#ifdef WITH_ZENFS
      bytedance_tags_("dbname=" + MetricsTag(env_) + "/" + dbname),
#else
      bytedance_tags_("dbname=" + dbname),
#endif

      metrics_reporter_factory_(
          options.metrics_reporter_factory == nullptr
              ? std::make_shared<ByteDanceMetricsReporterFactory>()
              : options.metrics_reporter_factory),
      console_runner_(this, dbname, env_, immutable_db_options_.info_log.get()),

      write_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          write_qps_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      read_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          read_qps_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      newiterator_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          newiterator_qps_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      seek_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          seek_qps_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      next_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          next_qps_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      seekforprev_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          seekforprev_qps_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      prev_qps_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          prev_qps_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),

      write_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          write_latency_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      read_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          read_latency_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      newiterator_latency_reporter_(
          *metrics_reporter_factory_->BuildHistReporter(
              newiterator_latency_metric_name, bytedance_tags_,
              immutable_db_options_.info_log.get(), env_)),
      seek_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          seek_latency_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      next_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          next_latency_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      seekforprev_latency_reporter_(
          *metrics_reporter_factory_->BuildHistReporter(
              seekforprev_latency_metric_name, bytedance_tags_,
              immutable_db_options_.info_log.get(), env_)),
      prev_latency_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          prev_latency_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      zenfs_get_snapshot_latency_reporter_(
          *metrics_reporter_factory_->BuildHistReporter(
              zenfs_get_snapshot_latency_metric_name, bytedance_tags_,
              immutable_db_options_.info_log.get(), env_)),
      write_throughput_reporter_(*metrics_reporter_factory_->BuildCountReporter(
          write_throughput_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)),
      write_batch_size_reporter_(*metrics_reporter_factory_->BuildHistReporter(
          write_batch_size_metric_name, bytedance_tags_,
          immutable_db_options_.info_log.get(), env_)) {
  // !batch_per_trx_ implies seq_per_batch_ because it is only unset for
  // WriteUnprepared, which should use seq_per_batch_.
  assert(batch_per_txn_ || seq_per_batch_);
  env_->GetAbsolutePath(dbname, &db_absolute_path_);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  // Give a large number for setting of "infinite" open files.
  const int table_cache_size = (mutable_db_options_.max_open_files == -1)
                                   ? TableCache::kInfiniteCapacity
                                   : mutable_db_options_.max_open_files - 10;
  table_cache_ = NewLRUCache(table_cache_size,
                             immutable_db_options_.table_cache_numshardbits);

  versions_.reset(new VersionSet(dbname_, &immutable_db_options_, env_options_,
                                 seq_per_batch, table_cache_.get(),
                                 write_buffer_manager_, &write_controller_));
  column_family_memtables_.reset(
      new ColumnFamilyMemTablesImpl(versions_->GetColumnFamilySet()));

  DumpRocksDBBuildVersion(immutable_db_options_.info_log.get());
  SetDbSessionId();
  DumpDBFileSummary(immutable_db_options_, dbname_, db_session_id_);
  immutable_db_options_.Dump(immutable_db_options_.info_log.get());
  mutable_db_options_.Dump(immutable_db_options_.info_log.get());
  DumpSupportInfo(immutable_db_options_.info_log.get());

  // always open the DB with 0 here, which means if preserve_deletes_==true
  // we won't drop any deletion markers until SetPreserveDeletesSequenceNumber()
  // is called by client and this seqnum is advanced.
  preserve_deletes_seqnum_.store(0);
}

Status DBImpl::Resume() {
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "Resuming DB");

  InstrumentedMutexLock db_mutex(&mutex_);

  if (!error_handler_.IsDBStopped() && !error_handler_.IsBGWorkStopped()) {
    // Nothing to do
    return Status::OK();
  }

  if (error_handler_.IsRecoveryInProgress()) {
    // Don't allow a mix of manual and automatic recovery
    return Status::Busy();
  }

  mutex_.Unlock();
  Status s = error_handler_.RecoverFromBGError(true);
  mutex_.Lock();
  return s;
}

// This function implements the guts of recovery from a background error. It
// is eventually called for both manual as well as automatic recovery. It does
// the following -
// 1. Wait for currently scheduled background flush/compaction to exit, in
//    order to inadvertently causing an error and thinking recovery failed
// 2. Flush memtables if there's any data for all the CFs. This may result
//    another error, which will be saved by error_handler_ and reported later
//    as the recovery status
// 3. Find and delete any obsolete files
// 4. Schedule compactions if needed for all the CFs. This is needed as the
//    flush in the prior step might have been a no-op for some CFs, which
//    means a new super version wouldn't have been installed
Status DBImpl::ResumeImpl() {
  mutex_.AssertHeld();
  WaitForBackgroundWork();

  Status bg_error = error_handler_.GetBGError();
  Status s;
  if (shutdown_initiated_) {
    // Returning shutdown status to SFM during auto recovery will cause it
    // to abort the recovery and allow the shutdown to progress
    s = Status::ShutdownInProgress();
  }
  if (s.ok() && bg_error.severity() > Status::Severity::kHardError) {
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log,
        "DB resume requested but failed due to Fatal/Unrecoverable error");
    s = bg_error;
  }

  // We cannot guarantee consistency of the WAL. So force flush Memtables of
  // all the column families
  if (s.ok()) {
    FlushOptions flush_opts;
    // We allow flush to stall write since we are trying to resume from error.
    flush_opts.allow_write_stall = true;
    autovector<ColumnFamilyData*> cfds;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      cfd->Ref();
      cfds.push_back(cfd);
    }
    mutex_.Unlock();
    s = FlushMemTable(cfds, flush_opts, FlushReason::kErrorRecovery);
    mutex_.Lock();
    for (auto cfd : cfds) {
      cfd->Unref();
    }
    if (!s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "DB resume requested but failed due to Flush failure [%s]",
                     s.ToString().c_str());
    }
  }

  JobContext job_context(0);
  FindObsoleteFiles(&job_context, true);
  if (s.ok()) {
    s = error_handler_.ClearBGError();
  }
  mutex_.Unlock();

  job_context.manifest_file_number = 1;
  if (job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean(&mutex_);

  if (s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Successfully resumed DB");
  }
  mutex_.Lock();
  // Check for shutdown again before scheduling further compactions,
  // since we released and re-acquired the lock above
  if (shutdown_initiated_) {
    s = Status::ShutdownInProgress();
  }
  if (s.ok()) {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      SchedulePendingCompaction(cfd);
      SchedulePendingGarbageCollection(cfd);
    }
    MaybeScheduleFlushOrCompaction();
  }

  // Wake up any waiters - in this case, it could be the shutdown thread
  bg_cv_.SignalAll();

  // No need to check BGError again. If something happened, event listener would
  // be notified and the operation causing it would have failed
  return s;
}

void DBImpl::WaitForBackgroundWork() {
  // Wait for background work to finish
  while (bg_bottom_compaction_scheduled_ || bg_compaction_scheduled_ ||
         bg_flush_scheduled_) {
    bg_cv_.Wait();
  }
}

// Will lock the mutex_,  will wait for completion if wait is true
void DBImpl::CancelAllBackgroundWork(bool wait) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Shutdown: canceling all background work");

#ifndef ROCKSDB_LITE
  if (periodic_work_scheduler_ != nullptr) {
    periodic_work_scheduler_->Unregister(this);
  }
#endif  // !ROCKSDB_LITE

  InstrumentedMutexLock l(&mutex_);
  if (!shutting_down_.load(std::memory_order_acquire) &&
      has_unpersisted_data_.load(std::memory_order_relaxed) &&
      !mutable_db_options_.avoid_flush_during_shutdown) {
    autovector<ColumnFamilyData*> cfds;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->IsDropped() && cfd->initialized() && !cfd->mem()->IsEmpty()) {
        cfd->Ref();
        cfds.push_back(cfd);
      }
    }
    mutex_.Unlock();
    FlushMemTable(cfds, FlushOptions(), FlushReason::kShutDown);
    mutex_.Lock();
    for (auto cfd : cfds) {
      cfd->Unref();
    }
    versions_->GetColumnFamilySet()->FreeDeadColumnFamilies();
  }

  shutting_down_.store(true, std::memory_order_release);
  bg_cv_.SignalAll();
  if (!wait) {
    return;
  }
  WaitForBackgroundWork();
}

Status DBImpl::CloseHelper() {
  console_runner_.closing_ = true;

  // Guarantee that there is no background error recovery in progress before
  // continuing with the shutdown
  mutex_.Lock();
  shutdown_initiated_ = true;
  error_handler_.CancelErrorRecovery();
  while (error_handler_.IsRecoveryInProgress()) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  // CancelAllBackgroundWork called with false means we just set the shutdown
  // marker. After this we do a variant of the waiting and unschedule work
  // (to consider: moving all the waiting into CancelAllBackgroundWork(true))
  CancelAllBackgroundWork(false);

  Status ret;
  mutex_.Lock();
  int bg_unscheduled = env_->UnSchedule(this, Env::Priority::BOTTOM);
  bg_unscheduled += env_->UnSchedule(this, Env::Priority::LOW);
  bg_unscheduled += env_->UnSchedule(this, Env::Priority::HIGH);

  // Wait for background work to finish
  while (true) {
    int bg_scheduled = bg_bottom_compaction_scheduled_ +
                       bg_compaction_scheduled_ + bg_flush_scheduled_ +
                       bg_purge_scheduled_ - bg_unscheduled;
    if (bg_scheduled || pending_purge_obsolete_files_ ||
        error_handler_.IsRecoveryInProgress() || !console_runner_.closed_) {
      TEST_SYNC_POINT("DBImpl::~DBImpl:WaitJob");
      bg_cv_.TimedWait(env_->NowMicros() + 10000);
    } else {
      bg_bottom_compaction_scheduled_ = 0;
      bg_compaction_scheduled_ = 0;
      bg_flush_scheduled_ = 0;
      bg_purge_scheduled_ = 0;
      break;
    }
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::CloseHelper:PendingPurgeFinished",
                           &files_grabbed_for_purge_);
  EraseThreadStatusDbInfo();
  flush_scheduler_.Clear();

  while (!flush_queue_.empty()) {
    const FlushRequest& flush_req = PopFirstFromFlushQueue();
    for (const auto& iter : flush_req) {
      ColumnFamilyData* cfd = iter.first;
      cfd->dec_queued_for_flush();
      if (cfd->Unref()) {
        delete cfd;
      }
    }
  }
  while (!compaction_queue_.empty()) {
    auto cfd = PopFirstFromCompactionQueue();
    if (cfd->Unref()) {
      delete cfd;
    }
  }
  while (!garbage_collection_queue_.empty()) {
    auto cfd = PopFirstFromGarbageCollectionQueue();
    if (cfd->Unref()) {
      delete cfd;
    }
  }

  log_writer_pool_.clear();
  for (auto& item : memtable_info_queue_) {
    if (item.first->Unref()) {
      delete item.first;
    }
  }
  memtable_info_queue_.clear();

  if (default_cf_handle_ != nullptr || persist_stats_cf_handle_ != nullptr) {
    // we need to delete handle outside of lock because it does its own locking
    mutex_.Unlock();
    if (default_cf_handle_) {
      delete default_cf_handle_;
      default_cf_handle_ = nullptr;
    }
    if (persist_stats_cf_handle_) {
      delete persist_stats_cf_handle_;
      persist_stats_cf_handle_ = nullptr;
    }
    mutex_.Lock();
  }

  // Clean up obsolete files due to SuperVersion release.
  // (1) Need to delete to obsolete files before closing because RepairDB()
  // scans all existing files in the file system and builds manifest file.
  // Keeping obsolete files confuses the repair process.
  // (2) Need to check if we Open()/Recover() the DB successfully before
  // deleting because if VersionSet recover fails (may be due to corrupted
  // manifest file), it is not able to identify live files correctly. As a
  // result, all "live" files can get deleted by accident. However, corrupted
  // manifest is recoverable by RepairDB().
  if (opened_successfully_) {
    JobContext job_context(next_job_id_.fetch_add(1));
    FindObsoleteFiles(&job_context, true);

    mutex_.Unlock();
    // manifest number starting from 2
    job_context.manifest_file_number = 1;
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
    mutex_.Lock();
    job_context.Clean(nullptr);
  }

  for (auto l : logs_to_free_) {
    delete l;
  }
  for (auto l : logs_to_free_queue_) {
    delete l;
  }
  for (auto& log : logs_) {
    uint64_t log_number = log.writer->get_log_number();
    Status s = log.ClearWriter();
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          immutable_db_options_.info_log,
          "Unable to Sync WAL file %s with error -- %s",
          LogFileName(immutable_db_options_.wal_dir, log_number).c_str(),
          s.ToString().c_str());
      // Retain the first error
      if (ret.ok()) {
        ret = s;
      }
    }
  }
  logs_.clear();

  // Table cache may have table handles holding blocks from the block cache.
  // We need to release them before the block cache is destroyed. The block
  // cache may be destroyed inside versions_.reset(), when column family data
  // list is destroyed, so leaving handles in table cache after
  // versions_.reset() may cause issues.
  // Here we clean all unreferenced handles in table cache.
  // Now we assume all user queries have finished, so only version set itself
  // can possibly hold the blocks from block cache. After releasing unreferenced
  // handles here, only handles held by version set left and inside
  // versions_.reset(), we will release them. There, we need to make sure every
  // time a handle is released, we erase it from the cache too. By doing that,
  // we can guarantee that after versions_.reset(), table cache is empty
  // so the cache can be safely destroyed.
  table_cache_->EraseUnRefEntries();

  for (auto& txn_entry : recovered_transactions_) {
    delete txn_entry.second;
  }

  // versions need to be destroyed before table_cache since it can hold
  // references to table_cache.
  versions_.reset();
  mutex_.Unlock();
  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log, "Shutdown complete");
  LogFlush(immutable_db_options_.info_log);

#ifndef ROCKSDB_LITE
  // If the sst_file_manager was allocated by us during DB::Open(), ccall
  // Close() on it before closing the info_log. Otherwise, background thread
  // in SstFileManagerImpl might try to log something
  if (immutable_db_options_.sst_file_manager && own_sfm_) {
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    sfm->Close();
  }
#endif  // ROCKSDB_LITE

  if (immutable_db_options_.info_log && own_info_log_) {
    Status s = immutable_db_options_.info_log->Close();
    if (ret.ok()) {
      ret = s;
    }
  }
  return ret;
}

Status DBImpl::CloseImpl() { return CloseHelper(); }

DBImpl::~DBImpl() {
  if (!closed_) {
    closed_ = true;
    CloseHelper();
  }
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || immutable_db_options_.paranoid_checks) {
    // No change needed
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "Ignoring error %s",
                   s->ToString().c_str());
    *s = Status::OK();
  }
}

const Status DBImpl::CreateArchivalDirectory() {
  if (immutable_db_options_.wal_ttl_seconds > 0 ||
      immutable_db_options_.wal_size_limit_mb > 0) {
    std::string archivalPath = ArchivalDirectory(immutable_db_options_.wal_dir);
    return env_->CreateDirIfMissing(archivalPath);
  }
  return Status::OK();
}

void DBImpl::PrintStatistics() {
  auto dbstats = immutable_db_options_.statistics.get();
  if (dbstats) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "STATISTICS:\n %s",
                   dbstats->ToString().c_str());
  }
}

void DBImpl::StartPeriodicWorkScheduler() {
#ifndef ROCKSDB_LITE
  {
    InstrumentedMutexLock l(&mutex_);
    periodic_work_scheduler_ = PeriodicWorkScheduler::Default();
    TEST_SYNC_POINT_CALLBACK("DBImpl::StartPeriodicWorkScheduler:Init",
                             &periodic_work_scheduler_);
  }

  periodic_work_scheduler_->Register(
      this, mutable_db_options_.stats_dump_period_sec,
      mutable_db_options_.stats_persist_period_sec);
#endif  // !ROCKSDB_LITE
}

// esitmate the total size of stats_history_
size_t DBImpl::EstimateInMemoryStatsHistorySize() const {
  size_t size_total =
      sizeof(std::map<uint64_t, std::map<std::string, uint64_t>>);
  if (stats_history_.size() == 0) return size_total;
  size_t size_per_slice =
      sizeof(uint64_t) + sizeof(std::map<std::string, uint64_t>);
  // non-empty map, stats_history_.begin() guaranteed to exist
  std::map<std::string, uint64_t> sample_slice(stats_history_.begin()->second);
  for (const auto& pairs : sample_slice) {
    size_per_slice +=
        pairs.first.capacity() + sizeof(pairs.first) + sizeof(pairs.second);
  }
  size_total = size_per_slice * stats_history_.size();
  return size_total;
}

void DBImpl::PersistStats() {
  TEST_SYNC_POINT("DBImpl::PersistStats:Entry");
#ifndef ROCKSDB_LITE
  if (shutdown_initiated_) {
    return;
  }
  TEST_SYNC_POINT("DBImpl::PersistStats:StartRunning");
  uint64_t now_seconds = env_->NowMicros() / 1000 * 1000;
  Statistics* statistics = immutable_db_options_.statistics.get();
  if (!statistics) {
    return;
  }
  size_t stats_history_size_limit = 0;
  {
    InstrumentedMutexLock l(&mutex_);
    stats_history_size_limit = mutable_db_options_.stats_history_buffer_size;
  }

  std::map<std::string, uint64_t> stats_map;
  if (!statistics->getTickerMap(&stats_map)) {
    return;
  }

  if (immutable_db_options_.persist_stats_to_disk) {
    WriteBatch batch;
    if (stats_slice_initialized_) {
      for (const auto& stat : stats_map) {
        char key[100];
        int length =
            EncodePersistentStatsKey(now_seconds, stat.first, 100, key);
        // calculate the delta from last time
        if (stats_slice_.find(stat.first) != stats_slice_.end()) {
          uint64_t delta = stat.second - stats_slice_[stat.first];
          batch.Put(persist_stats_cf_handle_, Slice(key, std::min(100, length)),
                    ToString(delta));
        }
      }
    }
    stats_slice_initialized_ = true;
    std::swap(stats_slice_, stats_map);
    WriteOptions wo;
    wo.low_pri = true;
    wo.no_slowdown = true;
    wo.sync = false;
    Status s = Write(wo, &batch);
    if (!s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Writing to persistent stats CF failed -- %s\n",
                     s.ToString().c_str());
    }
    // TODO(Zhongyi): add purging for persisted data
  } else {
    InstrumentedMutexLock l(&stats_history_mutex_);
    // calculate the delta from last time
    if (stats_slice_initialized_) {
      std::map<std::string, uint64_t> stats_delta;
      for (const auto& stat : stats_map) {
        if (stats_slice_.find(stat.first) != stats_slice_.end()) {
          stats_delta[stat.first] = stat.second - stats_slice_[stat.first];
        }
      }
      stats_history_[now_seconds] = stats_delta;
    }
    stats_slice_initialized_ = true;
    std::swap(stats_slice_, stats_map);
    TEST_SYNC_POINT("DBImpl::PersistStats:StatsCopied");

    // delete older stats snapshots to control memory consumption
    bool purge_needed =
        EstimateInMemoryStatsHistorySize() > stats_history_size_limit;
    while (purge_needed && !stats_history_.empty()) {
      stats_history_.erase(stats_history_.begin());
      purge_needed =
          EstimateInMemoryStatsHistorySize() > stats_history_size_limit;
    }
  }

  TEST_SYNC_POINT("DBImpl::PersistStats:End");
#endif  // !ROCKSDB_LITE
}

bool DBImpl::FindStatsByTime(uint64_t start_time, uint64_t end_time,
                             uint64_t* new_time,
                             std::map<std::string, uint64_t>* stats_map) {
  assert(new_time);
  assert(stats_map);
  if (!new_time || !stats_map) return false;
  // lock when search for start_time
  {
    InstrumentedMutexLock l(&stats_history_mutex_);
    auto it = stats_history_.lower_bound(start_time);
    if (it != stats_history_.end() && it->first < end_time) {
      // make a copy for timestamp and stats_map
      *new_time = it->first;
      *stats_map = it->second;
      return true;
    } else {
      return false;
    }
  }
}

Status DBImpl::GetStatsHistory(
    uint64_t start_time, uint64_t end_time,
    std::unique_ptr<StatsHistoryIterator>* stats_iterator) {
  if (!stats_iterator) {
    return Status::InvalidArgument("stats_iterator not preallocated.");
  }
  if (immutable_db_options_.persist_stats_to_disk) {
    stats_iterator->reset(
        new PersistentStatsHistoryIterator(start_time, end_time, this));
  } else {
    stats_iterator->reset(
        new InMemoryStatsHistoryIterator(start_time, end_time, this));
  }
  return (*stats_iterator)->status();
}

void DBImpl::ScheduleTtlGC() {
  TEST_SYNC_POINT("DBImpl:ScheduleTtlGC");
  LogBuffer log_buffer_info(InfoLogLevel::INFO_LEVEL,
                            immutable_db_options_.info_log.get());
  LogBuffer log_buffer_debug(InfoLogLevel::DEBUG_LEVEL,
                             immutable_db_options_.info_log.get());

  auto should_marked_for_compacted = [&](int level, uint64_t file_number,
                                         uint64_t earliest_time_begin_compact,
                                         uint64_t latest_time_end_compact,
                                         uint64_t now) {
    bool should_mark =
        std::min(earliest_time_begin_compact, latest_time_end_compact) <= now;
    if (should_mark) {
      auto max_to_zero = [](uint64_t v) {
        return v == port::kMaxUint64 ? 0 : v;
      };
      ROCKS_LOG_BUFFER(
          &log_buffer_info,
          "SST #%" PRIu64 " marked for compaction @L%d , property: (%" PRIu64
          " , %" PRIu64 ") now: %" PRIu64,
          file_number, level, max_to_zero(earliest_time_begin_compact),
          max_to_zero(latest_time_end_compact), now);
    }
    return should_mark;
  };
  mutex_.Lock();
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    uint64_t new_mark_count = 0;
    uint64_t old_mark_count = 0;
    uint64_t total_count = 0;
    if (!cfd->initialized() || cfd->IsDropped() ||
        cfd->ioptions()->ttl_extractor_factory == nullptr) {
      continue;
    }
    uint64_t now = cfd->ioptions()->ttl_extractor_factory->Now();
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    for (int l = 0; l < vstorage->num_non_empty_levels(); l++) {
      for (auto meta : vstorage->LevelFiles(l)) {
        if (meta->being_compacted) {
          continue;
        }
        ++total_count;
        bool marked =
            !!(meta->marked_for_compaction & FileMetaData::kMarkedFromTTL);
        old_mark_count += marked;
        TEST_SYNC_POINT("DBImpl:Exist-SST");
        if (!marked &&
            should_marked_for_compacted(
                l, meta->fd.GetNumber(), meta->prop.earliest_time_begin_compact,
                meta->prop.latest_time_end_compact, now)) {
          meta->marked_for_compaction |= FileMetaData::kMarkedFromTTL;
          marked = true;
        }
        if (marked) {
          new_mark_count++;
          TEST_SYNC_POINT("DBImpl:ScheduleTtlGC-mark");
        }
      }
    }
    if (new_mark_count > old_mark_count) {
      vstorage->ComputeCompactionScore(*cfd->ioptions(),
                                       *cfd->GetLatestMutableCFOptions());
      if (!cfd->queued_for_compaction()) {
        AddToCompactionQueue(cfd);
        unscheduled_compactions_++;
      }
    }
    ROCKS_LOG_BUFFER(&log_buffer_debug,
                     "[%s] SSTs total marked = %" PRIu64
                     ", new marked = %" PRIu64 ", file count: %" PRIu64,
                     cfd->GetName().c_str(), old_mark_count, new_mark_count,
                     total_count);
  }
  if (unscheduled_compactions_ > 0) {
    MaybeScheduleFlushOrCompaction();
  }
  mutex_.Unlock();
  log_buffer_info.FlushBufferToLog();
  log_buffer_debug.FlushBufferToLog();
}

#ifdef WITH_ZENFS
// Implemented inside `env/env_zenfs.cc`
void GetStat(Env* env, BDZenFSStat& stat);
void GetZenFSSnapshot(Env* env, ZenFSSnapshot& snapshot,
                      const ZenFSSnapshotOptions& options);
// Migrate target zone's all extents to a new zone
void MigrateExtents(Env* env, const std::vector<ZoneExtentSnapshot*>& ext,
                    bool direct_io);

void DBImpl::ScheduleMetricsReporter() {
  // TEST_SYNC_POINT("DBImpl:ScheduleMetricsReporter");
  LatencyHistGuard guard(&zenfs_get_snapshot_latency_reporter_);
  ZenFSSnapshot snapshot;
  ZenFSSnapshotOptions options;
  options.zbd_ = 1;
  options.zone_file_ = 0;
  options.trigger_report_ = 1;
  GetZenFSSnapshot(env_, snapshot, options);
}

void DBImpl::ScheduleZNSGC() {
  TEST_SYNC_POINT("DBImpl:ScheduleZNSGC");
  uint64_t nowSeconds = env_->NowMicros() / 1000U / 1000U;
  LogBuffer log_buffer_info(InfoLogLevel::INFO_LEVEL,
                            immutable_db_options_.info_log.get());
  LogBuffer log_buffer_debug(InfoLogLevel::DEBUG_LEVEL,
                             immutable_db_options_.info_log.get());

  chash_set<uint64_t> mark_for_gc;

  BDZenFSStat zenfs_stat;
  {
    LatencyHistGuard guard(&zenfs_get_snapshot_latency_reporter_);
    // Pick files for GC
    GetStat(env_, zenfs_stat);
    // ROCKS_LOG_BUFFER(&log_buffer_info,"ZNS GC :\n\t[GetStat]=%s\n",
    //                  zenfs_stat.ToString());
  }
  std::vector<BDZoneStat>& stat = zenfs_stat.zone_stats_;

  uint64_t number;
  FileType type;

  // Merge db paths and column family paths together
  chash_set<std::string> db_paths;

  // Get column family paths
  mutex_.Lock();
  double low_r = mutable_db_options_.zenfs_low_gc_ratio;
  double high_r = mutable_db_options_.zenfs_high_gc_ratio;
  double force_r = mutable_db_options_.zenfs_force_gc_ratio;

  for (auto cfd : *versions_->GetColumnFamilySet()) {
    for (const auto& path : cfd->ioptions()->db_paths) {
      db_paths.emplace(path.path);
    }
  }
  mutex_.Unlock();

  // Get database paths
  for (const auto& path : immutable_db_options_.db_paths) {
    db_paths.emplace(path.path);
  }

  std::set<uint64_t> migrate_zone_ids;
  std::string strip_filename;
  size_t free = 0, used = 0, reclaim = 0, total = 0;
  for (const auto& zone : stat) {
    // Collect all zones with over 80% garbage
    if (zone.GarbageRate() > 0.8) {
      migrate_zone_ids.emplace(zone.start_position);
    }

    free += zone.free_capacity;
    used += zone.used_capacity;
    total += zone.used_capacity + zone.reclaim_capacity;
    if (zone.free_capacity == 0) {
      reclaim += zone.reclaim_capacity;
    }
  }

  // Migrate all proper extents larger than `min_szie`
  uint32_t min_size = 128 << 10;
  std::vector<ZoneExtentSnapshot*> migrate_exts;
  for (auto& ext : zenfs_stat.snapshot_.extents_) {
    if (migrate_zone_ids.find(ext.zone_start) != migrate_zone_ids.end() &&
        ext.length > min_size) {
      migrate_exts.push_back(&ext);
    }
  }
  MigrateExtents(env_, migrate_exts,
                 immutable_db_options_.use_direct_io_for_flush_and_compaction);
  ROCKS_LOG_BUFFER(&log_buffer_info, "ZNS GC: Migrate Extent Count: %d",
                   migrate_exts.size());

  // Overall free capacity ratio of disk.
  double free_r = double(free) / total;

  // Overall used capacity ratio of disk.
  double used_r = 1.0 - free_r;

  // Overall trash capacity ratio of disk.
  double trash_r = double(reclaim) / total;

  // Variable target free space ratio threshold for single zone,
  // Recycle the zone when valid data in zone <= target_r * total_capacity.
  double target_r = 0.95 - force_r * free_r;

  // Scan the disk in order to find files which needs to be marked.
  for (const auto& zone : stat) {
    std::vector<uint64_t> sst_in_zone;
    // Skip unfinished zones.
    if (zone.free_capacity == 0) {
      uint64_t total_size = 0;
      bool ignore_zone = false;
      for (const auto& file : zone.files) {
        strip_filename.clear();

        for (const auto& path : db_paths) {
          if (Slice(file.filename).starts_with(path)) {
            strip_filename.assign(file.filename, path.length(),
                                  file.filename.length() - path.length());
            break;
          }
        }

        if (strip_filename.empty()) {
          // This file is not in DB folder.
          ignore_zone = true;
          break;
        }

        if (ParseFileName(strip_filename, &number, Slice(), &type)) {
          // Is SST file, and is of current TerarkDB instance.
          if (type == kTableFile) {
            total_size += file.size_in_zone;
            sst_in_zone.push_back(number);
          } else {
            // This zone contains file other than SSTs or files from other
            // databases. We ignore the zone for now. When other files (like
            // logs) have been deleted, we will come back and recycle this zone.
            ignore_zone = true;
            break;
          }
        } else {
          // This file is not recognized by TerarkDB (or RocksDB). Even if we
          // move the file, the zone may not be reset. Therefore, we simply
          // ignore this zone.
          ignore_zone = true;
          break;
        }
      }

      if (ignore_zone) {
        continue;
      }

      // if data in zone <= target_r * total_capacity, recycle the zone
      if (total_size <=
          target_r * (zone.used_capacity + zone.reclaim_capacity)) {
        for (auto&& file_id : sst_in_zone) {
          mark_for_gc.insert(file_id);
        }
        if (used_r >= force_r) {
          // Only one zone forcely recycled
          break;
        }
      }
    }
  }

  auto mask = free_r < high_r ? FileMetaData::kMarkedFromFileSystemHigh
                              : FileMetaData::kMarkedFromFileSystem;

  uint64_t total_count = 0;
  uint64_t total_new_mark_count = 0;
  uint64_t total_old_mark_count = 0;

  mutex_.Lock();
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    uint64_t new_mark_count = 0;
    uint64_t old_mark_count = 0;
    if (!cfd->initialized() || cfd->IsDropped()) {
      continue;
    }
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    // Level -1 contains SSTs inside lazy compaction SST index.
    // By iterating level -1, we could collect that kind of garbage.
    // But we still recommend using ZNS GC without lazy compaction
    // enabled.
    for (int l = -1; l < vstorage->num_non_empty_levels(); l++) {
      for (auto meta : vstorage->LevelFiles(l)) {
        ++total_count;
        if (meta->being_compacted) {
          continue;
        }
        bool marked = !!(meta->marked_for_compaction & mask);
        old_mark_count += marked;
        TEST_SYNC_POINT("DBImpl:Exist-SST");
        if (mark_for_gc.count(meta->fd.GetNumber()) > 0) {
          if (!marked) {
            meta->marked_for_compaction |= mask;
            marked = true;
          }
          if (used_r >= force_r) {
            // Generate a compaction and schedule at once.
            auto ca = new CompactionArg;
            ca->db = this;
            ca->prepicked_compaction = new PrepickedCompaction;
            ca->prepicked_compaction->manual_compaction_state = nullptr;
            std::vector<CompactionInputFiles> inputs(1);
            inputs[0].level = l;
            inputs[0].files.push_back(meta);
            ca->prepicked_compaction->compaction =
                cfd->compaction_picker()->CompactFiles(
                    CompactionOptions(), inputs, l, vstorage,
                    *cfd->GetLatestMutableCFOptions(), meta->fd.GetPathId());
            ca->prepicked_compaction->compaction->SetInputVersion(
                cfd->current());
            bg_compaction_scheduled_++;
            env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::FORCE,
                           this, &DBImpl::UnscheduleCallback);
          }
        }
        if (marked) {
          new_mark_count++;
          TEST_SYNC_POINT("DBImpl:ScheduleZNSGC-mark");
        }
      }
    }
    total_new_mark_count += new_mark_count;
    total_old_mark_count += old_mark_count;
    if (new_mark_count > old_mark_count) {
      vstorage->ComputeCompactionScore(*cfd->ioptions(),
                                       *cfd->GetLatestMutableCFOptions());
      if (!cfd->queued_for_compaction()) {
        AddToCompactionQueue(cfd);
        unscheduled_compactions_++;
      }
    }
  }
  if (unscheduled_compactions_ > 0) {
    MaybeScheduleFlushOrCompaction();
  }
  mutex_.Unlock();
  ROCKS_LOG_BUFFER(&log_buffer_info,
                   "ZNS GC :\n"
                   "\t[SSTable]\n"
                   "\tExisted\tNewly\tTotal\n"
                   "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64
                   "\n"
                   "\t[Sizes]\n"
                   "\tFree\tUsed\tReclaim\tTotal in (GB)\n"
                   "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64
                   "\n"
                   "\t[Ratio]\n"
                   "\tFree\tUsed\tTrash\tTarget\n"
                   "\t%.3f\t%.3f\t%.3f\t%.3f\n",
                   total_old_mark_count, total_new_mark_count, total_count,
                   free >> 30, used >> 30, reclaim >> 30, total >> 30, free_r,
                   used_r, trash_r, target_r);
  log_buffer_info.FlushBufferToLog();
  log_buffer_debug.FlushBufferToLog();
}
#endif

void DBImpl::DumpStats() {
  TEST_SYNC_POINT("DBImpl::DumpStats:1");
#ifndef ROCKSDB_LITE
  const DBPropertyInfo* cf_property_info =
      GetPropertyInfo(DB::Properties::kCFStats);
  assert(cf_property_info != nullptr);
  const DBPropertyInfo* db_property_info =
      GetPropertyInfo(DB::Properties::kDBStats);
  assert(db_property_info != nullptr);

  if (shutdown_initiated_) {
    return;
  }
  std::vector<ColumnFamilyData*> cfd_vec;
  std::string stats;
  TEST_SYNC_POINT("DBImpl::DumpStats:StartRunning");
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->initialized()) {
        cfd->Ref();
        cfd_vec.emplace_back(cfd);
      }
    }
    default_cf_internal_stats_->GetStringProperty(
        *db_property_info, DB::Properties::kDBStats, &stats);
  }
  for (size_t i = 0; i < cfd_vec.size(); ++i) {
    stats.reserve(i == 0 ? 4096 : stats.size() / i * cfd_vec.size() + 4096);
    // Reduce foreground latency
    env_->SleepForMicroseconds(kDumpStatsWaitMicroseconds);
    InstrumentedMutexLock l(&mutex_);
    auto cfd = cfd_vec[i];
    if (cfd->initialized()) {
      cfd->internal_stats()->GetStringProperty(
          *cf_property_info, DB::Properties::kCFStatsNoFileHistogram, &stats);
      cfd->internal_stats()->GetStringProperty(
          *cf_property_info, DB::Properties::kCFFileHistogram, &stats);
    }
  }
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto cfd : cfd_vec) {
      if (cfd->Unref()) {
        delete cfd;
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::DumpStats:2");
  ROCKS_LOG_WARN(immutable_db_options_.info_log,
                 "------- DUMPING STATS -------");
  ROCKS_LOG_WARN(immutable_db_options_.info_log, "%s", stats.c_str());
  if (immutable_db_options_.dump_malloc_stats) {
    stats.clear();
    DumpMallocStats(&stats);
    if (!stats.empty()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "------- Malloc STATS -------");
      ROCKS_LOG_WARN(immutable_db_options_.info_log, "%s", stats.c_str());
    }
  }
#endif  // !ROCKSDB_LITE

  PrintStatistics();
}

void DBImpl::ScheduleBgFree(JobContext* job_context, SuperVersion* sv) {
  mutex_.AssertHeld();
  bool schedule = false;
  if (job_context != nullptr && !job_context->logs_to_free.empty()) {
    for (auto l : job_context->logs_to_free) {
      AddToLogsToFreeQueue(l);
    }
    job_context->logs_to_free.clear();
    schedule = true;
  }
  if (sv != nullptr) {
    AddToSuperVersionToFreeQueue(sv);
    schedule = true;
  }
  if (schedule) {
    SchedulePurge();
  }
}
void DBImpl::FlushInfoLog() {
  if (shutdown_initiated_) {
    return;
  }
  TEST_SYNC_POINT("DBImpl::FlushInfoLog:StartRunning");
  LogFlush(immutable_db_options_.info_log);
}

Directory* DBImpl::GetDataDir(ColumnFamilyData* cfd, size_t path_id) const {
  assert(cfd);
  Directory* ret_dir = cfd->GetDataDir(path_id);
  if (ret_dir == nullptr) {
    return directories_.GetDataDir(path_id);
  }
  return ret_dir;
}

Directory* DBImpl::Directories::GetDataDir(size_t path_id) const {
  assert(path_id < data_dirs_.size());
  Directory* ret_dir = data_dirs_[path_id].get();
  if (ret_dir == nullptr) {
    // Should use db_dir_
    return db_dir_.get();
  }
  return ret_dir;
}

Status DBImpl::SetOptions(
    ColumnFamilyHandle* column_family,
    const std::unordered_map<std::string, std::string>& options_map) {
#ifdef ROCKSDB_LITE
  (void)column_family;
  (void)options_map;
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (options_map.empty()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "SetOptions() on column family [%s], empty input",
                   cfd->GetName().c_str());
    return Status::InvalidArgument("empty input");
  }

  MutableCFOptions new_options;
  Status s;
  Status persist_options_status;
  SuperVersionContext sv_context(/* create_superversion */ true);
  {
    InstrumentedMutexLock l(&mutex_);
    s = cfd->SetOptions(immutable_db_options_, options_map);
    if (s.ok()) {
      new_options = *cfd->GetLatestMutableCFOptions();
      // Append new version to recompute compaction score.
      VersionEdit dummy_edit;
      versions_->LogAndApply(cfd, new_options, &dummy_edit, &mutex_,
                             directories_.GetDbDir());
      // Trigger possible flush/compactions. This has to be before we persist
      // options to file, otherwise there will be a deadlock with writer
      // thread.
      InstallSuperVersionAndScheduleWork(cfd, &sv_context, new_options);

      persist_options_status = WriteOptionsFile(
          false /*need_mutex_lock*/, true /*need_enter_write_thread*/);
      bg_cv_.SignalAll();
    }
  }
  sv_context.Clean();

  ROCKS_LOG_INFO(
      immutable_db_options_.info_log,
      "SetOptions() on column family [%s], inputs:", cfd->GetName().c_str());
  for (const auto& o : options_map) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s: %s\n", o.first.c_str(),
                   o.second.c_str());
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[%s] SetOptions() succeeded", cfd->GetName().c_str());
    new_options.Dump(immutable_db_options_.info_log.get());
    if (!persist_options_status.ok()) {
      s = persist_options_status;
    }
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "[%s] SetOptions() failed",
                   cfd->GetName().c_str());
  }
  LogFlush(immutable_db_options_.info_log);
  return s;
#endif  // ROCKSDB_LITE
}

Status DBImpl::SetDBOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
#ifdef ROCKSDB_LITE
  (void)options_map;
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  if (options_map.empty()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "SetDBOptions(), empty input.");
    return Status::InvalidArgument("empty input");
  }

  MutableDBOptions new_options;
  Status s;
  Status persist_options_status;
  bool wal_changed = false;
  {
    WriteContext write_context(immutable_db_options_.info_log.get());
    InstrumentedMutexLock l(&mutex_);
    s = GetMutableDBOptionsFromStrings(mutable_db_options_, options_map,
                                       &new_options);
    if (s.ok()) {
      auto bg_job_limits = DBImpl::GetBGJobLimits(
          immutable_db_options_.max_background_flushes,
          new_options.max_background_compactions,
          new_options.max_background_garbage_collections,
          new_options.max_background_jobs, true /* parallelize_compactions */);
      if (bg_job_limits.max_compactions >
              env_->GetBackgroundThreads(Env::Priority::LOW) ||
          bg_job_limits.max_flushes >
              env_->GetBackgroundThreads(Env::Priority::HIGH)) {
        env_->IncBackgroundThreadsIfNeeded(bg_job_limits.max_compactions,
                                           Env::Priority::LOW);
        env_->IncBackgroundThreadsIfNeeded(bg_job_limits.max_flushes,
                                           Env::Priority::HIGH);
        MaybeScheduleFlushOrCompaction();
      }
      if (new_options.stats_dump_period_sec !=
              mutable_db_options_.stats_dump_period_sec ||
          new_options.stats_persist_period_sec !=
              mutable_db_options_.stats_persist_period_sec) {
        mutex_.Unlock();
        periodic_work_scheduler_->Unregister(this);
        periodic_work_scheduler_->Register(
            this, new_options.stats_dump_period_sec,
            new_options.stats_persist_period_sec);
        mutex_.Lock();
      }
      write_controller_.set_max_delayed_write_rate(
          new_options.delayed_write_rate);
      table_cache_.get()->SetCapacity(new_options.max_open_files == -1
                                          ? TableCache::kInfiniteCapacity
                                          : new_options.max_open_files - 10);
      wal_changed = mutable_db_options_.wal_bytes_per_sync !=
                    new_options.wal_bytes_per_sync;
      if (new_options.bytes_per_sync == 0) {
        new_options.bytes_per_sync = 1024 * 1024;
      }
      mutable_db_options_ = new_options;
      env_options_for_compaction_ = EnvOptions(
          BuildDBOptions(immutable_db_options_, mutable_db_options_));
      env_options_for_compaction_ = env_->OptimizeForCompactionTableWrite(
          env_options_for_compaction_, immutable_db_options_);
      versions_->ChangeEnvOptions(mutable_db_options_);
      env_options_for_compaction_ = env_->OptimizeForCompactionTableRead(
          env_options_for_compaction_, immutable_db_options_);
      env_options_for_compaction_.compaction_readahead_size =
          mutable_db_options_.compaction_readahead_size;
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      if (alive_log_files_.back().size > GetMaxWalSize() ||
          total_log_size_ > GetMaxTotalWalSize() || wal_changed) {
        Status purge_wal_status = SwitchWAL(&write_context);
        if (!purge_wal_status.ok()) {
          ROCKS_LOG_WARN(immutable_db_options_.info_log,
                         "Unable to purge WAL files in SetDBOptions() -- %s",
                         purge_wal_status.ToString().c_str());
        }
      }
      persist_options_status = WriteOptionsFile(
          false /*need_mutex_lock*/, false /*need_enter_write_thread*/);
      write_thread_.ExitUnbatched(&w);
    }
  }
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "SetDBOptions(), inputs:");
  for (const auto& o : options_map) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s: %s\n", o.first.c_str(),
                   o.second.c_str());
  }
  if (s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "SetDBOptions() succeeded");
    new_options.Dump(immutable_db_options_.info_log.get());
    if (!persist_options_status.ok()) {
      if (immutable_db_options_.fail_if_options_file_error) {
        s = Status::IOError(
            "SetDBOptions() succeeded, but unable to persist options",
            persist_options_status.ToString());
      }
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Unable to persist options in SetDBOptions() -- %s",
                     persist_options_status.ToString().c_str());
    }
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "SetDBOptions failed");
  }
  LogFlush(immutable_db_options_.info_log);
  return s;
#endif  // ROCKSDB_LITE
}

// return the same level if it cannot be moved
int DBImpl::FindMinimumEmptyLevelFitting(
    ColumnFamilyData* cfd, const MutableCFOptions& /*mutable_cf_options*/,
    int level) {
  mutex_.AssertHeld();
  const auto* vstorage = cfd->current()->storage_info();
  int minimum_level = level;
  for (int i = level - 1; i > 0; --i) {
    // stop if level i is not empty
    if (vstorage->NumLevelFiles(i) > 0) break;
    // stop if level i is too small (cannot fit the level files)
    if (vstorage->MaxBytesForLevel(i) < vstorage->NumLevelBytes(level)) {
      break;
    }

    minimum_level = i;
  }
  return minimum_level;
}

Status DBImpl::FlushWAL(bool sync) {
  if (manual_wal_flush_) {
    // We need to lock log_write_mutex_ since logs_ might change concurrently
    InstrumentedMutexLock wl(&log_write_mutex_);
    log::Writer* cur_log_writer = logs_.back().writer;
    auto s = cur_log_writer->WriteBuffer();
    if (!s.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log, "WAL flush error %s",
                      s.ToString().c_str());
      // In case there is a fs error we should set it globally to prevent the
      // future writes
      WriteStatusCheck(s);
      // whether sync or not, we should abort the rest of function upon error
      return s;
    }
    if (!sync) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "FlushWAL sync=false");
      return s;
    }
  }
  if (!sync) {
    return Status::OK();
  }
  // sync = true
  ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "FlushWAL sync=true");
  return SyncWAL();
}

Status DBImpl::SyncWAL() {
  autovector<log::Writer*, 1> logs_to_sync;
  bool need_log_dir_sync;
  uint64_t current_log_number;

  {
    InstrumentedMutexLock l(&mutex_);
    assert(!logs_.empty());

    // This SyncWAL() call only cares about logs up to this number.
    current_log_number = logfile_number_;

    while (logs_.front().number <= current_log_number &&
           logs_.front().getting_synced) {
      log_sync_cv_.Wait();
    }
    // First check that logs are safe to sync in background.
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= current_log_number; ++it) {
      if (!it->writer->file()->writable_file()->IsSyncThreadSafe()) {
        return Status::NotSupported(
            "SyncWAL() is not supported for this implementation of WAL file",
            immutable_db_options_.allow_mmap_writes
                ? "try setting Options::allow_mmap_writes to false"
                : Slice());
      }
    }
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= current_log_number; ++it) {
      auto& log = *it;
      assert(!log.getting_synced);
      log.getting_synced = true;
      logs_to_sync.push_back(log.writer);
    }

    need_log_dir_sync = !log_dir_synced_;
  }

  TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:1");
  RecordTick(stats_, WAL_FILE_SYNCED);
  Status status;
  for (log::Writer* log : logs_to_sync) {
    status = log->file()->SyncWithoutFlush(immutable_db_options_.use_fsync);
    if (!status.ok()) {
      break;
    }
  }
  if (status.ok() && need_log_dir_sync) {
    status = directories_.GetWalDir()->Fsync();
  }
  TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:2");

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:1");
  {
    InstrumentedMutexLock l(&mutex_);
    MarkLogsSynced(current_log_number, need_log_dir_sync, status);
  }
  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:2");

  return status;
}

Status DBImpl::LockWAL() {
  log_write_mutex_.Lock();
  auto cur_log_writer = logs_.back().writer;
  auto status = cur_log_writer->WriteBuffer();
  if (!status.ok()) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log, "WAL flush error %s",
                    status.ToString().c_str());
    // In case there is a fs error we should set it globally to prevent the
    // future writes
    WriteStatusCheck(status);
  }
  return status;
}

Status DBImpl::UnlockWAL() {
  log_write_mutex_.Unlock();
  return Status::OK();
}

void DBImpl::MarkLogsSynced(uint64_t up_to, bool synced_dir,
                            const Status& status) {
  mutex_.AssertHeld();
  if (synced_dir && logfile_number_ == up_to && status.ok()) {
    log_dir_synced_ = true;
  }
  for (auto it = logs_.begin(); it != logs_.end() && it->number <= up_to;) {
    auto& log = *it;
    assert(log.getting_synced);
    if (status.ok() && logs_.size() > 1) {
      logs_to_free_.push_back(log.ReleaseWriter());
      // To modify logs_ both mutex_ and log_write_mutex_ must be held
      InstrumentedMutexLock l(&log_write_mutex_);
      it = logs_.erase(it);
    } else {
      log.getting_synced = false;
      ++it;
    }
  }
  assert(!status.ok() || logs_.empty() || logs_[0].number > up_to ||
         (logs_.size() == 1 && !logs_[0].getting_synced));
  log_sync_cv_.SignalAll();
}

SequenceNumber DBImpl::GetLatestSequenceNumber() const {
  return versions_->LastSequence();
}

void DBImpl::SetLastPublishedSequence(SequenceNumber seq) {
  versions_->SetLastPublishedSequence(seq);
}

bool DBImpl::SetPreserveDeletesSequenceNumber(SequenceNumber seqnum) {
  if (seqnum > preserve_deletes_seqnum_.load()) {
    preserve_deletes_seqnum_.store(seqnum);
    return true;
  } else {
    return false;
  }
}

InternalIterator* DBImpl::NewInternalIterator(
    Arena* arena, RangeDelAggregator* range_del_agg, SequenceNumber sequence,
    ColumnFamilyHandle* column_family, SeparateHelper** separate_helper) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    cfd = cfh->cfd();
  }

  mutex_.Lock();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  mutex_.Unlock();
  ReadOptions roptions;
  if (separate_helper != nullptr) {
    *separate_helper = super_version->current;
  }
  return NewInternalIterator(roptions, cfd, super_version, arena, range_del_agg,
                             sequence);
}

void DBImpl::SchedulePurge() {
  mutex_.AssertHeld();
  assert(opened_successfully_);

  // Purge operations are put into High priority queue
  bg_purge_scheduled_++;
  env_->Schedule(&DBImpl::BGWorkPurge, this, Env::Priority::HIGH, nullptr);
}

void DBImpl::BackgroundCallPurge() {
  mutex_.Lock();

  // We use one single loop to clear both queues so that after existing the loop
  // both queues are empty. This is stricter than what is needed, but can make
  // it easier for us to reason the correctness.
  while (!purge_queue_.empty() | !superversion_to_free_queue_.empty() |
         !logs_to_free_queue_.empty()) {
    if (!superversion_to_free_queue_.empty()) {
      SuperVersion* sv = *superversion_to_free_queue_.begin();
      superversion_to_free_queue_.pop_front();
      mutex_.Unlock();
      delete sv;
      mutex_.Lock();
    }
    if (!logs_to_free_queue_.empty()) {
      log::Writer* log_writer = *logs_to_free_queue_.begin();
      logs_to_free_queue_.pop_front();
      mutex_.Unlock();
      delete log_writer;
      mutex_.Lock();
    }
    if (!purge_queue_.empty()) {
      auto purge_file = purge_queue_.begin();
      auto fname = purge_file->fname;
      auto dir_to_sync = purge_file->dir_to_sync;
      auto type = purge_file->type;
      auto number = purge_file->number;
      auto job_id = purge_file->job_id;
      purge_queue_.pop_front();

      mutex_.Unlock();
      DeleteObsoleteFileImpl(job_id, fname, dir_to_sync, type, number);
#ifndef ROCKSDB_LITE
      wal_manager_.PurgeObsoleteWALFiles();
#endif  // ROCKSDB_LITE
      mutex_.Lock();
    }
  }
  bg_purge_scheduled_--;

  bg_cv_.SignalAll();
  // IMPORTANT:there should be no code after calling SignalAll. This call may
  // signal the DB destructor that it's OK to proceed with destruction. In
  // that case, all DB variables will be dealloacated and referencing them
  // will cause trouble.
  mutex_.Unlock();
}

namespace {
struct IterState {
  IterState(DBImpl* _db, InstrumentedMutex* _mu, SuperVersion* _super_version,
            bool _background_purge)
      : db(_db),
        mu(_mu),
        super_version(_super_version),
        background_purge(_background_purge) {}

  DBImpl* db;
  InstrumentedMutex* mu;
  SuperVersion* super_version;
  bool background_purge;
};

static void CleanupIteratorState(void* arg1, void* /*arg2*/) {
  IterState* state = reinterpret_cast<IterState*>(arg1);

  if (state->super_version->Unref()) {
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    JobContext job_context(0);

    state->mu->Lock();
    state->super_version->Cleanup();
    state->db->FindObsoleteFiles(&job_context, false, true);
    if (state->background_purge) {
      state->db->ScheduleBgFree(&job_context, state->super_version);
      state->super_version = nullptr;
    }
    state->mu->Unlock();

    delete state->super_version;
    if (job_context.HaveSomethingToDelete()) {
      if (state->background_purge) {
        // PurgeObsoleteFiles here does not delete files. Instead, it adds the
        // files to be deleted to a job queue, and deletes it in a separate
        // background thread.
        state->db->PurgeObsoleteFiles(job_context, true /* schedule only */);
        state->mu->Lock();
        state->db->SchedulePurge();
        state->mu->Unlock();
      } else {
        state->db->PurgeObsoleteFiles(job_context);
      }
    }
    job_context.Clean(state->mu);
  }

  delete state;
}
}  // namespace

InternalIterator* DBImpl::NewInternalIterator(
    const ReadOptions& read_options, ColumnFamilyData* cfd,
    SuperVersion* super_version, Arena* arena,
    RangeDelAggregator* range_del_agg, SequenceNumber sequence,
    SeparateHelper** separate_helper) {
  InternalIterator* internal_iter;
  assert(arena != nullptr);
  assert(range_del_agg != nullptr);
  // Need to create internal iterator from the arena.
  MergeIteratorBuilder merge_iter_builder(
      &cfd->internal_comparator(), arena,
      !read_options.total_order_seek &&
          super_version->mutable_cf_options.prefix_extractor != nullptr);
  // Collect iterator for mutable mem
  merge_iter_builder.AddIterator(
      super_version->mem->NewIterator(read_options, arena));
  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter;
  if (!read_options.ignore_range_deletions) {
    range_del_iter.reset(
        super_version->mem->NewRangeTombstoneIterator(read_options, sequence));
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }
  // Collect all needed child iterators for immutable memtables
  Status s;
  super_version->imm->AddIterators(read_options, &merge_iter_builder);
  if (!read_options.ignore_range_deletions) {
    s = super_version->imm->AddRangeTombstoneIterators(read_options, arena,
                                                       range_del_agg);
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::NewInternalIterator:StatusCallback", &s);
  if (s.ok()) {
    // Collect iterators for files in L0 - Ln
    if (read_options.read_tier != kMemtableTier) {
      super_version->current->AddIterators(read_options, env_options_,
                                           &merge_iter_builder, range_del_agg);
    }
    internal_iter = merge_iter_builder.Finish();
    bool background_purge = read_options.background_purge_on_iterator_cleanup ||
                            immutable_db_options_.avoid_unnecessary_blocking_io;
    IterState* cleanup =
        new IterState(this, &mutex_, super_version, background_purge);
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);
    if (separate_helper != nullptr) {
      *separate_helper = super_version->current;
    }
    return internal_iter;
  } else {
    CleanupSuperVersion(super_version);
  }
  return NewErrorInternalIterator<LazyBuffer>(s, arena);
}

ColumnFamilyHandle* DBImpl::DefaultColumnFamily() const {
  return default_cf_handle_;
}

ColumnFamilyHandle* DBImpl::PersistentStatsColumnFamily() const {
  return persist_stats_cf_handle_;
}

Status DBImpl::Get(const ReadOptions& read_options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   LazyBuffer* value) {
  auto s = GetImpl(read_options, column_family, key, value);
  assert(!s.ok() || value == nullptr || value->valid());
  return s;
}

Status DBImpl::GetImpl(const ReadOptions& read_options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       LazyBuffer* lazy_val, bool* value_found,
                       ReadCallback* callback) {
  LatencyHistGuard guard(&read_latency_reporter_);
  read_qps_reporter_.AddCount(1);

  StopWatch sw(env_, stats_, DB_GET);
  PERF_TIMER_GUARD(get_snapshot_time);

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  if (tracer_) {
    // TODO: This mutex should be removed later, to improve performance when
    // tracing is enabled.
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      tracer_->Get(column_family, key);
    }
  }

  // Acquire SuperVersion
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  TEST_SYNC_POINT("DBImpl::GetImpl:1");
  TEST_SYNC_POINT("DBImpl::GetImpl:2");

  SequenceNumber snapshot;
  if (read_options.snapshot != nullptr) {
    // Note: In WritePrepared txns this is not necessary but not harmful
    // either.  Because prep_seq > snapshot => commit_seq > snapshot so if
    // a snapshot is specified we should be fine with skipping seq numbers
    // that are greater than that.
    //
    // In WriteUnprepared, we cannot set snapshot in the lookup key because we
    // may skip uncommitted data that should be visible to the transaction for
    // reading own writes.
    snapshot =
        reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
    if (callback) {
      snapshot = std::max(snapshot, callback->MaxUnpreparedSequenceNumber());
    }
  } else {
    // Since we get and reference the super version before getting
    // the snapshot number, without a mutex protection, it is possible
    // that a memtable switch happened in the middle and not all the
    // data for this snapshot is available. But it will contain all
    // the data available in the super version we have, which is also
    // a valid snapshot to read from.
    // We shouldn't get snapshot before finding and referencing the super
    // version because a flush happening in between may compact away data for
    // the snapshot, but the snapshot is earlier than the data overwriting it,
    // so users may see wrong results.
    snapshot = last_seq_same_as_publish_seq_
                   ? versions_->LastSequence()
                   : versions_->LastPublishedSequence();
  }
  TEST_SYNC_POINT("DBImpl::GetImpl:3");
  TEST_SYNC_POINT("DBImpl::GetImpl:4");

  // Prepare to store a list of merge operations if merge occurs.
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;

  Status s;
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  LookupKey lkey(key, snapshot);
  PERF_TIMER_STOP(get_snapshot_time);

  bool skip_memtable = (read_options.read_tier == kPersistedTier &&
                        has_unpersisted_data_.load(std::memory_order_relaxed));
  bool done = false;
  if (!skip_memtable) {
    if (sv->mem->Get(lkey, lazy_val, &s, &merge_context,
                     &max_covering_tombstone_seq, read_options, callback)) {
      done = true;
      RecordTick(stats_, MEMTABLE_HIT);
    } else if ((s.ok() || s.IsMergeInProgress()) &&
               sv->imm->Get(lkey, lazy_val, &s, &merge_context,
                            &max_covering_tombstone_seq, read_options,
                            callback)) {
      done = true;
      RecordTick(stats_, MEMTABLE_HIT);
    }
    if (!done && !s.ok() && !s.IsMergeInProgress()) {
      ReturnAndCleanupSuperVersion(cfd, sv);
      return s;
    }
  }
  if (!done) {
    PERF_TIMER_GUARD(get_from_output_files_time);
    sv->current->Get(read_options, key, lkey, lazy_val, &s, &merge_context,
                     &max_covering_tombstone_seq, value_found, nullptr, nullptr,
                     callback);
    RecordTick(stats_, MEMTABLE_MISS);
  }

  if (s.ok() && lazy_val != nullptr) {
    lazy_val->pin(LazyBufferPinLevel::DB);
    s = lazy_val->fetch();
  }

  {
    PERF_TIMER_GUARD(get_post_process_time);

    ReturnAndCleanupSuperVersion(cfd, sv);

    RecordTick(stats_, NUMBER_KEYS_READ);
    // size_t size = lazy_val->size();
    // RecordTick(stats_, BYTES_READ, size);
    // MeasureTime(stats_, BYTES_PER_READ, size);
    // PERF_COUNTER_ADD(get_read_bytes, size);
  }
  return s;
}

#ifdef WITH_BOOSTLIB
struct SimpleFiberTls {
  static constexpr intptr_t MAX_QUEUE_LEN = 256;
  static constexpr intptr_t DEFAULT_FIBER_CNT = 8;
  typedef std::function<void()> task_t;
  intptr_t fiber_count = 0;
  intptr_t pending_count = 0;
  terark::FiberYield m_fy;
  boost::fibers::buffered_channel<task_t> channel;

  SimpleFiberTls(boost::fibers::context** activepp)
      : m_fy(activepp), channel(MAX_QUEUE_LEN) {
    update_fiber_count(DEFAULT_FIBER_CNT);
  }

  void update_fiber_count(intptr_t count) {
    if (count <= 0) {
      return;
    }
    count = std::min<intptr_t>(count, +MAX_QUEUE_LEN);
    using boost::fibers::channel_op_status;
    for (intptr_t i = fiber_count; i < count; ++i) {
      boost::fibers::fiber([this, i]() {
        task_t task;
        while (i < fiber_count &&
               channel.pop(task) == channel_op_status::success) {
          task();
          pending_count--;
        }
      }).detach();
    }
    fiber_count = count;
  }

  void push(task_t&& task) {
    channel.push(std::move(task));
    pending_count++;
  }

  bool try_push(const task_t& task) {
    using boost::fibers::channel_op_status;
    if (channel.try_push(task) == channel_op_status::success) {
      pending_count++;
      return true;
    }
    return false;
  }

  int wait(int timeout_us) {
    intptr_t old_pending_count = pending_count;
    if (old_pending_count == 0) {
      return 0;
    }

    using namespace std::chrono;

    // do not use sleep_for, because we want to return as soon as possible
    // boost::this_fiber::sleep_for(microseconds(timeout_us));
    // return tls->pending_count - old_pending_count;

    auto start = std::chrono::system_clock::now();
    while (true) {
      // boost::this_fiber::yield(); // wait once
      m_fy.unchecked_yield();
      if (pending_count > 0) {
        auto now = system_clock::now();
        auto dur = duration_cast<microseconds>(now - start).count();
        if (dur >= timeout_us) {
          return int(pending_count - old_pending_count - 1);  // negtive
        }
      } else {
        break;
      }
    }
    return int(old_pending_count);
  }

  int wait() {
    intptr_t cnt = pending_count;
    while (pending_count > 0) {
      // boost::this_fiber::yield(); // wait once
      m_fy.unchecked_yield();
    }
    return int(cnt);
  }
};

// ensure fiber thread locals are constructed first
// because SimpleFiberTls.channel must be destructed first
static thread_local SimpleFiberTls gt_fibers(
    boost::fibers::context::active_pp());
#endif  // WITH_BOOSTLIB

std::vector<Status> DBImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  LatencyHistGuard guard(&read_latency_reporter_);
  read_qps_reporter_.AddCount(keys.size());
  StopWatch sw(env_, stats_, DB_MULTIGET);
  PERF_TIMER_GUARD(get_snapshot_time);

  SequenceNumber snapshot;

  struct MultiGetColumnFamilyData {
    ColumnFamilyData* cfd;
    SuperVersion* super_version;
  };
  std::unordered_map<uint32_t, MultiGetColumnFamilyData*> multiget_cf_data;
  // fill up and allocate outside of mutex
  for (auto cf : column_family) {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(cf);
    auto cfd = cfh->cfd();
    if (multiget_cf_data.find(cfd->GetID()) == multiget_cf_data.end()) {
      auto mgcfd = new MultiGetColumnFamilyData();
      mgcfd->cfd = cfd;
      multiget_cf_data.insert({cfd->GetID(), mgcfd});
    }
  }

  mutex_.Lock();
  if (read_options.snapshot != nullptr) {
    snapshot =
        reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
  } else {
    snapshot = last_seq_same_as_publish_seq_
                   ? versions_->LastSequence()
                   : versions_->LastPublishedSequence();
  }
  for (auto mgd_iter : multiget_cf_data) {
    mgd_iter.second->super_version =
        mgd_iter.second->cfd->GetSuperVersion()->Ref();
  }
  mutex_.Unlock();

  // Note: this always resizes the values array
  size_t num_keys = keys.size();
  std::vector<Status> stat_list(num_keys);
  values->resize(num_keys);

  // Keep track of bytes that we read for statistics-recording later
  uint64_t bytes_read = 0;
  PERF_TIMER_STOP(get_snapshot_time);

  // For each of the given keys, apply the entire "get" process as follows:
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  size_t num_found = 0;
  size_t counting = num_keys;
  auto get_one = [&](size_t i) {
    // Contain a list of merge operations if merge occurs.
    MergeContext merge_context;
    Status& s = stat_list[i];
    std::string* value = &(*values)[i];
    LazyBuffer lazy_val(value);

    LookupKey lkey(keys[i], snapshot);
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family[i]);
    SequenceNumber max_covering_tombstone_seq = 0;
    auto mgd_iter = multiget_cf_data.find(cfh->cfd()->GetID());
    assert(mgd_iter != multiget_cf_data.end());
    auto mgd = mgd_iter->second;
    auto super_version = mgd->super_version;
    bool skip_memtable =
        (read_options.read_tier == kPersistedTier &&
         has_unpersisted_data_.load(std::memory_order_relaxed));
    bool done = false;
    if (!skip_memtable) {
      if (super_version->mem->Get(lkey, &lazy_val, &s, &merge_context,
                                  &max_covering_tombstone_seq, read_options)) {
        done = true;
        RecordTick(stats_, MEMTABLE_HIT);
      } else if (super_version->imm->Get(lkey, &lazy_val, &s, &merge_context,
                                         &max_covering_tombstone_seq,
                                         read_options)) {
        done = true;
        RecordTick(stats_, MEMTABLE_HIT);
      }
    }
    if (!done) {
      PERF_TIMER_GUARD(get_from_output_files_time);
      super_version->current->Get(read_options, keys[i], lkey, &lazy_val, &s,
                                  &merge_context, &max_covering_tombstone_seq);
      RecordTick(stats_, MEMTABLE_MISS);
    }
    if (s.ok()) {
      s = std::move(lazy_val).dump(value);
    }
    if (s.ok()) {
      bytes_read += value->size();
      num_found++;
    }
    counting--;
  };
#ifdef WITH_BOOSTLIB
  if (read_options.aio_concurrency && immutable_db_options_.use_aio_reads) {
#if 0
    static thread_local terark::RunOnceFiberPool fiber_pool(16);
    // current calling fiber's list head, can be treated as a handle
    int myhead = -1; // must be initialized to -1
    for (size_t i = 0; i < num_keys; ++i) {
      fiber_pool.submit(myhead, get_one, i);
    }
    fiber_pool.reap(myhead);
    assert(0 == counting);
#else
    auto tls = &gt_fibers;
    tls->update_fiber_count(read_options.aio_concurrency);
    for (size_t i = 0; i < num_keys; ++i) {
      tls->push([&, i]() { get_one(i); });
    }
    while (counting) {
      // boost::this_fiber::yield();
      tls->m_fy.unchecked_yield();
    }
#endif
  } else {
#endif
    for (size_t i = 0; i < num_keys; ++i) {
      get_one(i);
    }
#ifdef WITH_BOOSTLIB
  }
#endif

  // Post processing (decrement reference counts and record statistics)
  PERF_TIMER_GUARD(get_post_process_time);
  autovector<SuperVersion*> superversions_to_delete;

  // TODO(icanadi) do we need lock here or just around Cleanup()?
  mutex_.Lock();
  for (auto mgd_iter : multiget_cf_data) {
    auto mgd = mgd_iter.second;
    if (mgd->super_version->Unref()) {
      mgd->super_version->Cleanup();
      superversions_to_delete.push_back(mgd->super_version);
    }
  }
  mutex_.Unlock();

  for (auto td : superversions_to_delete) {
    delete td;
  }
  for (auto mgd : multiget_cf_data) {
    delete mgd.second;
  }

  RecordTick(stats_, NUMBER_MULTIGET_CALLS);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_READ, num_keys);
  RecordTick(stats_, NUMBER_MULTIGET_KEYS_FOUND, num_found);
  RecordTick(stats_, NUMBER_MULTIGET_BYTES_READ, bytes_read);
  MeasureTime(stats_, BYTES_PER_MULTIGET, bytes_read);
  PERF_COUNTER_ADD(multiget_read_bytes, bytes_read);
  PERF_TIMER_STOP(get_post_process_time);

  return stat_list;
}  // namespace TERARKDB_NAMESPACE

Status DBImpl::CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                                  const std::string& column_family,
                                  ColumnFamilyHandle** handle) {
  assert(handle != nullptr);
  Status s =
      CreateColumnFamilyImpl({&cf_options}, {&column_family}, {handle}).front();
  if (s.ok()) {
    s = WriteOptionsFile(true /*need_mutex_lock*/,
                         true /*need_enter_write_thread*/);
  }
  return s;
}

Status DBImpl::CreateColumnFamilies(
    const ColumnFamilyOptions& cf_options,
    const std::vector<std::string>& column_family_names,
    std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  handles->clear();
  size_t num_cf = column_family_names.size();
  autovector<const ColumnFamilyOptions*> cf_options_list;
  autovector<const std::string*> column_family_name_list;
  autovector<ColumnFamilyHandle**> handle_list;
  handles->resize(num_cf);
  for (size_t i = 0; i < num_cf; i++) {
    cf_options_list.emplace_back(&cf_options);
    column_family_name_list.emplace_back(&column_family_names[i]);
    handle_list.emplace_back(&(*handles)[i]);
  }
  autovector<Status> s_list = CreateColumnFamilyImpl(
      cf_options_list, column_family_name_list, handle_list);
  int success_count = std::count_if(s_list.begin(), s_list.end(),
                                    [](const Status& s) { return s.ok(); });
  Status s;
  if (success_count > 0) {
    Status persist_options_status = WriteOptionsFile(
        true /*need_mutex_lock*/, true /*need_enter_write_thread*/);
    if (success_count == num_cf && !persist_options_status.ok()) {
      s = persist_options_status;
    }
  }
  if (success_count == num_cf) {
    return s;
  } else {
    handles->resize(
        std::remove_if(handles->begin(), handles->end(),
                       [](ColumnFamilyHandle* h) { return h == nullptr; }) -
        handles->begin());
    return *std::find_if(s_list.begin(), s_list.end(),
                         [](const Status& s) { return !s.ok(); });
  }
}

Status DBImpl::CreateColumnFamilies(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  handles->clear();
  size_t num_cf = column_families.size();
  autovector<const ColumnFamilyOptions*> cf_options_list;
  autovector<const std::string*> column_family_name_list;
  autovector<ColumnFamilyHandle**> handle_list;
  handles->resize(num_cf);
  for (size_t i = 0; i < num_cf; i++) {
    cf_options_list.emplace_back(&column_families[i].options);
    column_family_name_list.emplace_back(&column_families[i].name);
    handle_list.emplace_back(&(*handles)[i]);
  }
  autovector<Status> s_list = CreateColumnFamilyImpl(
      cf_options_list, column_family_name_list, handle_list);
  int success_count = std::count_if(s_list.begin(), s_list.end(),
                                    [](const Status& s) { return s.ok(); });
  Status s;
  if (success_count > 0) {
    Status persist_options_status = WriteOptionsFile(
        true /*need_mutex_lock*/, true /*need_enter_write_thread*/);
    if (success_count == num_cf && !persist_options_status.ok()) {
      s = persist_options_status;
    }
  }
  if (success_count == num_cf) {
    return s;
  } else {
    handles->resize(
        std::remove_if(handles->begin(), handles->end(),
                       [](ColumnFamilyHandle* h) { return h == nullptr; }) -
        handles->begin());
    return *std::find_if(s_list.begin(), s_list.end(),
                         [](const Status& s) { return !s.ok(); });
  }
}

autovector<Status> DBImpl::CreateColumnFamilyImpl(
    autovector<const ColumnFamilyOptions*> cf_options,
    autovector<const std::string*> column_family_name,
    autovector<ColumnFamilyHandle**> handle) {
  assert(cf_options.size() == column_family_name.size());
  assert(cf_options.size() == handle.size());
  autovector<Status> s;
  s.resize(cf_options.size());
  Status persist_options_status;
  for (auto& h : handle) {
    *h = nullptr;
  }
#if !defined(_MSC_VER) && !defined(__APPLE__)
  const char* terarkdb_localTempDir = getenv("TerarkZipTable_localTempDir");
  const char* terarkConfigString = getenv("TerarkConfigString");
  if (terarkdb_localTempDir || terarkConfigString) {
    if (terarkdb_localTempDir &&
        ::access(terarkdb_localTempDir, R_OK | W_OK) != 0) {
      Status check_s = Status::InvalidArgument(
          "Must exists, and Permission ReadWrite is required on "
          "env TerarkZipTable_localTempDir",
          terarkdb_localTempDir);
      for (size_t i = 0; i < cf_options.size(); ++i) {
        s[i] = check_s;
      }
      return s;
    }
#ifdef WITH_TERARK_ZIP
    for (size_t i = 0; i < cf_options.size(); ++i) {
      if (!TerarkZipIsBlackListCF(*column_family_name[i])) {
        TerarkZipCFOptionsFromEnv(
            const_cast<ColumnFamilyOptions&>(*cf_options[i]), dbname_);
      }
    }
#endif
  }
#endif

  size_t ok_count = 0;
  for (size_t i = 0; i < cf_options.size(); ++i) {
    s[i] = CheckCompressionSupported(*cf_options[i]);
    if (s[i].ok() && immutable_db_options_.allow_concurrent_memtable_write) {
      s[i] = CheckConcurrentWritesSupported(*cf_options[i]);
    }
    if (s[i].ok()) {
      s[i] = CheckCFPathsSupported(initial_db_options_, *cf_options[i]);
    }
    if (s[i].ok()) {
      for (auto& cf_path : cf_options[i]->cf_paths) {
        s[i] = env_->CreateDirIfMissing(cf_path.path);
        if (!s[i].ok()) {
          break;
        }
      }
    }
    ok_count += s[i].ok();
  }

  SuperVersionContext sv_context(/* create_superversion */ true);
  if (ok_count > 0) {
    struct CreateCFContext {
      VersionEdit edit;
      MutableCFOptions mopt;
    };
    autovector<CreateCFContext> context_vec;
    autovector<ColumnFamilyData*> cfds;
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    autovector<autovector<VersionEdit*>> edit_lists;
    autovector<const ColumnFamilyOptions*> column_family_options_list;
    context_vec.resize(cf_options.size());

    InstrumentedMutexLock l(&mutex_);

    ok_count = 0;
    for (size_t i = 0; i < cf_options.size(); ++i) {
      if (!s[i].ok()) {
        continue;
      }
      if (versions_->GetColumnFamilySet()->GetColumnFamily(
              *column_family_name[i]) != nullptr) {
        s[i] = Status::InvalidArgument("Column family already exists");
        continue;
      }

      auto& context = context_vec[ok_count];
      VersionEdit& edit = context.edit;
      edit.AddColumnFamily(*column_family_name[i]);
      uint32_t new_id =
          versions_->GetColumnFamilySet()->GetNextColumnFamilyID();
      edit.SetColumnFamily(new_id);
      edit.SetLogNumber(logfile_number_);
      edit.SetComparatorName(cf_options[i]->comparator->Name());
      context.mopt = MutableCFOptions(*cf_options[i], env_);

      cfds.emplace_back(nullptr);
      mutable_cf_options_list.emplace_back(&context.mopt);
      autovector<VersionEdit*> edit_list;
      edit_list.emplace_back(&edit);
      edit_lists.emplace_back(std::move(edit_list));
      column_family_options_list.emplace_back(cf_options[i]);

      ++ok_count;
      assert(ok_count == cfds.size());
      assert(ok_count == mutable_cf_options_list.size());
      assert(ok_count == edit_lists.size());
      assert(ok_count == column_family_options_list.size());
    }
    // LogAndApply will both write the creation in MANIFEST and create
    // ColumnFamilyData object
    if (ok_count > 0) {  // write thread
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      // LogAndApply will both write the creation in MANIFEST and create
      // ColumnFamilyData object
      auto apply_s = versions_->LogAndApply(
          cfds, mutable_cf_options_list, edit_lists, &mutex_,
          directories_.GetDbDir(), false, column_family_options_list);
      write_thread_.ExitUnbatched(&w);
      if (!apply_s.ok()) {
        for (size_t i = 0; i < cf_options.size(); ++i) {
          if (s[i].ok()) {
            s[i] = apply_s;
          }
        }
      }
    }
    for (size_t i = 0; i < cf_options.size(); ++i) {
      if (s[i].ok()) {
        auto* cfd = versions_->GetColumnFamilySet()->GetColumnFamily(
            *column_family_name[i]);
        assert(cfd != nullptr);
        s[i] = cfd->AddDirectories();
      }
      if (s[i].ok()) {
        single_column_family_mode_ = false;
        auto* cfd = versions_->GetColumnFamilySet()->GetColumnFamily(
            *column_family_name[i]);
        assert(cfd != nullptr);
        InstallSuperVersionAndScheduleWork(cfd, &sv_context,
                                           *cfd->GetLatestMutableCFOptions());

        if (!cfd->mem()->IsSnapshotSupported()) {
          is_snapshot_supported_ = false;
        }

        cfd->set_initialized();

        *handle[i] = new ColumnFamilyHandleImpl(cfd, this, &mutex_);
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "Created column family [%s] (ID %u)",
                       column_family_name[i]->c_str(), (unsigned)cfd->GetID());
      } else {
        ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                        "Creating column family [%s] FAILED -- %s",
                        column_family_name[i]->c_str(),
                        s[i].ToString().c_str());
      }
    }
  }  // InstrumentedMutexLock l(&mutex_)

  sv_context.Clean();
  // this is outside the mutex
  for (size_t i = 0; i < cf_options.size(); ++i) {
    if (s[i].ok()) {
      NewThreadStatusCfInfo(
          reinterpret_cast<ColumnFamilyHandleImpl*>(*handle[i])->cfd());
    }
  }
  return s;
}

Status DBImpl::DropColumnFamily(ColumnFamilyHandle* column_family) {
  assert(column_family != nullptr);
  Status s = DropColumnFamilyImpl(column_family);
  if (s.ok()) {
    s = WriteOptionsFile(true /*need_mutex_lock*/,
                         true /*need_enter_write_thread*/);
  }
  return s;
}

Status DBImpl::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& column_families) {
  Status s;
  bool success_once = false;
  for (auto* handle : column_families) {
    s = DropColumnFamilyImpl(handle);
    if (!s.ok()) {
      break;
    }
    success_once = true;
  }
  if (success_once) {
    Status persist_options_status = WriteOptionsFile(
        true /*need_mutex_lock*/, true /*need_enter_write_thread*/);
    if (s.ok() && !persist_options_status.ok()) {
      s = persist_options_status;
    }
  }
  return s;
}

Status DBImpl::DropColumnFamilyImpl(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  if (cfd->GetID() == 0) {
    return Status::InvalidArgument("Can't drop default column family");
  }

  bool cf_support_snapshot = cfd->mem()->IsSnapshotSupported();

  VersionEdit edit;
  edit.DropColumnFamily();
  edit.SetColumnFamily(cfd->GetID());

  Status s;
  {
    InstrumentedMutexLock l(&mutex_);
    if (cfd->IsDropped()) {
      s = Status::InvalidArgument("Column family already dropped!\n");
    }
    if (s.ok()) {
      // we drop column family from a single write thread
      WriteThread::Writer w;
      write_thread_.EnterUnbatched(&w, &mutex_);
      s = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(), &edit,
                                 &mutex_);
      write_thread_.ExitUnbatched(&w);
    }
    if (s.ok()) {
      auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
      max_total_in_memory_state_ -= mutable_cf_options->write_buffer_size *
                                    mutable_cf_options->max_write_buffer_number;
    }

    if (!cf_support_snapshot) {
      // Dropped Column Family doesn't support snapshot. Need to recalculate
      // is_snapshot_supported_.
      bool new_is_snapshot_supported = true;
      for (auto c : *versions_->GetColumnFamilySet()) {
        if (!c->IsDropped() && !c->mem()->IsSnapshotSupported()) {
          new_is_snapshot_supported = false;
          break;
        }
      }
      is_snapshot_supported_ = new_is_snapshot_supported;
    }
    bg_cv_.SignalAll();
  }

  if (s.ok()) {
    // Note that here we erase the associated cf_info of the to-be-dropped
    // cfd before its ref-count goes to zero to avoid having to erase cf_info
    // later inside db_mutex.
    EraseThreadStatusCfInfo(cfd);
    assert(cfd->IsDropped());
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Dropped column family with id %u\n", cfd->GetID());
  } else {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Dropping column family with id %u FAILED -- %s\n",
                    cfd->GetID(), s.ToString().c_str());
  }

  return s;
}

bool DBImpl::KeyMayExist(const ReadOptions& read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         std::string* value, bool* value_found) {
  assert(value != nullptr);
  if (value_found != nullptr) {
    // falsify later if key-may-exist but can't fetch value
    *value_found = true;
  }
  ReadOptions roptions = read_options;
  roptions.read_tier = kBlockCacheTier;  // read from block cache only
  LazyBuffer lazy_val(value);
  auto s = GetImpl(roptions, column_family, key, &lazy_val, value_found);
  if (s.ok()) {
    s = std::move(lazy_val).dump(value);
  }

  // If block_cache is enabled and the index block of the table didn't
  // not present in block_cache, the return value will be Status::Incomplete.
  // In this case, key may still exist in the table.
  return s.ok() || s.IsIncomplete();
}

Iterator* DBImpl::NewIterator(const ReadOptions& read_options,
                              ColumnFamilyHandle* column_family) {
  LatencyHistGuard guard(&newiterator_latency_reporter_);
  newiterator_qps_reporter_.AddCount(1);

  if (read_options.managed) {
    return NewErrorIterator(
        Status::NotSupported("Managed iterator is not supported anymore."));
  }
  Iterator* result = nullptr;
  if (read_options.read_tier == kPersistedTier) {
    return NewErrorIterator(Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators."));
  }
  // if iterator wants internal keys, we can only proceed if
  // we can guarantee the deletes haven't been processed yet
  if (immutable_db_options_.preserve_deletes &&
      read_options.iter_start_seqnum > 0 &&
      read_options.iter_start_seqnum < preserve_deletes_seqnum_.load()) {
    return NewErrorIterator(Status::InvalidArgument(
        "Iterator requested internal keys which are too old and are not"
        " guaranteed to be preserved, try larger iter_start_seqnum opt."));
  }
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  ReadCallback* read_callback = nullptr;  // No read callback provided.
  if (read_options.tailing) {
#ifdef ROCKSDB_LITE
    // not supported in lite version
    result = nullptr;

#else
    SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
    auto iter = new ForwardIterator(this, read_options, cfd, sv);
    result = NewDBIterator(
        env_, read_options, *cfd->ioptions(), sv->mutable_cf_options,
        cfd->user_comparator(), iter, iter, kMaxSequenceNumber, sv->current,
        sv->mutable_cf_options.max_sequential_skip_in_iterations, read_callback,
        this, cfd);
#endif
  } else {
    // Note: no need to consider the special case of
    // last_seq_same_as_publish_seq_==false since NewIterator is overridden in
    // WritePreparedTxnDB
    auto snapshot = read_options.snapshot != nullptr
                        ? read_options.snapshot->GetSequenceNumber()
                        : versions_->LastSequence();
    result = NewIteratorImpl(read_options, cfd, snapshot, read_callback);
  }
  return result;
}

ArenaWrappedDBIter* DBImpl::NewIteratorImpl(const ReadOptions& read_options,
                                            ColumnFamilyData* cfd,
                                            SequenceNumber snapshot,
                                            ReadCallback* read_callback,
                                            bool allow_refresh) {
  SuperVersion* sv = cfd->GetReferencedSuperVersion(this);

  // Try to generate a DB iterator tree in continuous memory area to be
  // cache friendly. Here is an example of result:
  // +-------------------------------+
  // |                               |
  // | ArenaWrappedDBIter            |
  // |  +                            |
  // |  +---> Inner Iterator   ------------+
  // |  |                            |     |
  // |  |    +-- -- -- -- -- -- -- --+     |
  // |  +--- | Arena                 |     |
  // |       |                       |     |
  // |          Allocated Memory:    |     |
  // |       |   +-------------------+     |
  // |       |   | DBIter            | <---+
  // |           |  +                |
  // |       |   |  +-> iter_  ------------+
  // |       |   |                   |     |
  // |       |   +-------------------+     |
  // |       |   | MergingIterator   | <---+
  // |           |  +                |
  // |       |   |  +->child iter1  ------------+
  // |       |   |  |                |          |
  // |           |  +->child iter2  ----------+ |
  // |       |   |  |                |        | |
  // |       |   |  +->child iter3  --------+ | |
  // |           |                   |      | | |
  // |       |   +-------------------+      | | |
  // |       |   | Iterator1         | <--------+
  // |       |   +-------------------+      | |
  // |       |   | Iterator2         | <------+
  // |       |   +-------------------+      |
  // |       |   | Iterator3         | <----+
  // |       |   +-------------------+
  // |       |                       |
  // +-------+-----------------------+
  //
  // ArenaWrappedDBIter inlines an arena area where all the iterators in
  // the iterator tree are allocated in the order of being accessed when
  // querying.
  // Laying out the iterators in the order of being accessed makes it more
  // likely that any iterator pointer is close to the iterator it points to so
  // that they are likely to be in the same cache line and/or page.
  ArenaWrappedDBIter* db_iter = NewArenaWrappedDbIterator(
      env_, read_options, *cfd->ioptions(), sv->mutable_cf_options, snapshot,
      sv->mutable_cf_options.max_sequential_skip_in_iterations,
      sv->version_number, read_callback, this, cfd,
      ((read_options.snapshot != nullptr) ? false : allow_refresh));

  InternalIterator* internal_iter =
      NewInternalIterator(read_options, cfd, sv, db_iter->GetArena(),
                          db_iter->GetRangeDelAggregator(), snapshot);
  db_iter->SetIterUnderDBIter(internal_iter, nullptr, sv->current);

  return db_iter;
}

Status DBImpl::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (read_options.managed) {
    return Status::NotSupported("Managed iterator is not supported anymore.");
  }
  if (read_options.read_tier == kPersistedTier) {
    return Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators.");
  }
  LatencyHistGuard guard(&newiterator_latency_reporter_);
  newiterator_qps_reporter_.AddCount(column_families.size());

  ReadCallback* read_callback = nullptr;  // No read callback provided.
  iterators->clear();
  iterators->reserve(column_families.size());
  if (read_options.tailing) {
#ifdef ROCKSDB_LITE
    return Status::InvalidArgument(
        "Tailing interator not supported in RocksDB lite");
#else
    for (auto cfh : column_families) {
      auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
      SuperVersion* sv = cfd->GetReferencedSuperVersion(this);
      auto iter = new ForwardIterator(this, read_options, cfd, sv);
      iterators->push_back(NewDBIterator(
          env_, read_options, *cfd->ioptions(), sv->mutable_cf_options,
          cfd->user_comparator(), iter, iter, kMaxSequenceNumber, sv->current,
          sv->mutable_cf_options.max_sequential_skip_in_iterations,
          read_callback, this, cfd));
    }
#endif
  } else {
    // Note: no need to consider the special case of
    // last_seq_same_as_publish_seq_==false since NewIterators is overridden in
    // WritePreparedTxnDB
    auto snapshot = read_options.snapshot != nullptr
                        ? read_options.snapshot->GetSequenceNumber()
                        : versions_->LastSequence();
    for (size_t i = 0; i < column_families.size(); ++i) {
      auto* cfd =
          reinterpret_cast<ColumnFamilyHandleImpl*>(column_families[i])->cfd();
      iterators->push_back(
          NewIteratorImpl(read_options, cfd, snapshot, read_callback));
    }
  }

  return Status::OK();
}

const Snapshot* DBImpl::GetSnapshot() { return GetSnapshotImpl(false); }

#ifndef ROCKSDB_LITE
const Snapshot* DBImpl::GetSnapshotForWriteConflictBoundary() {
  return GetSnapshotImpl(true);
}
#endif  // ROCKSDB_LITE

SnapshotImpl* DBImpl::GetSnapshotImpl(bool is_write_conflict_boundary) {
  int64_t unix_time = 0;
  env_->GetCurrentTime(&unix_time);  // Ignore error
  SnapshotImpl* s = new SnapshotImpl;

  InstrumentedMutexLock l(&mutex_);
  // returns null if the underlying memtable does not support snapshot.
  if (!is_snapshot_supported_) {
    delete s;
    return nullptr;
  }
  auto snapshot_seq = last_seq_same_as_publish_seq_
                          ? versions_->LastSequence()
                          : versions_->LastPublishedSequence();
  return snapshots_.New(s, snapshot_seq, unix_time, is_write_conflict_boundary);
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  const SnapshotImpl* casted_s = reinterpret_cast<const SnapshotImpl*>(s);
  {
    InstrumentedMutexLock l(&mutex_);
    snapshots_.Delete(casted_s);
    uint64_t oldest_snapshot;
    if (snapshots_.empty()) {
      oldest_snapshot = last_seq_same_as_publish_seq_
                            ? versions_->LastSequence()
                            : versions_->LastPublishedSequence();
    } else {
      oldest_snapshot = snapshots_.oldest()->number_;
    }
    for (auto* cfd : *versions_->GetColumnFamilySet()) {
      cfd->current()->storage_info()->UpdateOldestSnapshot(oldest_snapshot);
      if (!cfd->current()
               ->storage_info()
               ->BottommostFilesMarkedForCompaction()
               .empty()) {
        SchedulePendingCompaction(cfd);
        SchedulePendingGarbageCollection(cfd);
        MaybeScheduleFlushOrCompaction();
      }
    }
  }
  delete casted_s;
}

#ifndef ROCKSDB_LITE

TablePropertiesCollectionIterator* DBImpl::NewPropertiesOfAllTablesIterator(
    ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  return new TablePropertiesCollectionIteratorImpl(this, cfd);
}

Status DBImpl::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                        TablePropertiesCollection* props) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  auto s = version->GetPropertiesOfAllTables(props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}

Status DBImpl::GetPropertiesOfTablesInRange(ColumnFamilyHandle* column_family,
                                            const Range* range, std::size_t n,
                                            TablePropertiesCollection* props,
                                            bool include_blob) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  auto s = version->GetPropertiesOfTablesInRange(range, n, props, include_blob);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();

  return s;
}

#endif  // ROCKSDB_LITE

const std::string& DBImpl::GetName() const { return dbname_; }

Env* DBImpl::GetEnv() const { return env_; }

Options DBImpl::GetOptions(ColumnFamilyHandle* column_family) const {
  InstrumentedMutexLock l(&mutex_);
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return Options(BuildDBOptions(immutable_db_options_, mutable_db_options_),
                 cfh->cfd()->GetLatestCFOptions());
}

DBOptions DBImpl::GetDBOptions() const {
  InstrumentedMutexLock l(&mutex_);
  return BuildDBOptions(immutable_db_options_, mutable_db_options_);
}

bool DBImpl::GetProperty(ColumnFamilyHandle* column_family,
                         const Slice& property, std::string* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_int) {
    uint64_t int_value;
    bool ret_value =
        GetIntPropertyInternal(cfd, *property_info, false, &int_value);
    if (ret_value) {
      *value = ToString(int_value);
    }
    return ret_value;
  } else if (property_info->handle_string) {
    InstrumentedMutexLock l(&mutex_);
    return cfd->internal_stats()->GetStringProperty(*property_info, property,
                                                    value);
  } else if (property_info->handle_string_dbimpl) {
    std::string tmp_value;
    bool ret_value = (this->*(property_info->handle_string_dbimpl))(&tmp_value);
    if (ret_value) {
      *value = tmp_value;
    }
    return ret_value;
  }
  // Shouldn't reach here since exactly one of handle_string and handle_int
  // should be non-nullptr.
  assert(false);
  return false;
}

bool DBImpl::GetMapProperty(ColumnFamilyHandle* column_family,
                            const Slice& property,
                            std::map<std::string, std::string>* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_map) {
    InstrumentedMutexLock l(&mutex_);
    return cfd->internal_stats()->GetMapProperty(*property_info, property,
                                                 value);
  }
  // If we reach this point it means that handle_map is not provided for the
  // requested property
  return false;
}

bool DBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, uint64_t* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    return false;
  }
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  return GetIntPropertyInternal(cfd, *property_info, false, value);
}

bool DBImpl::GetIntPropertyInternal(ColumnFamilyData* cfd,
                                    const DBPropertyInfo& property_info,
                                    bool is_locked, uint64_t* value) {
  assert(property_info.handle_int != nullptr);
  if (!property_info.need_out_of_mutex) {
    if (is_locked) {
      mutex_.AssertHeld();
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    }
  } else {
    SuperVersion* sv = nullptr;
    if (!is_locked) {
      sv = GetAndRefSuperVersion(cfd);
    } else {
      sv = cfd->GetSuperVersion();
    }

    bool ret = cfd->internal_stats()->GetIntPropertyOutOfMutex(
        property_info, sv->current, value);

    if (!is_locked) {
      ReturnAndCleanupSuperVersion(cfd, sv);
    }

    return ret;
  }
}

bool DBImpl::GetPropertyHandleOptionsStatistics(std::string* value) {
  assert(value != nullptr);
  Statistics* statistics = immutable_db_options_.statistics.get();
  if (!statistics) {
    return false;
  }
  *value = statistics->ToString();
  return true;
}

#ifndef ROCKSDB_LITE
Status DBImpl::ResetStats() {
  InstrumentedMutexLock l(&mutex_);
  for (auto* cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->initialized()) {
      cfd->internal_stats()->Clear();
    }
  }
  return Status::OK();
}
#endif  // ROCKSDB_LITE

bool DBImpl::GetAggregatedIntProperty(const Slice& property,
                                      uint64_t* aggregated_value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    return false;
  }

  uint64_t sum = 0;
  {
    // Needs mutex to protect the list of column families.
    InstrumentedMutexLock l(&mutex_);
    uint64_t value;
    for (auto* cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->initialized()) {
        continue;
      }
      if (GetIntPropertyInternal(cfd, *property_info, true, &value)) {
        sum += value;
      } else {
        return false;
      }
    }
  }
  *aggregated_value = sum;
  return true;
}

SuperVersion* DBImpl::GetAndRefSuperVersion(ColumnFamilyData* cfd) {
  // TODO(ljin): consider using GetReferencedSuperVersion() directly
  return cfd->GetThreadLocalSuperVersion(this);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
SuperVersion* DBImpl::GetAndRefSuperVersion(uint32_t column_family_id) {
  auto column_family_set = versions_->GetColumnFamilySet();
  auto cfd = column_family_set->GetColumnFamily(column_family_id);
  if (!cfd) {
    return nullptr;
  }

  return GetAndRefSuperVersion(cfd);
}

void DBImpl::CleanupSuperVersion(SuperVersion* sv) {
  // Release SuperVersion
  if (sv->Unref()) {
    {
      InstrumentedMutexLock l(&mutex_);
      sv->Cleanup();
      if (immutable_db_options_.avoid_unnecessary_blocking_io) {
        ScheduleBgFree(nullptr, sv);
        sv = nullptr;
      }
    }
    delete sv;
    RecordTick(stats_, NUMBER_SUPERVERSION_CLEANUPS);
  }
  RecordTick(stats_, NUMBER_SUPERVERSION_RELEASES);
}

void DBImpl::ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd,
                                          SuperVersion* sv) {
  if (!cfd->ReturnThreadLocalSuperVersion(sv)) {
    CleanupSuperVersion(sv);
  }
}

// REQUIRED: this function should only be called on the write thread.
void DBImpl::ReturnAndCleanupSuperVersion(uint32_t column_family_id,
                                          SuperVersion* sv) {
  auto column_family_set = versions_->GetColumnFamilySet();
  auto cfd = column_family_set->GetColumnFamily(column_family_id);

  // If SuperVersion is held, and we successfully fetched a cfd using
  // GetAndRefSuperVersion(), it must still exist.
  assert(cfd != nullptr);
  ReturnAndCleanupSuperVersion(cfd, sv);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
ColumnFamilyHandle* DBImpl::GetColumnFamilyHandle(uint32_t column_family_id) {
  ColumnFamilyMemTables* cf_memtables = column_family_memtables_.get();

  if (!cf_memtables->Seek(column_family_id)) {
    return nullptr;
  }

  return cf_memtables->GetColumnFamilyHandle();
}

// REQUIRED: mutex is NOT held.
std::unique_ptr<ColumnFamilyHandle> DBImpl::GetColumnFamilyHandleUnlocked(
    uint32_t column_family_id) {
  InstrumentedMutexLock l(&mutex_);

  auto* cfd =
      versions_->GetColumnFamilySet()->GetColumnFamily(column_family_id);
  if (cfd == nullptr) {
    return nullptr;
  }

  return std::unique_ptr<ColumnFamilyHandleImpl>(
      new ColumnFamilyHandleImpl(cfd, this, &mutex_));
}

void DBImpl::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                         const Range& range,
                                         uint64_t* const count,
                                         uint64_t* const size) {
  ColumnFamilyHandleImpl* cfh =
      reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  // Convert user_key into a corresponding internal key.
  InternalKey k1(range.start, kMaxSequenceNumber, kValueTypeForSeek);
  InternalKey k2(range.limit, kMaxSequenceNumber, kValueTypeForSeek);
  MemTable::MemTableStats memStats =
      sv->mem->ApproximateStats(k1.Encode(), k2.Encode());
  MemTable::MemTableStats immStats =
      sv->imm->ApproximateStats(k1.Encode(), k2.Encode());
  *count = memStats.count + immStats.count;
  *size = memStats.size + immStats.size;

  ReturnAndCleanupSuperVersion(cfd, sv);
}

void DBImpl::GetApproximateSizes(ColumnFamilyHandle* column_family,
                                 const Range* range, int n, uint64_t* sizes,
                                 uint8_t include_flags) {
  assert(include_flags & DB::SizeApproximationFlags::INCLUDE_FILES ||
         include_flags & DB::SizeApproximationFlags::INCLUDE_MEMTABLES);
  Version* v;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);
  v = sv->current;

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    sizes[i] = 0;
    if (include_flags & DB::SizeApproximationFlags::INCLUDE_FILES) {
      sizes[i] += versions_->ApproximateSize(v, k1.Encode(), k2.Encode());
    }
    if (include_flags & DB::SizeApproximationFlags::INCLUDE_MEMTABLES) {
      sizes[i] += sv->mem->ApproximateStats(k1.Encode(), k2.Encode()).size;
      sizes[i] += sv->imm->ApproximateStats(k1.Encode(), k2.Encode()).size;
    }
  }

  ReturnAndCleanupSuperVersion(cfd, sv);
}

std::list<uint64_t>::iterator
DBImpl::CaptureCurrentFileNumberInPendingOutputs() {
  // We need to remember the iterator of our insert, because after the
  // background job is done, we need to remove that element from
  // pending_outputs_.
  pending_outputs_.push_back(versions_->current_next_file_number());
  auto pending_outputs_inserted_elem = pending_outputs_.end();
  --pending_outputs_inserted_elem;
  return pending_outputs_inserted_elem;
}

void DBImpl::ReleaseFileNumberFromPendingOutputs(
    std::list<uint64_t>::iterator v) {
  pending_outputs_.erase(v);
}

#ifndef ROCKSDB_LITE
Status DBImpl::GetUpdatesSince(
    SequenceNumber seq, std::unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options) {
  RecordTick(stats_, GET_UPDATES_SINCE_CALLS);
  if (seq > versions_->LastSequence()) {
    return Status::NotFound("Requested sequence not yet written in the db");
  }
  return wal_manager_.GetUpdatesSince(seq, iter, read_options, versions_.get());
}

void DBImpl::SetGuardSeqno(SequenceNumber guard_seqno) {
  wal_manager_.SetGuardSeqno(guard_seqno);
}

Status DBImpl::DeleteFile(std::string name) {
  uint64_t number;
  FileType type;
  WalFileType log_type;
  if (!ParseFileName(name, &number, &type, &log_type) ||
      (type != kTableFile && type != kLogFile)) {
    ROCKS_LOG_ERROR(immutable_db_options_.info_log, "DeleteFile %s failed.\n",
                    name.c_str());
    return Status::InvalidArgument("Invalid file name");
  }

  Status status;
  if (type == kLogFile) {
    // Only allow deleting archived log files
    if (log_type != kArchivedLogFile) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "DeleteFile %s failed - not archived log.\n",
                      name.c_str());
      return Status::NotSupported("Delete only supported for archived logs");
    }
    status = wal_manager_.DeleteFile(name, number);
    if (!status.ok()) {
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "DeleteFile %s failed -- %s.\n", name.c_str(),
                      status.ToString().c_str());
    }
    return status;
  }

  int level;
  FileMetaData* metadata;
  ColumnFamilyData* cfd;
  VersionEdit edit;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  {
    InstrumentedMutexLock l(&mutex_);
    status = versions_->GetMetadataForFile(number, &level, &metadata, &cfd);
    if (!status.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "DeleteFile %s failed. File not found\n", name.c_str());
      mutex_.AssertHeld();
      job_context.Clean(nullptr);
      return Status::InvalidArgument("File not found");
    }
    assert(level < cfd->NumberLevels());

    // If the file is being compacted no need to delete.
    if (metadata->being_compacted) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "DeleteFile %s Skipped. File about to be compacted\n",
                     name.c_str());
      mutex_.AssertHeld();
      job_context.Clean(nullptr);
      return Status::OK();
    }

    // Only the files in the last level can be deleted externally.
    // This is to make sure that any deletion tombstones are not
    // lost. Check that the level passed is the last level.
    auto* vstoreage = cfd->current()->storage_info();
    for (int i = level + 1; i < cfd->NumberLevels(); i++) {
      if (vstoreage->NumLevelFiles(i) != 0) {
        ROCKS_LOG_WARN(immutable_db_options_.info_log,
                       "DeleteFile %s FAILED. File not in last level\n",
                       name.c_str());
        mutex_.AssertHeld();
        job_context.Clean(nullptr);
        return Status::InvalidArgument("File not in last level");
      }
    }
    // if level == 0, it has to be the oldest file
    if (level == 0 &&
        vstoreage->LevelFiles(0).back()->fd.GetNumber() != number) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "DeleteFile %s failed ---"
                     " target file in level 0 must be the oldest.",
                     name.c_str());
      mutex_.AssertHeld();
      job_context.Clean(nullptr);
      return Status::InvalidArgument("File in level 0, but not oldest");
    }
    metadata->being_compacted = true;
    edit.SetColumnFamily(cfd->GetID());
    edit.DeleteFile(level, number);
    status = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                                    &edit, &mutex_, directories_.GetDbDir());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          cfd, &job_context.superversion_contexts[0],
          *cfd->GetLatestMutableCFOptions(), FlushReason::kDeleteFiles);
    }
    metadata->being_compacted = false;
    FindObsoleteFiles(&job_context, false);
  }  // lock released here

  LogFlush(immutable_db_options_.info_log);
  // remove files outside the db-lock
  if (job_context.HaveSomethingToDelete()) {
    // Call PurgeObsoleteFiles() without holding mutex.
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean(&mutex_);
  return status;
}

Status DBImpl::DeleteFilesInRanges(ColumnFamilyHandle* column_family,
                                   const RangePtr* ranges, size_t n) {
  Status status;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  std::set<FileMetaData*> deleted_files;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  {
    VersionEdit edit;
    mutex_.Lock();
    Version* input_version = cfd->current();
    edit.SetColumnFamily(cfd->GetID());
    input_version->Ref();

    struct AutoRelease {
      std::list<uint64_t>::iterator pending_outputs_inserted_elem;
      DBImpl* db_impl;
      Version* input_version;
      bool is_lock;
      InstrumentedMutex* db_mutex;
      std::unordered_set<FileMetaData*> file_marked;

      void Lock() {
        assert(!is_lock);
        is_lock = true;
        db_mutex->Lock();
      }
      void Unlock() {
        assert(is_lock);
        is_lock = false;
        db_mutex->Unlock();
      }

      ~AutoRelease() {
        db_impl->ReleaseFileNumberFromPendingOutputs(
            pending_outputs_inserted_elem);
        if (!is_lock) {
          db_mutex->Lock();
        }
        input_version->Unref();
        for (auto f : file_marked) {
          f->being_compacted = false;
        }
        db_mutex->Unlock();
      }
    } auto_release = {CaptureCurrentFileNumberInPendingOutputs(), this,
                      input_version, true, &mutex_};

    auto* vstorage = input_version->storage_info();
    if (cfd->ioptions()->enable_lazy_compaction) {
      const InternalKeyComparator& ic = cfd->ioptions()->internal_comparator;
      auto uc = ic.user_comparator();
      // deref nullptr of start/limit
      InternalKey* nullptr_start = nullptr;
      InternalKey* nullptr_limit = nullptr;
      for (int level = 0; level < cfd->NumberLevels(); level++) {
        auto& level_files = vstorage->LevelFiles(level);
        if (level == 0) {
          for (size_t i = 0; i < level_files.size(); ++i) {
            auto& f = level_files[i];
            if (nullptr_start == nullptr ||
                ic.Compare(f->smallest, *nullptr_start) < 0) {
              nullptr_start = &f->smallest;
            }
            if (nullptr_limit == nullptr ||
                ic.Compare(f->largest, *nullptr_limit) > 0) {
              nullptr_limit = &f->largest;
            }
          }
        } else if (!level_files.empty()) {
          auto& f0 = level_files.front();
          auto& fn = level_files.back();
          if (nullptr_start == nullptr ||
              ic.Compare(f0->smallest, *nullptr_start) < 0) {
            nullptr_start = &f0->smallest;
          }
          if (nullptr_limit == nullptr ||
              ic.Compare(fn->largest, *nullptr_limit) > 0) {
            nullptr_limit = &fn->largest;
          }
        }
      }
      if (nullptr_start == nullptr || nullptr_limit == nullptr) {
        // empty vstorage ...
        mutex_.AssertHeld();
        job_context.Clean(nullptr);
        return Status::OK();
      }
      // fix user_key to [ ... )
      std::vector<Range> deleted_range;
      deleted_range.reserve(n);
      Arena arena;
      auto create_iter = [&](Arena* arena) {
        ReadOptions read_options;
        read_options.verify_checksums = true;
        read_options.fill_cache = false;
        read_options.total_order_seek = true;
        MergeIteratorBuilder builder(&ic, arena);
        input_version->AddIterators(read_options, env_options_, &builder,
                                    nullptr /* range_del_agg */);
        return builder.Finish();
      };
      LazyInternalIteratorWrapper version_iter(c_style_callback(create_iter),
                                               &create_iter, nullptr, nullptr,
                                               &arena);
      InternalKey ik;
      auto set_ik = [&ik](const Slice& uk) {
        ik.Clear();
        ik.SetMaxPossibleForUserKey(uk);
        return ik.Encode();
      };
      for (size_t i = 0; i < n; ++i) {
        Range deleted;
        if (ranges[i].start == nullptr) {
          deleted.start = nullptr_start->user_key();
        } else {
          if (ranges[i].include_start) {
            deleted.start = *ranges[i].start;
          } else {
            version_iter.Seek(set_ik(*ranges[i].start));
            if (version_iter.Valid() &&
                uc->Compare(ExtractUserKey(version_iter.key()),
                            *ranges[i].start) == 0) {
              version_iter.Next();
            }
            if (version_iter.Valid()) {
              deleted.start =
                  ArenaPinSlice(ExtractUserKey(version_iter.key()), &arena);
            } else if (!version_iter.status().ok()) {
              return version_iter.status();
            } else {
              // ranges[i].start >
              continue;
            }
          }
        }
        deleted.include_start = true;
        if (ranges[i].limit == nullptr) {
          deleted.limit = nullptr_limit->user_key();
          deleted.include_limit = true;
        } else {
          if (!ranges[i].include_limit) {
            deleted.limit = *ranges[i].limit;
            deleted.include_limit = false;
          } else {
            version_iter.Seek(set_ik(*ranges[i].limit));
            if (version_iter.Valid() &&
                uc->Compare(ExtractUserKey(version_iter.key()),
                            *ranges[i].limit) == 0) {
              version_iter.Next();
            }
            if (version_iter.Valid()) {
              deleted.limit =
                  ArenaPinSlice(ExtractUserKey(version_iter.key()), &arena);
              deleted.include_limit = false;
            } else if (!version_iter.status().ok()) {
              return version_iter.status();
            } else {
              deleted.limit = *ranges[i].limit;
              deleted.include_limit = true;
            }
          }
        }
        // deleted.include_limit ? limit < start : limit <= start
        if (uc->Compare(deleted.limit, deleted.start) <
            !deleted.include_limit) {
          continue;
        }
        deleted_range.push_back(deleted);
      }
      // sort & merge ranges
      terark::sort_a(deleted_range, TERARK_FIELD(start) < *uc);
      size_t c = 0;
      n = deleted_range.size();
      for (size_t i = 1; i < n; ++i) {
        if (uc->Compare(deleted_range[c].limit, deleted_range[i].start) >= 0) {
          if (uc->Compare(deleted_range[c].limit, deleted_range[i].limit) <=
              0) {
            deleted_range[c].limit = deleted_range[i].limit;
            deleted_range[c].include_limit |= deleted_range[i].include_limit;
          }
        } else if (++c != i) {
          deleted_range[c] = deleted_range[i];
        }
      }
      deleted_range.resize(c + 1);
      MapBuilder map_builder(job_context.job_id, immutable_db_options_,
                             env_options_, versions_.get(), stats_, dbname_);
      auto level_being_compacted = [vstorage, &auto_release](int level) {
        for (auto f : vstorage->LevelFiles(level)) {
          if (auto_release.file_marked.count(f) == 0) {
            return true;
          }
        }
        return false;
      };
      for (int i = 0; i < cfd->NumberLevels(); i++) {
        for (auto f : vstorage->LevelFiles(i)) {
          if (!f->being_compacted) {
            auto_release.file_marked.emplace(f);
            f->being_compacted = true;
          }
        }
      }
      auto_release.Unlock();
      for (int i = 0; i < cfd->NumberLevels(); i++) {
        if (vstorage->LevelFiles(i).empty()) {
          continue;
        }
        if (cfd->ioptions()->enable_lazy_compaction &&
            !level_being_compacted(i)) {
          status = map_builder.Build(
              {CompactionInputFiles{i, vstorage->LevelFiles(i)}}, deleted_range,
              {}, i /* level */, vstorage->LevelFiles(i)[0]->fd.GetPathId(),
              cfd, false /* optimize_range_deletion */, input_version, &edit,
              nullptr /* file_meta */, nullptr /* porp */, &deleted_files);
          if (!status.ok()) {
            break;
          }
        } else {
          for (auto f : vstorage->LevelFiles(i)) {
            if (auto_release.file_marked.count(f) == 0) {
              continue;
            }
            status = map_builder.Build(
                {CompactionInputFiles{i, {f}}}, deleted_range, {},
                i /* level */, f->fd.GetPathId(), cfd,
                false /* optimize_range_deletion */, input_version, &edit,
                nullptr /* file_meta */, nullptr /* porp */, &deleted_files);
            if (!status.ok()) {
              break;
            }
          }
        }
      }
      if (status.ok()) {
        for (auto f : deleted_files) {
          auto_release.file_marked.erase(f);
        }
      }
      auto_release.Lock();
    } else {
      for (size_t r = 0; r < n; r++) {
        auto begin = ranges[r].start, end = ranges[r].limit;
        auto include_begin = ranges[r].include_start;
        auto include_end = ranges[r].include_limit;
        for (int i = 1; i < cfd->NumberLevels(); i++) {
          if (vstorage->LevelFiles(i).empty() ||
              !vstorage->OverlapInLevel(i, begin, end)) {
            continue;
          }
          std::vector<FileMetaData*> level_files;
          InternalKey begin_storage, end_storage, *begin_key, *end_key;
          if (begin == nullptr) {
            begin_key = nullptr;
          } else {
            begin_storage.SetMinPossibleForUserKey(*begin);
            begin_key = &begin_storage;
          }
          if (end == nullptr) {
            end_key = nullptr;
          } else {
            end_storage.SetMaxPossibleForUserKey(*end);
            end_key = &end_storage;
          }

          vstorage->GetCleanInputsWithinInterval(
              i, begin_key, end_key, &level_files, -1 /* hint_index */,
              nullptr /* file_index */);
          FileMetaData* level_file;
          for (uint32_t j = 0; j < level_files.size(); j++) {
            level_file = level_files[j];
            if (level_file->being_compacted) {
              continue;
            }
            if (deleted_files.find(level_file) != deleted_files.end()) {
              continue;
            }
            if (!include_begin && begin != nullptr &&
                cfd->user_comparator()->Compare(level_file->smallest.user_key(),
                                                *begin) == 0) {
              continue;
            }
            if (!include_end && end != nullptr &&
                cfd->user_comparator()->Compare(level_file->largest.user_key(),
                                                *end) == 0) {
              continue;
            }
            edit.DeleteFile(i, level_file->fd.GetNumber());
            deleted_files.insert(level_file);
            level_file->being_compacted = true;
          }
        }
      }
    }
    if (!status.ok() ||
        (edit.GetNewFiles().empty() && edit.GetDeletedFiles().empty())) {
      mutex_.AssertHeld();
      job_context.Clean(nullptr);
      return status;
    }
    status = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                                    &edit, &mutex_, directories_.GetDbDir());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          cfd, &job_context.superversion_contexts[0],
          *cfd->GetLatestMutableCFOptions(), FlushReason::kDeleteFiles);
    }
    for (auto* deleted_file : deleted_files) {
      deleted_file->being_compacted = false;
    }
    FindObsoleteFiles(&job_context, false);
  }  // lock released here

  LogFlush(immutable_db_options_.info_log);
  // remove files outside the db-lock
  if (job_context.HaveSomethingToDelete()) {
    // Call PurgeObsoleteFiles() without holding mutex.
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean(&mutex_);
  return status;
}

void DBImpl::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  InstrumentedMutexLock l(&mutex_);
  versions_->GetLiveFilesMetaData(metadata);
}

void DBImpl::GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                                     ColumnFamilyMetaData* cf_meta) {
  assert(column_family);
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  auto* sv = GetAndRefSuperVersion(cfd);
  sv->current->GetColumnFamilyMetaData(cf_meta);
  ReturnAndCleanupSuperVersion(cfd, sv);
}

#endif  // ROCKSDB_LITE

Status DBImpl::CheckConsistency(bool read_only) {
  mutex_.AssertHeld();
  std::vector<LiveFileMetaData> metadata;
  versions_->GetLiveFilesMetaData(&metadata);

  std::string corruption_messages;
  for (const auto& md : metadata) {
    // md.name has a leading "/".
    std::string file_path = md.db_path + md.name;

    uint64_t fsize = 0;
    Status s = env_->GetFileSize(file_path, &fsize);
    if (!s.ok() &&
        env_->GetFileSize(Rocks2LevelTableFileName(file_path), &fsize).ok()) {
      s = Status::OK();
    }
    if (!s.ok()) {
      if (!read_only) {
        corruption_messages +=
            "Can't access " + md.name + ": " + s.ToString() + "\n";
      }
    } else if (fsize != md.size) {
      corruption_messages += "Sst file size mismatch: " + file_path +
                             ". Size recorded in manifest " +
                             ToString(md.size) + ", actual size " +
                             ToString(fsize) + "\n";
    }
  }
  if (corruption_messages.empty()) {
    return Status::OK();
  } else {
    return Status::Corruption(corruption_messages);
  }
}

Status DBImpl::GetDbIdentity(std::string& identity) const {
  std::string idfilename = IdentityFileName(dbname_);
  const EnvOptions soptions;
  std::unique_ptr<SequentialFileReader> id_file_reader;
  Status s;
  {
    std::unique_ptr<SequentialFile> idfile;
    s = env_->NewSequentialFile(idfilename, &idfile, soptions);
    if (!s.ok()) {
      return s;
    }
    id_file_reader.reset(
        new SequentialFileReader(std::move(idfile), idfilename));
  }

  uint64_t file_size;
  s = env_->GetFileSize(idfilename, &file_size);
  if (!s.ok()) {
    return s;
  }
  char* buffer =
      reinterpret_cast<char*>(alloca(static_cast<size_t>(file_size)));
  Slice id;
  s = id_file_reader->Read(static_cast<size_t>(file_size), &id, buffer);
  if (!s.ok()) {
    return s;
  }
  identity.assign(id.ToString());
  // If last character is '\n' remove it from identity
  if (identity.size() > 0 && identity.back() == '\n') {
    identity.pop_back();
  }
  return s;
}

Status DBImpl::GetDbSessionId(std::string& session_id) const {
  session_id.assign(db_session_id_);
  return Status::OK();
}

void DBImpl::SetDbSessionId() {
  // GenerateUniqueId() generates an identifier
  // that has a negligible probability of being duplicated
  db_session_id_ = env_->GenerateUniqueId();
  // Remove the extra '\n' at the end if there is one
  if (!db_session_id_.empty() && db_session_id_.back() == '\n') {
    db_session_id_.pop_back();
  }
}

// Default implementation -- returns not supported status
Status DB::CreateColumnFamily(const ColumnFamilyOptions& /*cf_options*/,
                              const std::string& /*column_family_name*/,
                              ColumnFamilyHandle** /*handle*/) {
  return Status::NotSupported("");
}

Status DB::CreateColumnFamilies(
    const ColumnFamilyOptions& /*cf_options*/,
    const std::vector<std::string>& /*column_family_names*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/) {
  return Status::NotSupported("");
}

Status DB::CreateColumnFamilies(
    const std::vector<ColumnFamilyDescriptor>& /*column_families*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/) {
  return Status::NotSupported("");
}

Status DB::DropColumnFamily(ColumnFamilyHandle* /*column_family*/) {
  return Status::NotSupported("");
}

Status DB::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& /*column_families*/) {
  return Status::NotSupported("");
}

Status DB::DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) {
  delete column_family;
  return Status::OK();
}

DB::~DB() {}

#ifdef WITH_BOOSTLIB
void DB::CallOnMainStack(const std::function<void()>& fn) {
  gt_fibers.m_fy.sched()->call_on_main_stack(fn);
}

void DB::SubmitAsyncTask(std::function<void()> fn) {
  gt_fibers.push(std::move(fn));
}

void DB::SubmitAsyncTask(std::function<void()> fn, size_t aio_concurrency) {
  auto tls = &gt_fibers;
  tls->update_fiber_count(aio_concurrency);
  tls->push(std::move(fn));
}

bool DB::TrySubmitAsyncTask(const std::function<void()>& fn) {
  auto tls = &gt_fibers;
  return tls->try_push(fn);
}

bool DB::TrySubmitAsyncTask(const std::function<void()>& fn,
                            size_t aio_concurrency) {
  auto tls = &gt_fibers;
  tls->update_fiber_count(aio_concurrency);
  return tls->try_push(fn);
}

void DB::GetAsync(const ReadOptions& ro, ColumnFamilyHandle* cfh,
                  std::string key, std::string* value, GetAsyncCallback cb) {
  using namespace boost::fibers;
  using std::move;
  auto tls = &gt_fibers;
  tls->update_fiber_count(ro.aio_concurrency);
  tls->push([=, key = move(key), cb = move(cb)]() mutable {
    auto s = this->Get(ro, cfh, key, value);
    cb(move(s), move(key), value);
  });
}

void DB::GetAsync(const ReadOptions& ro, std::string key, std::string* value,
                  GetAsyncCallback cb) {
  using std::move;
  GetAsync(ro, DefaultColumnFamily(), move(key), value, move(cb));
}

void DB::GetAsync(const ReadOptions& ro, ColumnFamilyHandle* cfh,
                  std::string key, GetAsyncCallback cb) {
  using namespace boost::fibers;
  using std::move;
  auto tls = &gt_fibers;
  tls->update_fiber_count(ro.aio_concurrency);
  tls->push([=, key = move(key), cb = move(cb)]() mutable {
    std::string value;
    Status s = this->Get(ro, cfh, key, &value);
    cb(move(s), move(key), &value);
  });
}

void DB::GetAsync(const ReadOptions& ro, std::string key, GetAsyncCallback cb) {
  using std::move;
  GetAsync(ro, DefaultColumnFamily(), move(key), move(cb));
}

///@returns == 0 indicate there is nothing to wait
///          < 0 indicate number of finished GetAsync/GetFuture requests after
///              timeout
///          > 0 indicate number of all GetAsync/GetFuture requests have
///              finished within timeout
int DB::WaitAsync(int timeout_us) { return gt_fibers.wait(timeout_us); }

int DB::WaitAsync() { return gt_fibers.wait(); }
#endif  // WITH_BOOSTLIB

// using future needs boost symbols to be exported, but we don't want to
// export boost symbols
#if defined(TERARKDB_WITH_AIO_FUTURE)

// boost::fibers::promise has some problem for being captured by
// std::move for std::function
// use intrusive_ptr to workaround (capture by copy intrusive_ptr)
template <class T>
struct DB_Promise {
  DB_Promise(std::string& k) : key(std::move(k)) {}

  intptr_t refcnt = 0;
  std::string key;
  boost::fibers::promise<T> pr;

  friend void intrusive_ptr_add_ref(DB_Promise* p) { p->refcnt++; }

  friend void intrusive_ptr_release(DB_Promise* p) {
    if (0 == --p->refcnt) delete p;
  }
};

template <class T>
struct DB_PromisePtr : boost::intrusive_ptr<DB_Promise<T>> {
  DB_PromisePtr(std::string& k)
      : boost::intrusive_ptr<DB_Promise<T>>(new DB_Promise<T>(k)) {}
};

future<std::tuple<Status, std::string, std::string*>> DB::GetFuture(
    const ReadOptions& ro, ColumnFamilyHandle* cfh, std::string key,
    std::string* value) {
  using namespace boost::fibers;
  using std::move;
  auto tls = &gt_fibers;
  tls->update_fiber_count(ro.aio_concurrency);
  DB_PromisePtr<std::tuple<Status, std::string, std::string*>> p(key);
  auto fu = p->pr.get_future();
  tls->push([this, ro, cfh, p, value]() {
    std::tuple<Status, std::string, std::string*> result;
    std::get<0>(result) = this->Get(ro, cfh, p->key, value);
    std::get<1>(result) = std::move(p->key);
    std::get<2>(result) = value;
    p->pr.set_value(std::move(result));
  });
  return fu;
}

future<std::tuple<Status, std::string, std::string*>> DB::GetFuture(
    const ReadOptions& ro, std::string key, std::string* value) {
  return GetFuture(ro, DefaultColumnFamily(), std::move(key), value);
}

future<std::tuple<Status, std::string, std::string>> DB::GetFuture(
    const ReadOptions& ro, ColumnFamilyHandle* cfh, std::string key) {
  using namespace boost::fibers;
  using std::move;
  auto tls = &gt_fibers;
  tls->update_fiber_count(ro.aio_concurrency);
  DB_PromisePtr<std::tuple<Status, std::string, std::string>> p(key);
  auto fu = p->pr.get_future();
  tls->push([this, ro, cfh, p]() {
    std::tuple<Status, std::string, std::string> result;
    std::get<0>(result) = this->Get(ro, cfh, p->key, &std::get<2>(result));
    std::get<1>(result) = std::move(p->key);
    p->pr.set_value(std::move(result));
  });
  return fu;
}

future<std::tuple<Status, std::string, std::string>> DB::GetFuture(
    const ReadOptions& ro, std::string key) {
  return GetFuture(ro, DefaultColumnFamily(), std::move(key));
}

#endif  // TERARKDB_WITH_AIO_FUTURE

Status DBImpl::Close() {
  if (!closed_) {
    closed_ = true;
    return CloseImpl();
  }
  return Status::OK();
}

Status DB::ListColumnFamilies(const DBOptions& db_options,
                              const std::string& name,
                              std::vector<std::string>* column_families) {
  return VersionSet::ListColumnFamilies(column_families, name, db_options.env);
}

Snapshot::~Snapshot() {}

Status DestroyDB(const std::string& dbname, const Options& options,
                 const std::vector<ColumnFamilyDescriptor>& column_families) {
  ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;
  std::vector<std::string> filenames;

  // Reset the logger because it holds a handle to the
  // log file and prevents cleanup and directory removal
  soptions.info_log.reset();
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    InfoLogPrefix info_log_prefix(!soptions.db_log_dir.empty(), dbname);
    for (const auto& fname : filenames) {
      if (ParseFileName(fname, &number, info_log_prefix.prefix, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del;
        std::string path_to_delete = dbname + "/" + fname;
        if (type == kMetaDatabase) {
          del = DestroyDB(path_to_delete, options);
        } else if (type == kTableFile) {
          del = DeleteSSTFile(&soptions, path_to_delete, dbname);
        } else {
          del = env->DeleteFile(path_to_delete);
        }
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }

    std::vector<std::string> paths;

    for (const auto& path : options.db_paths) {
      paths.emplace_back(path.path);
    }
    for (const auto& cf : column_families) {
      for (const auto& path : cf.options.cf_paths) {
        paths.emplace_back(path.path);
      }
    }

    // Remove duplicate paths.
    // Note that we compare only the actual paths but not path ids.
    // This reason is that same path can appear at different path_ids
    // for different column families.
    std::sort(paths.begin(), paths.end());
    paths.erase(std::unique(paths.begin(), paths.end()), paths.end());

    for (const auto& path : paths) {
      if (env->GetChildren(path, &filenames).ok()) {
        for (const auto& fname : filenames) {
          if (ParseFileName(fname, &number, &type) &&
              type == kTableFile) {  // Lock file will be deleted at end
            std::string table_path = path + "/" + fname;
            Status del = DeleteSSTFile(&soptions, table_path, dbname);
            if (result.ok() && !del.ok()) {
              result = del;
            }
          }
        }
        env->DeleteDir(path);
      }
    }

    std::vector<std::string> walDirFiles;
    std::string archivedir = ArchivalDirectory(dbname);
    bool wal_dir_exists = false;
    if (dbname != soptions.wal_dir) {
      wal_dir_exists = env->GetChildren(soptions.wal_dir, &walDirFiles).ok();
      archivedir = ArchivalDirectory(soptions.wal_dir);
    }

    // Archive dir may be inside wal dir or dbname and should be
    // processed and removed before those otherwise we have issues
    // removing them
    std::vector<std::string> archiveFiles;
    if (env->GetChildren(archivedir, &archiveFiles).ok()) {
      // Delete archival files.
      for (const auto& file : archiveFiles) {
        if (ParseFileName(file, &number, &type) && type == kLogFile) {
          Status del = env->DeleteFile(archivedir + "/" + file);
          if (result.ok() && !del.ok()) {
            result = del;
          }
        }
      }
      env->DeleteDir(archivedir);
    }

    // Delete log files in the WAL dir
    if (wal_dir_exists) {
      for (const auto& file : walDirFiles) {
        if (ParseFileName(file, &number, &type) && type == kLogFile) {
          Status del = env->DeleteFile(LogFileName(soptions.wal_dir, number));
          if (result.ok() && !del.ok()) {
            result = del;
          }
        }
      }
      env->DeleteDir(soptions.wal_dir);
    }

    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

Status DBImpl::WriteOptionsFile(bool need_mutex_lock,
                                bool need_enter_write_thread) {
#ifndef ROCKSDB_LITE
  WriteThread::Writer w;
  if (need_mutex_lock) {
    mutex_.Lock();
  } else {
    mutex_.AssertHeld();
  }
  if (need_enter_write_thread) {
    write_thread_.EnterUnbatched(&w, &mutex_);
  }

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;

  // This part requires mutex to protect the column family options
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped()) {
      continue;
    }
    cf_names.push_back(cfd->GetName());
    cf_opts.push_back(cfd->GetLatestCFOptions());
  }

  // Unlock during expensive operations.  New writes cannot get here
  // because the single write thread ensures all new writes get queued.
  DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  mutex_.Unlock();

  TEST_SYNC_POINT("DBImpl::WriteOptionsFile:1");
  TEST_SYNC_POINT("DBImpl::WriteOptionsFile:2");

  std::string file_name =
      TempOptionsFileName(GetName(), versions_->NewFileNumber());
  Status s =
      PersistRocksDBOptions(db_options, cf_names, cf_opts, file_name, GetEnv());

  if (s.ok()) {
    s = RenameTempFileToOptionsFile(file_name);
  }
  // restore lock
  if (!need_mutex_lock) {
    mutex_.Lock();
  }
  if (need_enter_write_thread) {
    write_thread_.ExitUnbatched(&w);
  }
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Unable to persist options -- %s", s.ToString().c_str());
    if (immutable_db_options_.fail_if_options_file_error) {
      return Status::IOError("Unable to persist options.",
                             s.ToString().c_str());
    }
  }
#else
  (void)need_mutex_lock;
  (void)need_enter_write_thread;
#endif  // !ROCKSDB_LITE
  return Status::OK();
}

#ifndef ROCKSDB_LITE
namespace {
void DeleteOptionsFilesHelper(const std::map<uint64_t, std::string>& filenames,
                              const size_t num_files_to_keep,
                              const std::shared_ptr<Logger>& info_log,
                              Env* env) {
  if (filenames.size() <= num_files_to_keep) {
    return;
  }
  for (auto iter = std::next(filenames.begin(), num_files_to_keep);
       iter != filenames.end(); ++iter) {
    if (!env->DeleteFile(iter->second).ok()) {
      ROCKS_LOG_WARN(info_log, "Unable to delete options file %s",
                     iter->second.c_str());
    }
  }
}
}  // namespace
#endif  // !ROCKSDB_LITE

Status DBImpl::DeleteObsoleteOptionsFiles() {
#ifndef ROCKSDB_LITE
  std::vector<std::string> filenames;
  // use ordered map to store keep the filenames sorted from the newest
  // to the oldest.
  std::map<uint64_t, std::string> options_filenames;
  Status s;
  s = GetEnv()->GetChildren(GetName(), &filenames);
  if (!s.ok()) {
    return s;
  }
  for (auto& filename : filenames) {
    uint64_t file_number;
    FileType type;
    if (ParseFileName(filename, &file_number, &type) && type == kOptionsFile) {
      options_filenames.insert(
          {port::kMaxUint64 - file_number, GetName() + "/" + filename});
    }
  }

  // Keeps the latest 2 Options file
  const size_t kNumOptionsFilesKept = 2;
  DeleteOptionsFilesHelper(options_filenames, kNumOptionsFilesKept,
                           immutable_db_options_.info_log, GetEnv());
  return Status::OK();
#else
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

Status DBImpl::RenameTempFileToOptionsFile(const std::string& file_name) {
#ifndef ROCKSDB_LITE
  Status s;

  versions_->options_file_number_ = versions_->NewFileNumber();
  std::string options_file_name =
      OptionsFileName(GetName(), versions_->options_file_number_);
  // Retry if the file name happen to conflict with an existing one.
  s = GetEnv()->RenameFile(file_name, options_file_name);

  if (0 == disable_delete_obsolete_files_) {
    DeleteObsoleteOptionsFiles();
  }
  return s;
#else
  (void)file_name;
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

#ifdef ROCKSDB_USING_THREAD_STATUS

void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::NewColumnFamilyInfo(this, cfd, cfd->GetName(),
                                          cfd->ioptions()->env);
  }
}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseColumnFamilyInfo(cfd);
  }
}

void DBImpl::EraseThreadStatusDbInfo() const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseDatabaseInfo(this);
  }
}

#else
void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* /*cfd*/) const {}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* /*cfd*/) const {}

void DBImpl::EraseThreadStatusDbInfo() const {}
#endif  // ROCKSDB_USING_THREAD_STATUS

//
// A global method that can dump out the build version
void DumpRocksDBBuildVersion(Logger* log) {
#if !defined(IOS_CROSS_COMPILE)
  // if we compile with Xcode, we don't run build_detect_version, so we don't
  // generate util/build_version.cc
  ROCKS_LOG_HEADER(log, "RocksDB version: %d.%d.%d\n", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR, ROCKSDB_PATCH);
  ROCKS_LOG_HEADER(log, "Git sha %s", rocksdb_build_git_sha);
  ROCKS_LOG_HEADER(log, "Compile date %s", rocksdb_build_compile_date);
#endif
}

#ifndef ROCKSDB_LITE
SequenceNumber DBImpl::GetEarliestMemTableSequenceNumber(SuperVersion* sv,
                                                         bool include_history) {
  // Find the earliest sequence number that we know we can rely on reading
  // from the memtable without needing to check sst files.
  SequenceNumber earliest_seq =
      sv->imm->GetEarliestSequenceNumber(include_history);
  if (earliest_seq == kMaxSequenceNumber) {
    earliest_seq = sv->mem->GetEarliestSequenceNumber();
  }
  assert(sv->mem->GetEarliestSequenceNumber() >= earliest_seq);

  return earliest_seq;
}
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE
Status DBImpl::GetLatestSequenceForKey(SuperVersion* sv, const Slice& key,
                                       bool cache_only, SequenceNumber* seq,
                                       bool* found_record_for_key) {
  Status s;
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;

  ReadOptions read_options;
  SequenceNumber current_seq = versions_->LastSequence();
  LookupKey lkey(key, current_seq);

  *seq = kMaxSequenceNumber;
  *found_record_for_key = false;

  // Check if there is a record for this key in the latest memtable
  sv->mem->Get(lkey, nullptr, &s, &merge_context, &max_covering_tombstone_seq,
               seq, read_options, nullptr /*read_callback*/);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Unexpected status returned from MemTable::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check immutable memtables
    *found_record_for_key = true;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->Get(lkey, nullptr, &s, &merge_context, &max_covering_tombstone_seq,
               seq, read_options, nullptr /*read_callback*/);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                    "Unexpected status returned from MemTableList::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check memtable history
    *found_record_for_key = true;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->GetFromHistory(lkey, nullptr, &s, &merge_context,
                          &max_covering_tombstone_seq, seq, read_options);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    ROCKS_LOG_ERROR(
        immutable_db_options_.info_log,
        "Unexpected status returned from MemTableList::GetFromHistory: %s\n",
        s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check SST files
    *found_record_for_key = true;
    return Status::OK();
  }

  // TODO(agiardullo): possible optimization: consider checking cached
  // SST files if cache_only=true?
  if (!cache_only) {
    // Check tables
    sv->current->Get(read_options, key, lkey, nullptr, &s, &merge_context,
                     &max_covering_tombstone_seq, nullptr /* value_found */,
                     found_record_for_key, seq, nullptr /*read_callback*/);

    if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
      // unexpected error reading SST files
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Unexpected status returned from Version::Get: %s\n",
                      s.ToString().c_str());
    }
  }

  return s;
}

Status DBImpl::IngestExternalFile(
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& external_files,
    const IngestExternalFileOptions& ingestion_options) {
  if (external_files.empty()) {
    return Status::InvalidArgument("external_files is empty");
  }

  Status status;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  // Ingest should immediately fail if ingest_behind is requested,
  // but the DB doesn't support it.
  if (ingestion_options.ingest_behind) {
    if (!immutable_db_options_.allow_ingest_behind) {
      return Status::InvalidArgument(
          "Can't ingest_behind file in DB with allow_ingest_behind=false");
    }
  }

  ExternalSstFileIngestionJob ingestion_job(env_, versions_.get(), cfd,
                                            immutable_db_options_, env_options_,
                                            &snapshots_, ingestion_options);

  SuperVersionContext dummy_sv_ctx(/* create_superversion */ true);
  VersionEdit dummy_edit;
  uint64_t next_file_number = 0;
  std::list<uint64_t>::iterator pending_output_elem;
  {
    InstrumentedMutexLock l(&mutex_);
    if (error_handler_.IsDBStopped()) {
      // Don't ingest files when there is a bg_error
      return error_handler_.GetBGError();
    }

    // Make sure that bg cleanup wont delete the files that we are ingesting
    pending_output_elem = CaptureCurrentFileNumberInPendingOutputs();

    // If crash happen after a hard link established, Recover function may
    // reuse the file number that has already assigned to the internal file,
    // and this will overwrite the external file. To protect the external
    // file, we have to make sure the file number will never being reused.
    next_file_number = versions_->FetchAddFileNumber(external_files.size());
    auto cf_options = cfd->GetLatestMutableCFOptions();
    status = versions_->LogAndApply(cfd, *cf_options, &dummy_edit, &mutex_,
                                    directories_.GetDbDir());
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(cfd, &dummy_sv_ctx, *cf_options);
    }
  }
  dummy_sv_ctx.Clean();
  if (!status.ok()) {
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
    return status;
  }

  SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
  status =
      ingestion_job.Prepare(external_files, next_file_number, super_version);
  CleanupSuperVersion(super_version);
  if (!status.ok()) {
    InstrumentedMutexLock l(&mutex_);
    ReleaseFileNumberFromPendingOutputs(pending_output_elem);
    return status;
  }

  SuperVersionContext sv_context(/* create_superversion */ true);
  TEST_SYNC_POINT("DBImpl::AddFile:Start");
  {
    // Lock db mutex
    InstrumentedMutexLock l(&mutex_);
    TEST_SYNC_POINT("DBImpl::AddFile:MutexLock");

    // Stop writes to the DB by entering both write threads
    WriteThread::Writer w;
    write_thread_.EnterUnbatched(&w, &mutex_);
    WriteThread::Writer nonmem_w;
    if (two_write_queues_) {
      nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
    }

    num_running_ingest_file_++;
    TEST_SYNC_POINT("DBImpl::IngestExternalFile:AfterIncIngestFileCounter");

    // We cannot ingest a file into a dropped CF
    if (cfd->IsDropped()) {
      status = Status::InvalidArgument(
          "Cannot ingest an external file into a dropped CF");
    }

    // Figure out if we need to flush the memtable first
    if (status.ok()) {
      bool need_flush = false;
      status = ingestion_job.NeedsFlush(&need_flush, cfd->GetSuperVersion());
      TEST_SYNC_POINT_CALLBACK("DBImpl::IngestExternalFile:NeedFlush",
                               &need_flush);
      if (status.ok() && need_flush) {
        FlushOptions flush_opts;
        flush_opts.allow_write_stall = true;
        mutex_.Unlock();
        status = FlushMemTable({cfd}, flush_opts,
                               FlushReason::kExternalFileIngestion,
                               true /* writes_stopped */);
        mutex_.Lock();
      }
    }

    // Run the ingestion job
    if (status.ok()) {
      status = ingestion_job.Run();
    }

    // Install job edit [Mutex will be unlocked here]
    auto mutable_cf_options = cfd->GetLatestMutableCFOptions();
    if (status.ok()) {
      status =
          versions_->LogAndApply(cfd, *mutable_cf_options, ingestion_job.edit(),
                                 &mutex_, directories_.GetDbDir());
    }
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(cfd, &sv_context, *mutable_cf_options,
                                         FlushReason::kExternalFileIngestion);
    }

    // Resume writes to the DB
    if (two_write_queues_) {
      nonmem_write_thread_.ExitUnbatched(&nonmem_w);
    }
    write_thread_.ExitUnbatched(&w);

    // Update stats
    if (status.ok()) {
      ingestion_job.UpdateStats();
    }

    ReleaseFileNumberFromPendingOutputs(pending_output_elem);

    num_running_ingest_file_--;
    if (num_running_ingest_file_ == 0) {
      bg_cv_.SignalAll();
    }

    TEST_SYNC_POINT("DBImpl::AddFile:MutexUnlock");
  }
  // mutex_ is unlocked here

  // Cleanup
  sv_context.Clean();
  ingestion_job.Cleanup(status);

  if (status.ok()) {
    NotifyOnExternalFileIngested(cfd, ingestion_job);
  }

  return status;
}

Status DBImpl::VerifyChecksum() {
  Status s;
  std::vector<ColumnFamilyData*> cfd_list;
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->IsDropped() && cfd->initialized()) {
        cfd->Ref();
        cfd_list.push_back(cfd);
      }
    }
  }
  std::vector<SuperVersion*> sv_list;
  for (auto cfd : cfd_list) {
    sv_list.push_back(cfd->GetReferencedSuperVersion(this));
  }
  for (auto& sv : sv_list) {
    VersionStorageInfo* vstorage = sv->current->storage_info();
    ColumnFamilyData* cfd = sv->current->cfd();
    Options opts;
    {
      InstrumentedMutexLock l(&mutex_);
      opts = Options(BuildDBOptions(immutable_db_options_, mutable_db_options_),
                     cfd->GetLatestCFOptions());
    }
    for (int i = -1; i < vstorage->num_non_empty_levels() && s.ok(); i++) {
      for (size_t j = 0; j < vstorage->LevelFiles(i).size() && s.ok(); j++) {
        const auto& fd = vstorage->LevelFiles(i)[j]->fd;
        std::string fname = TableFileName(cfd->ioptions()->cf_paths,
                                          fd.GetNumber(), fd.GetPathId());
        s = TERARKDB_NAMESPACE::VerifySstFileChecksum(opts, env_options_,
                                                      fname);
      }
    }
    if (!s.ok()) {
      break;
    }
  }
  std::vector<SuperVersion*> sv_to_delete;
  {
    InstrumentedMutexLock l(&mutex_);
    for (auto sv : sv_list) {
      if (sv && sv->Unref()) {
        sv->Cleanup();
        sv_to_delete.emplace_back(sv);
      }
    }
    for (auto cfd : cfd_list) {
      cfd->Unref();
    }
  }
  for (auto sv : sv_to_delete) {
    delete sv;
  }
  return s;
}

void DBImpl::NotifyOnExternalFileIngested(
    ColumnFamilyData* cfd, const ExternalSstFileIngestionJob& ingestion_job) {
  if (immutable_db_options_.listeners.empty()) {
    return;
  }

  for (const IngestedFileInfo& f : ingestion_job.files_to_ingest()) {
    ExternalFileIngestionInfo info;
    info.cf_name = cfd->GetName();
    info.external_file_path = f.external_file_path;
    info.internal_file_path = f.internal_file_path;
    info.global_seqno = f.assigned_seqno;
    info.table_properties = f.table_properties;
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnExternalFileIngested(this, info);
    }
  }
}

void DBImpl::WaitForIngestFile() {
  mutex_.AssertHeld();
  while (num_running_ingest_file_ > 0) {
    bg_cv_.Wait();
  }
}

Status DBImpl::StartTrace(const TraceOptions& trace_options,
                          std::unique_ptr<TraceWriter>&& trace_writer) {
  InstrumentedMutexLock lock(&trace_mutex_);
  tracer_.reset(new Tracer(env_, trace_options, std::move(trace_writer)));
  return Status::OK();
}

Status DBImpl::EndTrace() {
  InstrumentedMutexLock lock(&trace_mutex_);
  Status s = tracer_->Close();
  tracer_.reset();
  return s;
}

Status DBImpl::TraceIteratorSeek(const uint32_t& cf_id, const Slice& key) {
  Status s;
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      s = tracer_->IteratorSeek(cf_id, key);
    }
  }
  return s;
}

Status DBImpl::TraceIteratorSeekForPrev(const uint32_t& cf_id,
                                        const Slice& key) {
  Status s;
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_) {
      s = tracer_->IteratorSeekForPrev(cf_id, key);
    }
  }
  return s;
}

#endif  // ROCKSDB_LITE

}  // namespace TERARKDB_NAMESPACE
