// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <gflags/gflags.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/sst_file_manager.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/lazy_buffer.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "table/terark_zip_table.h"
#include "util/string_util.h"

using namespace rocksdb;

DECLARE_uint64(terark$misc$cache_capacity);
DECLARE_int32(terark$db_opt$max_background_flushes);
DECLARE_int32(terark$db_opt$max_background_compactions);
DECLARE_double(terark$misc$threads_reserve_factor);

static std::mutex terarkdb_mutex;
static int terarkdb_instance_started = 0;

// static std::multimap<std::chrono::system_clock::time_point,
// TerarkDBRunningJob> terarkdb_running_job_map; static
// std::map<std::pair<std::string, std::string>, TerarkDBCFHandle>
// terarkdb_instance_map; static std::set<std::string> terarkdb_dbname_set,
// terarkdb_cfname_set; static std::multimap<std::string, TerarkDBCFHandle>
// terarkdb_option_handler;
static std::vector<std::shared_ptr<rocksdb::RateLimiter>> terarkdb_rate_limiter;
static std::vector<std::shared_ptr<rocksdb::SstFileManager>>
    terarkdb_sst_file_manager;

static std::unordered_map<std::string, std::string>
TerarkDBGetOptionsMapFromGFLAGS(const std::string &prefix) {
  std::unordered_map<std::string, std::string> options_map;
  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);
  for (const auto &info : all_flags) {
    if (rocksdb::Slice(info.name).starts_with(prefix)) {
      options_map.emplace(info.name.substr(prefix.size()), info.current_value);
    }
  }
  return options_map;
}

////////////////////////////////////////////////////////////////////////////////
// Tools Flags
////////////////////////////////////////////////////////////////////////////////

// 上次修改 TerarkDB 配置的结果
DEFINE_string(terark$misc$last_operation_status, "", "Last operation status.");

DEFINE_string(terark$misc$dbname_list, "", "List of valid dbname.");
DEFINE_string(terark$misc$cfname_list, "", "List of valid cfname.");

DEFINE_string(terark$misc$cmd_manual_run, "",
              "[flush,compact,ack]:[*,{dbname}]:[*,{cfname}]");
DEFINE_string(terark$misc$cmd_manual_job_state, "",
              "terarkdb manual flush/compaction job state");

DEFINE_string(terark$misc$cmd_property, "", "{property}:{dbname}:{cfname}");
DEFINE_string(terark$misc$cmd_property_value, "", "terarkdb property value");

DEFINE_string(terark$misc$thread_pool_state, "", "TerarkDB ThreadPool state");

// 是否启用 TerarkZipTable
DEFINE_bool(terark$misc$enable_terark_zip_table, false,
            "Enable TerarkZipTable");

// BlockCache 容量
DEFINE_uint64(terark$misc$cache_capacity, 128ull << 30,
              "Block cache capacity (in bytes). WARNING: OOM risk");
// DEFINE_validator(terark$misc$cache_capacity, terark_validate_cache_capacity);

// RateLimiter 速率
DEFINE_uint64(terark$misc$rate_limiter_bytes, 400ull << 20,
              "RateLimiter throughput (in bytes)");
// DEFINE_validator(terark$misc$rate_limiter_bytes,
// terark_validate_rate_limiter);

DEFINE_double(terark$misc$threads_reserve_factor, 0.3,
              "Instance thread pool grow ratio");
// DEFINE_validator(terark$misc$threads_reserve_factor,
// terark_validate_thread_factor);

// 可以迁移 INFO LOG 到别的目录
DEFINE_string(terark$misc$db_log_dir, "",
              "{dbname}={log_dir};{dbname}={log_dir};...");

// 可以迁移 WAL 到快速设备
// 暂时没搞定多实例如何配置，先注掉了
DEFINE_string(terark$misc$wal_dir, "",
              "{dbname}={wal_dir};{dbname}={wal_dir};...");

////////////////////////////////////////////////////////////////////////////////
// DBOptions
////////////////////////////////////////////////////////////////////////////////

// 所有 CF 总是一起 Flush，请确保有这个需求再开启该选项
DEFINE_bool(terark$db_opt$atomic_flush, false, "Enable atomic flush");

// INFO LOG 级别，默认
// INFO_LEVEL，DEBUG_LEVEL会多一些信息，但是也没到影响性能的程度
DEFINE_string(
    terark$db_opt$info_log_level, "DEBUG_LEVEL",
    "[DEBUG_LEVEL,INFO_LEVEL,WARN_LEVEL,ERROR_LEVEL,FATAL_LEVEL,HEADER_LEVEL]");

// 避免停机时候 Flush。打开的话可能安全停机的时候可以快一点？但是启动时候要
// Recover WAL
DEFINE_bool(terark$db_opt$avoid_flush_during_shutdown, false,
            "Don't flush mem-table when shutdown");

// 避免启动时候 Flush（可能会造出来一些小 SST）
// 打开的话会尽量不要 Flush（但是不保证），但是意外停机再重启就需要再 Recover
// WAL
DEFINE_bool(terark$db_opt$avoid_flush_during_recovery, false,
            "Don't flush mem-table when db open");

// WAL 恢复模式
// kTolerateCorruptedTailRecords
//   LevelDB 的策略, 允许每个 WAL 的末端有不完整数据。经典做法
// kAbsoluteConsistency
//   不允许 WAL 有任何损坏，高一致性
// kPointInTimeRecovery
//   掉电一致性，WAL 发现任何错误，就停止回放，丢弃后续数据
// kSkipAnyCorruptedRecords
//   容灾恢复，尽可能抢救更多的数据，无视一致性
DEFINE_string(terark$db_opt$wal_recovery_mode, "kPointInTimeRecovery",
              "[kTolerateCorruptedTailRecords,kAbsoluteConsistency,"
              "kPointInTimeRecovery,kSkipAnyCorruptedRecords]");

// 如果希望控制 INFO LOG 大小，请修改以下三条配置项
// 设置非 0 值，则每过 log_file_time_to_roll 秒，换一个新的 INFO LOG 文件
DEFINE_uint64(terark$db_opt$log_file_time_to_roll, 0,
              "Time for the info log file to roll (in seconds)");

// 设置非 0 值，则 INFO LOG 超过 max_log_file_size，换一个新的 INFO LOG 文件
DEFINE_uint64(terark$db_opt$max_log_file_size, 0,
              "Max info log file size (in bytes)");

// 最多保留 INFO LOG 文件个数
DEFINE_uint64(terark$db_opt$keep_log_file_num, 1000, "Keep log file num");

// Flush 线程数
DEFINE_int32(terark$db_opt$max_background_flushes, 6,
             "Max background flush jobs");
// DEFINE_validator(terark$db_opt$max_background_flushes,
// terark_validate_options);

// Compaction 线程数（包括 GarbageCollaction）
DEFINE_int32(terark$db_opt$max_background_compactions, 6,
             "Max background compaction jobs (include garbage collections)");
// DEFINE_validator(terark$db_opt$max_background_compactions,
// terark_validate_options);

// GarbageCollaction 线程数
DEFINE_int32(terark$db_opt$max_background_garbage_collections, 1,
             "Max background garbage collections jobs");
// DEFINE_validator(terark$db_opt$max_background_garbage_collections,
// terark_validate_options);

// 所有 MemTable 总内存限制
// 超过后依据 write_buffer_flush_pri 配置，挑选一个 CF 来 Flush
DEFINE_uint64(terark$db_opt$db_write_buffer_size, 4ull << 30,
              "Total mem-table size limit");

// # 可选项 kFlushOldest, kFlushLargest，大概不用具体描述了，如字面意思
DEFINE_string(terark$db_opt$write_buffer_flush_pri, "kFlushLargest",
              "[kFlushOldest,kFlushLargest]");

// 单个 WAL 文件最大值，超过后选一个最久没有 Flush 的 CF 来 Flush
DEFINE_uint64(terark$db_opt$max_wal_size, 256ull << 20,
              "WAL file size limit (in bytes)");

// 所有 WAL 文件最大值，超过后选中所有还在用最旧的 WAL 文件的 CF 来 Flush
DEFINE_uint64(terark$db_opt$max_total_wal_size, 1ull << 30,
              "Total WAL file size limit (in bytes)");

// 生成的新 SST
// 会进行一些校验，内部逻辑也启用更严格的校验（主要预防内部错误），些微影响性能
DEFINE_bool(terark$db_opt$paranoid_checks, true, "Enable paranoid checks");

// SST 写入时，调用 RangeSync 的粒度
DEFINE_uint64(terark$db_opt$bytes_per_sync, 32ull << 10,
              "SST file write RangeSync bytes");

// WAL 写入时，调用 RangeSync 的粒度
DEFINE_uint64(terark$db_opt$wal_bytes_per_sync, 32ull << 10,
              "WAL file write RangeSync bytes");

// 允许 MemTable 并发写入
DEFINE_bool(terark$db_opt$allow_concurrent_memtable_write, true,
            "Allow concurrent mem-table write");

// 发生限流时，以这个流量作为初始限制
DEFINE_uint64(terark$db_opt$delayed_write_rate, 200ull << 20,
              "Write rate limit (in bytes)");

// 写线程自适应 yield 等待
DEFINE_bool(terark$db_opt$enable_write_thread_adaptive_yield, true,
            "Enable write thread adaptive yield");

// 写入相关的 Magic Number，
DEFINE_uint64(terark$db_opt$write_thread_slow_yield_usec, 3,
              "Write thread slow yield usec");

// 写入相关的 Magic Number，不建议修改
DEFINE_uint64(terark$db_opt$write_thread_max_yield_usec, 100,
              "Write thread max yield usec");

// 转移一些阻塞 IO 到后台线程，优化 Latency
DEFINE_bool(terark$db_opt$avoid_unnecessary_blocking_io, true,
            "Avoid unnecessary blocking io");

// 打开后使用 FSync 刷盘，关闭时使用 FDataSync 刷盘
DEFINE_bool(terark$db_opt$use_fsync, false,
            "Use FSync if true, otherwise FDataSync");

// TableCache 内使用 2^N 个分片
DEFINE_int32(terark$db_opt$table_cache_numshardbits, 8,
             "TableCache shading bits (2^N)");

// 每 6 小时清理一次 INFO LOG、OPTIONS 文件
DEFINE_uint64(terark$db_opt$delete_obsolete_files_period_micros, 21600000000,
              "Delete obsolete files interval (in micros)");

// Open DB 时刻使用多线程加载 SST
DEFINE_int32(terark$db_opt$max_file_opening_threads, 16, "OpenDB threads");

// Compaction 时对文件预取，通常设备为 HDD 时候启用
DEFINE_uint64(terark$db_opt$compaction_readahead_size, 0,
              "Compaction readahead size (Optimized for HDD)");

// 每个 Compaction 都用全新的 TableReader，与 compaction_readahead_size 配合
// 同，通常设备为 HDD 时候启用
DEFINE_bool(terark$db_opt$new_table_reader_for_compaction_inputs, false,
            "New TableReader for compaction");

// WritableFileWriter 写缓冲区大小
DEFINE_uint64(terark$db_opt$writable_file_max_buffer_size, 1ull << 20,
              "Writable file max buffer size");

// Windows 才关注的选项，不要搭理就是了
DEFINE_uint64(terark$db_opt$random_access_max_buffer_size, 1ull << 20,
              "Random access max buffer size");

// 异步预创建若干个 WAL 文件，避免写入流程中创建文件
DEFINE_uint64(terark$db_opt$prepare_log_writer_num, 4,
              "Pre-create WAL file num");

// 使用自适应 SpinLock 的 Mutex，暂时不响应的选项
DEFINE_bool(terark$db_opt$use_adaptive_mutex, false, "Use adaptive mutex");

// 使用 fallocate 预分配空间
DEFINE_bool(terark$db_opt$allow_fallocate, true, "Allow fallocate");

// 每过一段时间自动 Dump 一下内部的统计信息
DEFINE_uint32(terark$db_opt$stats_dump_period_sec, 600,
              "INFO LOG stats dump interval (in seconds)");

// 如果启用了 JeMalloc，Dump 时候带上内存分配信息
DEFINE_bool(terark$db_opt$dump_malloc_stats, false,
            "INFO LOG dump malloc states");

// 总是打开所有 SST，可能 OpenDB 会花一些时间，但是可以降低 P99
DEFINE_int32(terark$db_opt$max_open_files, -1,
             "Max open files (-1 for unlimited)");

////////////////////////////////////////////////////////////////////////////////
// default(page) & mvpage_cf
////////////////////////////////////////////////////////////////////////////////

// MemTable 大小
DEFINE_uint64(terark$cf_opt$mvpage_cf$write_buffer_size, 256ull << 20,
              "Mem-table size (in bytes)");

// MemTable 内 Arena 大小
DEFINE_uint64(terark$cf_opt$mvpage_cf$arena_block_size, 16ull << 20,
              "Mem-table arena size (in bytes)");

// MemTable 数量
DEFINE_int32(terark$cf_opt$mvpage_cf$max_write_buffer_number, 10,
             "Max mem-table count");

// SST 基础大小
DEFINE_uint64(terark$cf_opt$mvpage_cf$target_file_size_base, 128ull << 20,
              "SST file size base (in bytes)");

// SST 大小倍数
DEFINE_int32(terark$cf_opt$mvpage_cf$target_file_size_multiplier, 1,
             "SST file size multiplier");

// Lv1 预期大小
DEFINE_uint64(terark$cf_opt$mvpage_cf$max_bytes_for_level_base, 2ull << 30,
              "Level 1 size (in bytes)");

// Level 大小倍数
DEFINE_double(terark$cf_opt$mvpage_cf$max_bytes_for_level_multiplier, 4,
              "Level size multiplier");

// Compaction style
DEFINE_string(terark$cf_opt$mvpage_cf$compaction_style, "kCompactionStyleLevel",
              "[kCompactionStyleLevel,kCompactionStyleUniversal]");

// Level Compaction 优先级
DEFINE_string(terark$cf_opt$mvpage_cf$compaction_pri, "kByCompensatedSize",
              "[kByCompensatedSize,kOldestLargestSeqFirst,"
              "kOldestSmallestSeqFirst,kMinOverlappingRatio]");

// LSM 最大层数
DEFINE_int32(terark$cf_opt$mvpage_cf$num_levels, 6, "LSM max level");

// 动态计算每层大小
DEFINE_bool(terark$cf_opt$mvpage_cf$level_compaction_dynamic_level_bytes, true,
            "Enable dynamic level size");

// 压缩算法
DEFINE_string(terark$cf_opt$mvpage_cf$compression, "kNoCompression",
              "[kNoCompression,kSnappyCompression,kZlibCompression,"
              "kBZip2Compression,kLZ4Compression,kLZ4HCCompression,"
              "kXpressCompression,kZSTD]");

// 开启延时 Compact 策略
DEFINE_bool(terark$cf_opt$mvpage_cf$enable_lazy_compaction, true,
            "Enable lazy compaction");

// Lv0 触发 Compaction 数量
DEFINE_int32(terark$cf_opt$mvpage_cf$level0_file_num_compaction_trigger, 4,
             "Level0 compaction trigger");

// Lv0 触发限速数量
DEFINE_int32(terark$cf_opt$mvpage_cf$level0_slowdown_writes_trigger, 1000,
             "Level0 slowdown trigger");

// Lv0 触发停写数量
DEFINE_int32(terark$cf_opt$mvpage_cf$level0_stop_writes_trigger, 1000,
             "Level0 stop trigger");

// 待 Compaction 数据量限速阈值
DEFINE_uint64(terark$cf_opt$mvpage_cf$soft_pending_compaction_bytes_limit,
              1ull << 60, "Pending compaction slowdown trigger (in bytes)");

// 待 Compaction 数据量停写阈值
DEFINE_uint64(terark$cf_opt$mvpage_cf$hard_pending_compaction_bytes_limit,
              1ull << 60, "Pending compaction stop trigger (in bytes)");

// Value 分离长度阈值
DEFINE_uint64(terark$cf_opt$mvpage_cf$blob_size, 256,
              "Separate value when val_len > blob_size");

// Key 长度超过 Value 长度多少就不分离了
DEFINE_double(terark$cf_opt$mvpage_cf$blob_large_key_ratio, 0.1,
              "Inline value when key_len > val_len * ratio");

// 垃圾量超过该值触发 GC
DEFINE_double(terark$cf_opt$mvpage_cf$blob_gc_ratio, 0.0625,
              "Garbage collection trigger ratio");

// 本 CF 最大子 Compaction
DEFINE_uint32(terark$cf_opt$mvpage_cf$max_subcompactions, 2,
              "Max sub compaction of curr CF");

// 手动 Compaction 单次执行大小
DEFINE_uint64(terark$cf_opt$mvpage_cf$max_compaction_bytes, 1ull << 30,
              "Manual compaction job size (in bytes)");

// 很少出现 Get NotFound 结果
DEFINE_bool(terark$cf_opt$mvpage_cf$optimize_filters_for_hits, true,
            "Optimize filters for Get hits");

// 暂停本 CF 的后台 Compaction
DEFINE_bool(terark$cf_opt$mvpage_cf$disable_auto_compactions, false,
            "Disable CF auto compaction");

////////////////////////////////////////////////////////////////////////////////
// log_cf
////////////////////////////////////////////////////////////////////////////////

// MemTable 大小
DEFINE_uint64(terark$cf_opt$log_cf$write_buffer_size, 128ull << 20,
              "Mem-table size (in bytes)");

// MemTable 内 Arena 大小
DEFINE_uint64(terark$cf_opt$log_cf$arena_block_size, 16ull << 20,
              "Mem-table arena size (in bytes)");

// MemTable 数量
DEFINE_int32(terark$cf_opt$log_cf$max_write_buffer_number, 6,
             "Max mem-table count");

// SST 基础大小
DEFINE_uint64(terark$cf_opt$log_cf$target_file_size_base, 128ull << 20,
              "SST file size base (in bytes)");

// SST 大小倍数
DEFINE_int32(terark$cf_opt$log_cf$target_file_size_multiplier, 1,
             "SST file size multiplier");

// Lv1 预期大小
DEFINE_uint64(terark$cf_opt$log_cf$max_bytes_for_level_base, 1ull << 30,
              "Level 1 size (in bytes)");

// Level 大小倍数
DEFINE_double(terark$cf_opt$log_cf$max_bytes_for_level_multiplier, 4,
              "Level size multiplier");

// Compaction style
DEFINE_string(terark$cf_opt$log_cf$compaction_style, "kCompactionStyleLevel",
              "[kCompactionStyleLevel,kCompactionStyleUniversal]");

// Level Compaction 优先级
DEFINE_string(terark$cf_opt$log_cf$compaction_pri, "kByCompensatedSize",
              "[kByCompensatedSize,kOldestLargestSeqFirst,"
              "kOldestSmallestSeqFirst,kMinOverlappingRatio]");

// LSM 最大层数
DEFINE_int32(terark$cf_opt$log_cf$num_levels, 5, "LSM max level");

// 动态计算每层大小
DEFINE_bool(terark$cf_opt$log_cf$level_compaction_dynamic_level_bytes, true,
            "Enable dynamic level size");

// 压缩算法
DEFINE_string(terark$cf_opt$log_cf$compression, "kNoCompression",
              "[kNoCompression,kSnappyCompression,kZlibCompression,"
              "kBZip2Compression,kLZ4Compression,kLZ4HCCompression,"
              "kXpressCompression,kZSTD]");

// 开启延时 Compact 策略
DEFINE_bool(terark$cf_opt$log_cf$enable_lazy_compaction, true,
            "Enable lazy compaction");

// Lv0 触发 Compaction 数量
DEFINE_int32(terark$cf_opt$log_cf$level0_file_num_compaction_trigger, 4,
             "Level0 compaction trigger");

// Lv0 触发限速数量
DEFINE_int32(terark$cf_opt$log_cf$level0_slowdown_writes_trigger, 1000,
             "Level0 slowdown trigger");

// Lv0 触发停写数量
DEFINE_int32(terark$cf_opt$log_cf$level0_stop_writes_trigger, 1000,
             "Level0 stop trigger");

// 待 Compaction 数据量限速阈值
DEFINE_uint64(terark$cf_opt$log_cf$soft_pending_compaction_bytes_limit,
              1ull << 60, "Pending compaction slowdown trigger (in bytes)");

// 待 Compaction 数据量停写阈值
DEFINE_uint64(terark$cf_opt$log_cf$hard_pending_compaction_bytes_limit,
              1ull << 60, "Pending compaction stop trigger (in bytes)");

// Value 分离长度阈值
DEFINE_uint64(terark$cf_opt$log_cf$blob_size, 256,
              "Separate value when val_len > blob_size");

// Key 长度超过 Value 长度多少就不分离了
DEFINE_double(terark$cf_opt$log_cf$blob_large_key_ratio, 0.1,
              "Inline value when key_len > val_len * ratio");

// 垃圾量超过该值触发 GC
DEFINE_double(terark$cf_opt$log_cf$blob_gc_ratio, 0.0625,
              "Garbage collection trigger ratio");

// 本 CF 最大子 Compaction
DEFINE_uint32(terark$cf_opt$log_cf$max_subcompactions, 2,
              "Max sub compaction of curr CF");

// 手动 Compaction 单次执行大小
DEFINE_uint64(terark$cf_opt$log_cf$max_compaction_bytes, 1ull << 30,
              "Manual compaction job size (in bytes)");

// 很少出现 Get NotFound 结果
DEFINE_bool(terark$cf_opt$log_cf$optimize_filters_for_hits, true,
            "Optimize filters for Get hits");

// 暂停本 CF 的后台 Compaction
DEFINE_bool(terark$cf_opt$log_cf$disable_auto_compactions, false,
            "Disable CF auto compaction");

////////////////////////////////////////////////////////////////////////////////
// meta_cf
////////////////////////////////////////////////////////////////////////////////

// MemTable 大小
DEFINE_uint64(terark$cf_opt$meta_cf$write_buffer_size, 32ull << 20,
              "Mem-table size (in bytes)");

// MemTable 内 Arena 大小
DEFINE_uint64(terark$cf_opt$meta_cf$arena_block_size, 4ull << 20,
              "Mem-table arena size (in bytes)");

// MemTable 数量
DEFINE_int32(terark$cf_opt$meta_cf$max_write_buffer_number, 6,
             "Max mem-table count");

// SST 基础大小
DEFINE_uint64(terark$cf_opt$meta_cf$target_file_size_base, 64ull << 20,
              "SST file size base (in bytes)");
// DEFINE_validator(terark$cf_opt$meta_cf$target_file_size_base,
// terark_validate_options);

// SST 大小倍数
DEFINE_int32(terark$cf_opt$meta_cf$target_file_size_multiplier, 1,
             "SST file size multiplier");
// DEFINE_validator(terark$cf_opt$meta_cf$target_file_size_multiplier,
// terark_validate_options);

// Lv1 预期大小
DEFINE_uint64(terark$cf_opt$meta_cf$max_bytes_for_level_base, 256ull << 20,
              "Level 1 size (in bytes)");
// DEFINE_validator(terark$cf_opt$meta_cf$max_bytes_for_level_base,
// terark_validate_options);

// Level 大小倍数
DEFINE_double(terark$cf_opt$meta_cf$max_bytes_for_level_multiplier, 4,
              "Level size multiplier");
// DEFINE_validator(terark$cf_opt$meta_cf$max_bytes_for_level_multiplier,
// terark_validate_options);

// Compaction style
DEFINE_string(terark$cf_opt$meta_cf$compaction_style, "kCompactionStyleLevel",
              "[kCompactionStyleLevel,kCompactionStyleUniversal]");

// Level Compaction 优先级
DEFINE_string(terark$cf_opt$meta_cf$compaction_pri, "kByCompensatedSize",
              "[kByCompensatedSize,kOldestLargestSeqFirst,"
              "kOldestSmallestSeqFirst,kMinOverlappingRatio]");

// LSM 最大层数
DEFINE_int32(terark$cf_opt$meta_cf$num_levels, 4, "LSM max level");

// 动态计算每层大小
DEFINE_bool(terark$cf_opt$meta_cf$level_compaction_dynamic_level_bytes, true,
            "Enable dynamic level size");

// 压缩算法
DEFINE_string(terark$cf_opt$meta_cf$compression, "kNoCompression",
              "[kNoCompression,kSnappyCompression,kZlibCompression,"
              "kBZip2Compression,kLZ4Compression,kLZ4HCCompression,"
              "kXpressCompression,kZSTD]");
// DEFINE_validator(terark$cf_opt$meta_cf$compression, terark_validate_options);

// 开启延时 Compact 策略
DEFINE_bool(terark$cf_opt$meta_cf$enable_lazy_compaction, true,
            "Enable lazy compaction");

// Lv0 触发 Compaction 数量
DEFINE_int32(terark$cf_opt$meta_cf$level0_file_num_compaction_trigger, 2,
             "Level0 compaction trigger");
// DEFINE_validator(terark$cf_opt$meta_cf$level0_file_num_compaction_trigger,
// terark_validate_options);

// Lv0 触发限速数量
DEFINE_int32(terark$cf_opt$meta_cf$level0_slowdown_writes_trigger, 1000,
             "Level0 slowdown trigger");
// DEFINE_validator(terark$cf_opt$meta_cf$level0_slowdown_writes_trigger,
// terark_validate_options);

// Lv0 触发停写数量
DEFINE_int32(terark$cf_opt$meta_cf$level0_stop_writes_trigger, 1000,
             "Level0 stop trigger");
// DEFINE_validator(terark$cf_opt$meta_cf$level0_stop_writes_trigger,
// terark_validate_options);

// 待 Compaction 数据量限速阈值
DEFINE_uint64(terark$cf_opt$meta_cf$soft_pending_compaction_bytes_limit,
              1ull << 60, "Pending compaction slowdown trigger (in bytes)");
// DEFINE_validator(terark$cf_opt$meta_cf$soft_pending_compaction_bytes_limit,
// terark_validate_options);

// 待 Compaction 数据量停写阈值
DEFINE_uint64(terark$cf_opt$meta_cf$hard_pending_compaction_bytes_limit,
              1ull << 60, "Pending compaction stop trigger (in bytes)");
// DEFINE_validator(terark$cf_opt$meta_cf$hard_pending_compaction_bytes_limit,
// terark_validate_options);

// Value 分离长度阈值
DEFINE_uint64(terark$cf_opt$meta_cf$blob_size, uint64_t(-1),
              "Separate value when val_len > blob_size");
// DEFINE_validator(terark$cf_opt$meta_cf$blob_size, terark_validate_options);

// Key 长度超过 Value 长度多少就不分离了
DEFINE_double(terark$cf_opt$meta_cf$blob_large_key_ratio, 0.1,
              "Inline value when key_len > val_len * ratio");
// DEFINE_validator(terark$cf_opt$meta_cf$blob_large_key_ratio,
// terark_validate_options);

// 垃圾量超过该值触发 GC
DEFINE_double(terark$cf_opt$meta_cf$blob_gc_ratio, 0.0625,
              "Garbage collection trigger ratio");
// DEFINE_validator(terark$cf_opt$meta_cf$blob_gc_ratio,
// terark_validate_options);

// 本 CF 最大子 Compaction
DEFINE_uint32(terark$cf_opt$meta_cf$max_subcompactions, 2,
              "Max sub compaction of curr CF");
// DEFINE_validator(terark$cf_opt$meta_cf$max_subcompactions,
// terark_validate_options);

// 手动 Compaction 单次执行大小
DEFINE_uint64(terark$cf_opt$meta_cf$max_compaction_bytes, 1ull << 30,
              "Manual compaction job size (in bytes)");
// DEFINE_validator(terark$cf_opt$meta_cf$max_compaction_bytes,
// terark_validate_options);

// 很少出现 Get NotFound 结果
DEFINE_bool(terark$cf_opt$meta_cf$optimize_filters_for_hits, true,
            "Optimize filters for Get hits");
// DEFINE_validator(terark$cf_opt$meta_cf$optimize_filters_for_hits,
// terark_validate_options);

// 暂停本 CF 的后台 Compaction
DEFINE_bool(terark$cf_opt$meta_cf$disable_auto_compactions, false,
            "Disable CF auto compaction");
// DEFINE_validator(terark$cf_opt$meta_cf$disable_auto_compactions,
// terark_validate_options);

////////////////////////////////////////////////////////////////////////////////
// delta_log_cf
////////////////////////////////////////////////////////////////////////////////

// MemTable 大小
DEFINE_uint64(terark$cf_opt$delta_log_cf$write_buffer_size, 256ull << 20,
              "Mem-table size (in bytes)");
// DEFINE_validator(terark$cf_opt$delta_log_cf$write_buffer_size,
// terark_validate_options);

// MemTable 内 Arena 大小
DEFINE_uint64(terark$cf_opt$delta_log_cf$arena_block_size, 16ull << 20,
              "Mem-table arena size (in bytes)");
// DEFINE_validator(terark$cf_opt$delta_log_cf$arena_block_size,
// terark_validate_options);

// MemTable 数量
DEFINE_int32(terark$cf_opt$delta_log_cf$max_write_buffer_number, 10,
             "Max mem-table count");
// DEFINE_validator(terark$cf_opt$delta_log_cf$max_write_buffer_number,
// terark_validate_options);

// SST 基础大小
DEFINE_uint64(terark$cf_opt$delta_log_cf$target_file_size_base, 64ull << 20,
              "SST file size base (in bytes)");
// DEFINE_validator(terark$cf_opt$delta_log_cf$target_file_size_base,
// terark_validate_options);

// SST 大小倍数
DEFINE_int32(terark$cf_opt$delta_log_cf$target_file_size_multiplier, 1,
             "SST file size multiplier");
// DEFINE_validator(terark$cf_opt$delta_log_cf$target_file_size_multiplier,
// terark_validate_options);

// Lv1 预期大小
DEFINE_uint64(terark$cf_opt$delta_log_cf$max_bytes_for_level_base, 1ull << 30,
              "Level 1 size (in bytes)");
// DEFINE_validator(terark$cf_opt$delta_log_cf$max_bytes_for_level_base,
// terark_validate_options);

// Level 大小倍数
DEFINE_double(terark$cf_opt$delta_log_cf$max_bytes_for_level_multiplier, 4,
              "Level size multiplier");
// DEFINE_validator(terark$cf_opt$delta_log_cf$max_bytes_for_level_multiplier,
// terark_validate_options);

// Compaction style
DEFINE_string(terark$cf_opt$delta_log_cf$compaction_style,
              "kCompactionStyleLevel",
              "[kCompactionStyleLevel,kCompactionStyleUniversal]");

// Level Compaction 优先级
DEFINE_string(terark$cf_opt$delta_log_cf$compaction_pri, "kByCompensatedSize",
              "[kByCompensatedSize,kOldestLargestSeqFirst,"
              "kOldestSmallestSeqFirst,kMinOverlappingRatio]");

// LSM 最大层数
DEFINE_int32(terark$cf_opt$delta_log_cf$num_levels, 3, "LSM max level");

// 动态计算每层大小
DEFINE_bool(terark$cf_opt$delta_log_cf$level_compaction_dynamic_level_bytes,
            true, "Enable dynamic level size");

// 压缩算法
DEFINE_string(terark$cf_opt$delta_log_cf$compression, "kNoCompression",
              "[kNoCompression,kSnappyCompression,kZlibCompression,"
              "kBZip2Compression,kLZ4Compression,kLZ4HCCompression,"
              "kXpressCompression,kZSTD]");
// DEFINE_validator(terark$cf_opt$delta_log_cf$compression,
// terark_validate_options);

// 开启延时 Compact 策略
DEFINE_bool(terark$cf_opt$delta_log_cf$enable_lazy_compaction, true,
            "Enable lazy compaction");

// Lv0 触发 Compaction 数量
DEFINE_int32(terark$cf_opt$delta_log_cf$level0_file_num_compaction_trigger, 8,
             "Level0 compaction trigger");
// DEFINE_validator(terark$cf_opt$delta_log_cf$level0_file_num_compaction_trigger,
// terark_validate_options);

// Lv0 触发限速数量
DEFINE_int32(terark$cf_opt$delta_log_cf$level0_slowdown_writes_trigger, 1000,
             "Level0 slowdown trigger");
// DEFINE_validator(terark$cf_opt$delta_log_cf$level0_slowdown_writes_trigger,
// terark_validate_options);

// Lv0 触发停写数量
DEFINE_int32(terark$cf_opt$delta_log_cf$level0_stop_writes_trigger, 1000,
             "Level0 stop trigger");
// DEFINE_validator(terark$cf_opt$delta_log_cf$level0_stop_writes_trigger,
// terark_validate_options);

// 待 Compaction 数据量限速阈值
DEFINE_uint64(terark$cf_opt$delta_log_cf$soft_pending_compaction_bytes_limit,
              1ull << 60, "Pending compaction slowdown trigger (in bytes)");
// DEFINE_validator(terark$cf_opt$delta_log_cf$soft_pending_compaction_bytes_limit,
// terark_validate_options);

// 待 Compaction 数据量停写阈值
DEFINE_uint64(terark$cf_opt$delta_log_cf$hard_pending_compaction_bytes_limit,
              1ull << 60, "Pending compaction stop trigger (in bytes)");
// DEFINE_validator(terark$cf_opt$delta_log_cf$hard_pending_compaction_bytes_limit,
// terark_validate_options);

// Value 分离长度阈值
DEFINE_uint64(terark$cf_opt$delta_log_cf$blob_size, 64,
              "Separate value when val_len > blob_size");
// DEFINE_validator(terark$cf_opt$delta_log_cf$blob_size,
// terark_validate_options);

// Key 长度超过 Value 长度多少就不分离了
DEFINE_double(terark$cf_opt$delta_log_cf$blob_large_key_ratio, 0.1,
              "Inline value when key_len > val_len * ratio");
// DEFINE_validator(terark$cf_opt$delta_log_cf$blob_large_key_ratio,
// terark_validate_options);

// 垃圾量超过该值触发 GC
DEFINE_double(terark$cf_opt$delta_log_cf$blob_gc_ratio, 0.0625,
              "Garbage collection trigger ratio");
// DEFINE_validator(terark$cf_opt$delta_log_cf$blob_gc_ratio,
// terark_validate_options);

// 本 CF 最大子 Compaction
DEFINE_uint32(terark$cf_opt$delta_log_cf$max_subcompactions, 2,
              "Max sub compaction of curr CF");
// DEFINE_validator(terark$cf_opt$delta_log_cf$max_subcompactions,
// terark_validate_options);

// 手动 Compaction 单次执行大小
DEFINE_uint64(terark$cf_opt$delta_log_cf$max_compaction_bytes, 1ull << 30,
              "Manual compaction job size (in bytes)");
// DEFINE_validator(terark$cf_opt$delta_log_cf$max_compaction_bytes,
// terark_validate_options);

// 很少出现 Get NotFound 结果
DEFINE_bool(terark$cf_opt$delta_log_cf$optimize_filters_for_hits, true,
            "Optimize filters for Get hits");
// DEFINE_validator(terark$cf_opt$delta_log_cf$optimize_filters_for_hits,
// terark_validate_options);

// 暂停本 CF 的后台 Compaction
DEFINE_bool(terark$cf_opt$delta_log_cf$disable_auto_compactions, false,
            "Disable CF auto compaction");
// DEFINE_validator(terark$cf_opt$delta_log_cf$disable_auto_compactions,
// terark_validate_options);

////////////////////////////////////////////////////////////////////////////////
// BlockBasedTable
////////////////////////////////////////////////////////////////////////////////

// Indicating if we'd put index/filter blocks to the block cache.
// If not specified, each "table reader" object will pre-load index/filter
// block during table initialization.
DEFINE_bool(terark$misc$block_based$cache_index_and_filter_blocks, true,
            "Cache index and filter blocks");

// If cache_index_and_filter_blocks is enabled, cache index and filter
// blocks with high priority. If set to true, depending on implementation of
// block cache, index and filter blocks may be less likely to be evicted
// than data blocks.
DEFINE_bool(
    terark$misc$block_based$cache_index_and_filter_blocks_with_high_priority,
    true, "Cache index and filter blocks with high priority");

// if cache_index_and_filter_blocks is true and the below is true, then
// filter and index blocks are stored in the cache, but a reference is
// held in the "table reader" object so the blocks are pinned and only
// evicted from cache when the table reader is freed.
DEFINE_bool(terark$misc$block_based$pin_l0_filter_and_index_blocks_in_cache,
            false, "Pin Lv0 filter and index blocks in cache");

// If cache_index_and_filter_blocks is true and the below is true, then
// the top-level index of partitioned filter and index blocks are stored in
// the cache, but a reference is held in the "table reader" object so the
// blocks are pinned and only evicted from cache when the table reader is
// freed. This is not limited to l0 in LSM tree.
DEFINE_bool(terark$misc$block_based$pin_top_level_index_and_filter, false,
            "Pin top level index and filter");

// Use the specified checksum type. Newly created table files will be
// protected with this checksum type. Old table files will still be readable,
// even though they have different checksum type.
DEFINE_string(terark$misc$block_based$checksum, "kCRC32c",
              "[kNoChecksum,kCRC32c,kxxHash,kxxHash64]");

// Disable block cache. If this is set to true,
// then no block cache should be used, and the block_cache should
// point to a nullptr object.
DEFINE_bool(terark$misc$block_based$no_block_cache, false,
            "Disable block cache");

// Approximate size of user data packed per block.  Note that the
// block size specified here corresponds to uncompressed data.  The
// actual size of the unit read from disk may be smaller if
// compression is enabled.  This parameter can be changed dynamically.
DEFINE_uint64(terark$misc$block_based$block_size, 8ull << 10,
              "Block Size (in bytes)");

// This is used to close a block before it reaches the configured
// 'block_size'. If the percentage of free space in the current block is less
// than this specified number and adding a new record to the block will
// exceed the configured block size, then this block will be closed and the
// new record will be written to the next block.
DEFINE_int32(terark$misc$block_based$block_size_deviation, 10,
             "Block size deviation");

// Number of keys between restart points for delta encoding of keys.
// This parameter can be changed dynamically.  Most clients should
// leave this parameter alone.  The minimum value allowed is 1.  Any smaller
// value will be silently overwritten with 1.
DEFINE_int32(terark$misc$block_based$block_restart_interval, 16,
             "Block restart interval");

// Same as block_restart_interval but used for the index block.
DEFINE_int32(terark$misc$block_based$index_block_restart_interval, 1,
             "Index block restart interval");

// Block size for partitioned metadata. Currently applied to indexes when
// kTwoLevelIndexSearch is used and to filters when partition_filters is used.
// Note: Since in the current implementation the filters and index partitions
// are aligned, an index/filter block is created when either index or filter
// block size reaches the specified limit.
// Note: this limit is currently applied to only index blocks; a filter
// partition is cut right after an index block is cut
DEFINE_uint64(terark$misc$block_based$metadata_block_size, 4096,
              "Metadata block size");

// Use partitioned full filters for each SST file. This option is
// incompatible with block-based filters.
DEFINE_bool(terark$misc$block_based$partition_filters, false,
            "Use partition filters");

// 过滤器配置
DEFINE_string(terark$misc$block_based$filter_policy, "bloomfilter:10:false",
              "Filter config");

// If true, place whole keys in the filter (not just prefixes).
// This must generally be true for gets to be efficient.
DEFINE_bool(terark$misc$block_based$whole_key_filtering, true,
            "Whole key filtering");

// We currently have three versions:
// 0 -- This version is currently written out by all RocksDB's versions by
// default.  Can be read by really old RocksDB's. Doesn't support changing
// checksum (default is CRC32).
// 1 -- Can be read by RocksDB's versions since 3.0. Supports non-default
// checksum, like xxHash. It is written by RocksDB when
// BlockBasedTableOptions::checksum is something other than kCRC32c. (version
// 0 is silently upconverted)
// 2 -- Can be read by RocksDB's versions since 3.10. Changes the way we
// encode compressed blocks with LZ4, BZip2 and Zlib compression. If you
// don't plan to run RocksDB before version 3.10, you should probably use
// this.
// 3 -- Can be read by RocksDB's versions since 5.15. Changes the way we
// encode the keys in index blocks. If you don't plan to run RocksDB before
// version 5.15, you should probably use this.
// This option only affects newly written tables. When reading existing
// tables, the information about version is read from the footer.
// 4 -- Can be read by RocksDB's versions since 5.16. Changes the way we
// encode the values in index blocks. If you don't plan to run RocksDB before
// version 5.16 and you are using index_block_restart_interval > 1, you should
// probably use this as it would reduce the index size.
// This option only affects newly written tables. When reading existing
// tables, the information about version is read from the footer.
DEFINE_uint32(terark$misc$block_based$format_version, 2, "Format version");

// Verify that decompressing the compressed block gives back the input. This
// is a verification mode that we use to detect bugs in compression
// algorithms.
DEFINE_bool(terark$misc$block_based$verify_compression, false,
            "Verify compression");

// If used, For every data block we load into memory, we will create a bitmap
// of size ((block_size / `read_amp_bytes_per_bit`) / 8) bytes. This bitmap
// will be used to figure out the percentage we actually read of the blocks.
//
// When this feature is used Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES and
// Tickers::READ_AMP_TOTAL_READ_BYTES can be used to calculate the
// read amplification using this formula
// (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
//
// value  =>  memory usage (percentage of loaded blocks memory)
// 1      =>  12.50 %
// 2      =>  06.25 %
// 4      =>  03.12 %
// 8      =>  01.56 %
// 16     =>  00.78 %
//
// Note: This number must be a power of 2, if not it will be sanitized
// to be the next lowest power of 2, for example a value of 7 will be
// treated as 4, a value of 19 will be treated as 16.
//
// Default: 0 (disabled)
DEFINE_uint32(terark$misc$block_based$read_amp_bytes_per_bit, 0,
              "Read amp bytes per bit");

// Store index blocks on disk in compressed format. Changing this option to
// false  will avoid the overhead of decompression if index blocks are evicted
// and read back
DEFINE_bool(terark$misc$block_based$enable_index_compression, true,
            "Enable index compression");

// Align data blocks on lesser of page size and block size
DEFINE_bool(terark$misc$block_based$block_align, false, "Block align");

////////////////////////////////////////////////////////////////////////////////
// TerarkZipTable
////////////////////////////////////////////////////////////////////////////////

// Max nest louds trie level
DEFINE_int32(terark$misc$terark_zip$indexNestLevel, 3, "Max index next level");

// 0 : check sum nothing
// 1 : check sum meta data and index, check on file load
// 2 : check sum all data, not check on file load, checksum is for
//     each record, this incurs 4 bytes overhead for each record
// 3 : check sum all data with one checksum value, not checksum each record,
//     if checksum doesn't match, load will fail
DEFINE_int32(terark$misc$terark_zip$checksumLevel, 2, "Checksum level");

// checksumSmallValSize only makes sense when checksumLevel == 2(record
// level) any value that is less than checksumSmallValSize will be verified
// by crc16c otherwise crc32c is used
DEFINE_int32(terark$misc$terark_zip$checksumSmallValSize, 40,
             "Checksum small value size");

// Entropy alg for TerarkZipTable
DEFINE_string(terark$misc$terark_zip$entropyAlgo, "kNoEntropy",
              "[kNoEntropy,kHuffman,kFSE]");

//     -1 : only last level using terarkZip
//          this is equivalent to terarkZipMinLevel == num_levels-1
//     -2 : disable terarkZip
// others : use terarkZip when curlevel >= terarkZipMinLevel
//          this includes the two special cases:
//                   == 0 : all levels using terarkZip
//          >= num_levels : all levels using fallback TableFactory
// it shown that set terarkZipMinLevel = 0 is the best choice
// if mixed with rocksdb's native SST, those SSTs may using too much
// memory & SSD, which degrades the performance
DEFINE_int32(terark$misc$terark_zip$terarkZipMinLevel, 0,
             "Min level to use TerarkZipTable");

// please always set to 0
// unless you know what it really does for
// 0 : no debug
// 1 : enable infomation output
// 2 : verify 2nd pass iter keys & values
// 3 : dump 1st & 2nd pass data to file
DEFINE_uint32(terark$misc$terark_zip$debugLevel, 0, "Debug Level");

// Enable nest level if core string size * indexNestScale > total size
DEFINE_uint32(terark$misc$terark_zip$indexNestScale, 8, "Index nest scale");

// Crit bit trie hash bits
DEFINE_uint32(terark$misc$terark_zip$cbtHashBits, 7, "Crit bit trie hash bits");

// Crit bit trie entry pre block
DEFINE_uint32(terark$misc$terark_zip$cbtEntryPerTrie, 32768,
              "Crit bit trie entry pre block");

// Use Crit bit trie when key_avg_size > cbtMinKeySize
DEFINE_uint32(terark$misc$terark_zip$cbtMinKeySize, 16,
              "Crit bit trie avg key size (in bytes)");

// Use Crit bit trie when total_key_size > key_value_size * cbtMinKeyRatio
DEFINE_double(terark$misc$terark_zip$cbtMinKeyRatio, 0.1,
              "Crit bit trie min key ratio");

// Enable compression probe
DEFINE_bool(terark$misc$terark_zip$enableCompressionProbe, true,
            "Enable compression probe");

// Use suffix array local match
DEFINE_bool(terark$misc$terark_zip$useSuffixArrayLocalMatch, false,
            "Use suffix array local match");

// Warm up index on open
DEFINE_bool(terark$misc$terark_zip$warmUpIndexOnOpen, true,
            "Warm up index on open");

// Warm up value on open
DEFINE_bool(terark$misc$terark_zip$warmUpValueOnOpen, false,
            "Warm up value on open");

// Disable second pass iterator
DEFINE_bool(terark$misc$terark_zip$disableSecondPassIter, false,
            "Disable second pass iterator");

// Enable entropy store
DEFINE_bool(terark$misc$terark_zip$enableEntropyStore, false,
            "Enable entropy store");

// Disable compress dict
DEFINE_bool(terark$misc$terark_zip$disableCompressDict, false,
            "Disable compress dict");

// Optimize for CPU L3 cache
DEFINE_bool(terark$misc$terark_zip$optimizeCpuL3Cache, true,
            "Optimize for CPU L3 cache");

// Force meta in memory
DEFINE_bool(terark$misc$terark_zip$forceMetaInMemory, false,
            "Force meta in memory");

// -1: dont use temp file for  any  index build
//  0: only use temp file for large index build, smart
//  1: only use temp file for large index build, same as NLT.tmpLevel
//  2: only use temp file for large index build, same as NLT.tmpLevel
//  3: only use temp file for large index build, same as NLT.tmpLevel
//  4: only use temp file for large index build, same as NLT.tmpLevel
//  5:      use temp file for  all  index build
DEFINE_int32(terark$misc$terark_zip$indexTempLevel, 0, "Index temp level");

// Offset array compress block size
DEFINE_uint32(terark$misc$terark_zip$offsetArrayBlockUnits, 128,
              "Offset array compress block size");

// DictZip sample ratio
DEFINE_double(terark$misc$terark_zip$sampleRatio, 0.01, "DictZip sample ratio");

// Nest louds trie type
DEFINE_string(terark$misc$terark_zip$indexType, "Mixed_XL_256_32_FL",
              "[IL_256_32,IL_256_32_FL,Mixed_SE_512,Mixed_SE_512_32_FL,Mixed_"
              "IL_256,Mixed_IL_256_32_FL,Mixed_XL_256,"
              "Mixed_XL_256_32_FL,SE_512_64,SE_512_64_FL]");

// Soft zip working memory limit
DEFINE_uint64(terark$misc$terark_zip$softZipWorkingMemLimit, 8ull << 30,
              "Soft zip working memory limit");

// Hoft zip working memory limit
DEFINE_uint64(terark$misc$terark_zip$hardZipWorkingMemLimit, 16ull << 30,
              "Hoft zip working memory limit");

// Small task memory limit
DEFINE_uint64(terark$misc$terark_zip$smallTaskMemory, 1200ull << 20,
              "Small task memory limit");

// use dictZip for value when average value length >= minDictZipValueSize
// otherwise do not use dictZip
DEFINE_uint32(terark$misc$terark_zip$minDictZipValueSize, 64ull << 20,
              "Min DictZip value size");

// should be a small value, typically 0.001
// default is to disable indexCache, because the improvement
// is about only 10% when set to 0.001
DEFINE_double(terark$misc$terark_zip$indexCacheRatio, 0.001,
              "Index cache ratio");

// Single index min size
DEFINE_uint64(terark$misc$terark_zip$singleIndexMinSize, 8ull << 20,
              "Single index min size");

// Single index max size
DEFINE_uint64(terark$misc$terark_zip$singleIndexMaxSize, 0x1E0000000,
              "Single index max size");  // 7.5G

///  < 0: do not use pread
/// == 0: always use pread
///  > 0: use pread if BlobStore avg record len > minPreadLen
DEFINE_int32(terark$misc$terark_zip$minPreadLen, 0,
             "Value min size to pread (in bytes)");

// to reduce lock competition
DEFINE_int32(terark$misc$terark_zip$cacheShards, 257,
             "User space block cache shard count");

// non-zero implies direct io read
DEFINE_uint64(terark$misc$terark_zip$cacheCapacityBytes, 0,
              "User space block cache capacity (in bytes)");

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

uint64_t nr_history_fault_io = 0;
DEFINE_bool(io_fault_switch, false, "IO falut inject switch");
static bool validate_io_fault_switch(const char *name, bool value) {
  nr_history_fault_io = 0;
  return true;
}
// DEFINE_validator(io_fault_switch, &validate_io_fault_switch);

DEFINE_uint32(io_fault_data_type, 0, "0:page 1:log 2:meta 3:delta log");
static bool validate_io_fault_data_type(const char *name, uint32_t value) {
  if (value <= 3) {
    nr_history_fault_io = 0;
    return true;
  }
  return false;
}
// DEFINE_validator(io_fault_data_type, &validate_io_fault_data_type);

DEFINE_uint32(io_fault_op_type, 0,
              "0:none 1:read 2:write 3:up seek 4:low seek 5:fetch keys 6:range "
              "query 7:delete 8:batch read 9:batch write 10:delete range");
static bool validate_io_fault_op_type(const char *name, uint32_t value) {
  if (value <= 10) {
    nr_history_fault_io = 0;
    return true;
  }
  return false;
}
// DEFINE_validator(io_fault_op_type, &validate_io_fault_op_type);

DEFINE_uint32(io_fault_interval, 100,
              "interval of fault IO,range[1,UINT32_MAX]");

DEFINE_uint32(io_fault_type, 0, "0:error 1:delay");
struct IORequest {
  enum IORequestType {
    IO_REQUEST_NONE = 0,
    IO_REQUEST_READ = 1,
    IO_REQUEST_WRITE = 2,
  };

  // 不要更改顺序
  enum IODataType {
    IO_DATA_PAGE = 0,      // database page
    IO_DATA_LOG = 1,       // redo log blocks
    IO_DATA_META = 2,      // segment meta data
    IO_DATE_PAGE_LOG = 3,  // page log (delta log)
    IO_DATE_MVPAGE = 4,    // multi-version page
    IO_DATA_COM = 5,
    NUM_IO_DATA_TYPES
  };

  IORequestType request_type_;
  Status status_;

  IORequest(IORequestType request_type = IO_REQUEST_NONE)
      : request_type_(request_type) {}
};
// DEFINE_validator(io_fault_interval, &validate_io_fault_interval);
class TerarkdbStorage {
 public:
  TerarkdbStorage(const std::string &work_dir_);
  ~TerarkdbStorage();
  rocksdb::Status init_db_options();
  rocksdb::Status init_cf_options();
  void clear_whole_cf(uint32_t cf);
  Status write(IORequest::IODataType type, const Slice &key,
               const Slice &value);
  Status start();
  Status stop();

 private:
  std::string work_dir_;
  const std::string cf_names_[IORequest::IODataType::NUM_IO_DATA_TYPES];

  rocksdb::DB *db_;
  rocksdb::DBOptions db_options_;
  std::shared_ptr<rocksdb::RateLimiter> rate_limiter_;
  std::shared_ptr<rocksdb::SstFileManager> sst_file_manager_;
  /* 0:PageCF 1:LogCF 2:MetaCF */
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  std::vector<rocksdb::ColumnFamilyOptions> cf_options_;
};
rocksdb::Status TerarkdbStorage::init_db_options() {
  db_options_.create_if_missing = true;
  db_options_.create_missing_column_families = true;
  db_options_.error_if_exists = false;
  db_options_.allow_mmap_reads = true;

  std::unordered_map<std::string, std::string> dbname_dir_map;
  auto s = rocksdb::StringToMap(FLAGS_terark$misc$db_log_dir, &dbname_dir_map);
  if (!s.ok()) {
    return rocksdb::Status::InvalidArgument("Unable to parse DBOptions:",
                                            "db_log_dir");
  }
  auto find = dbname_dir_map.find(work_dir_);
  if (find != dbname_dir_map.end()) {
    db_options_.db_log_dir = find->second;
  }
  dbname_dir_map.clear();
  s = rocksdb::StringToMap(FLAGS_terark$misc$wal_dir, &dbname_dir_map);
  if (!s.ok()) {
    return rocksdb::Status::InvalidArgument("Unable to parse DBOptions:",
                                            "wal_dir");
  }
  find = dbname_dir_map.find(work_dir_);
  if (find != dbname_dir_map.end()) {
    db_options_.wal_dir = find->second;
  }

  rate_limiter_.reset(rocksdb::NewGenericRateLimiter(
      FLAGS_terark$misc$rate_limiter_bytes, 1000 /* refill_period_us */));
  db_options_.rate_limiter = rate_limiter_;

  sst_file_manager_.reset(rocksdb::NewSstFileManager(
      rocksdb::Env::Default(), db_options_.info_log,
      std::string() /* trash_dir */, FLAGS_terark$misc$rate_limiter_bytes,
      true /* delete_existing_trash */, nullptr /* status */,
      1 /* max_trash_db_ratio */, 32ull << 20 /* bytes_max_delete_chunk */));
  db_options_.sst_file_manager = sst_file_manager_;

  s = rocksdb::GetDBOptionsFromMap(
      db_options_, TerarkDBGetOptionsMapFromGFLAGS("terark$db_opt$"),
      &db_options_);
  if (!s.ok()) {
    return s;
  }

  rocksdb::TerarkZipDeleteTempFiles(work_dir_);  // call once
  assert(db_options_.env == rocksdb::Env::Default());
  //  terark_update_backgroup_threads(FLAGS_terark$db_opt$max_background_flushes,
  //                                  FLAGS_terark$db_opt$max_background_compactions,
  //                                  FLAGS_terark$misc$threads_reserve_factor);
  return s;
}

rocksdb::Status TerarkdbStorage::init_cf_options() {
  cf_handles_.resize(IORequest::IODataType::NUM_IO_DATA_TYPES);
  for (uint32_t i = 0; i < IORequest::IODataType::NUM_IO_DATA_TYPES; i++) {
    cf_handles_[i] = nullptr;
  }

  cf_options_.resize(IORequest::IODataType::NUM_IO_DATA_TYPES);

  rocksdb::Status s;

  std::shared_ptr<rocksdb::TableFactory> block_based_table_factory;
  rocksdb::BlockBasedTableOptions block_based_table_options;
  //  block_based_table_options.block_cache = TerarkdbStorage::cache;

  s = rocksdb::GetBlockBasedTableOptionsFromMap(
      block_based_table_options,
      TerarkDBGetOptionsMapFromGFLAGS("terark$misc$block_based$"),
      &block_based_table_options);
  if (!s.ok()) {
    return s;
  }
  block_based_table_factory.reset(
      NewBlockBasedTableFactory(block_based_table_options));

  std::shared_ptr<rocksdb::TableFactory> terark_zip_table_factory;
  rocksdb::TerarkZipTableOptions terark_zip_table_options;
  terark_zip_table_options.localTempDir = work_dir_;

  s = rocksdb::GetTerarkZipTableOptionsFromMap(
      terark_zip_table_options,
      TerarkDBGetOptionsMapFromGFLAGS("terark$misc$terark_zip$"),
      &terark_zip_table_options);
  if (!s.ok()) {
    return s;
  }
  if (!FLAGS_terark$misc$enable_terark_zip_table) {
    terark_zip_table_options.terarkZipMinLevel = -2;
  }
  terark_zip_table_factory.reset(rocksdb::NewTerarkZipTableFactory(
      terark_zip_table_options, block_based_table_factory));

  // default(page)
  rocksdb::ColumnFamilyOptions &page_cf_option =
      cf_options_[IORequest::IODataType::IO_DATA_PAGE];
  page_cf_option.table_factory = terark_zip_table_factory;
  s = rocksdb::GetColumnFamilyOptionsFromMap(
      page_cf_option,
      TerarkDBGetOptionsMapFromGFLAGS("terark$cf_opt$mvpage_cf$"),
      &page_cf_option);
  if (!s.ok()) {
    return s;
  }

  // log cf
  rocksdb::ColumnFamilyOptions &log_cf_option =
      cf_options_[IORequest::IODataType::IO_DATA_LOG];
  log_cf_option.table_factory = terark_zip_table_factory;
  s = rocksdb::GetColumnFamilyOptionsFromMap(
      log_cf_option, TerarkDBGetOptionsMapFromGFLAGS("terark$cf_opt$log_cf$"),
      &log_cf_option);
  if (!s.ok()) {
    return s;
  }

  // meta cf
  rocksdb::ColumnFamilyOptions &meta_cf_option =
      cf_options_[IORequest::IODataType::IO_DATA_META];
  meta_cf_option.table_factory = terark_zip_table_factory;
  s = rocksdb::GetColumnFamilyOptionsFromMap(
      meta_cf_option, TerarkDBGetOptionsMapFromGFLAGS("terark$cf_opt$meta_cf$"),
      &meta_cf_option);
  if (!s.ok()) {
    return s;
  }

  // delta log cf
  rocksdb::ColumnFamilyOptions &delta_log_cf_option =
      cf_options_[IORequest::IODataType::IO_DATE_PAGE_LOG];
  delta_log_cf_option.table_factory = terark_zip_table_factory;
  s = rocksdb::GetColumnFamilyOptionsFromMap(
      delta_log_cf_option,
      TerarkDBGetOptionsMapFromGFLAGS("terark$cf_opt$delta_log_cf$"),
      &delta_log_cf_option);
  if (!s.ok()) {
    return s;
  }

  // mvpage cf (same as page)
  cf_options_[IORequest::IODataType::IO_DATE_MVPAGE] =
      cf_options_[IORequest::IODataType::IO_DATA_PAGE];
  // COM cf (same as meta)
  cf_options_[IORequest::IODataType::IO_DATA_COM] =
      cf_options_[IORequest::IODataType::IO_DATA_META];
  return s;
}

void TerarkdbStorage::clear_whole_cf(uint32_t cf) {
  rocksdb::ColumnFamilyHandle *cfh = cf_handles_[cf];
  rocksdb::RangePtr range;
  DeleteFilesInRanges(db_, cfh, &range, 1);
  std::string begin_key;
  std::string end_key;
  size_t last_key_size = 16;
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(rocksdb::ReadOptions(), cfh));
  if (iter->status().ok()) {
    iter->SeekToLast();
    if (iter->Valid() && iter->key().size() > end_key.size()) {
      last_key_size = iter->key().size();
    }
  }
  if (end_key.empty()) {
    end_key.resize(last_key_size + 1, char(255));
  }
  db_->DeleteRange(rocksdb::WriteOptions(), cfh, begin_key, end_key);
}

Status TerarkdbStorage::start() {
  rocksdb::Status s = init_db_options();
  if (s.ok()) {
    s = init_cf_options();
  }
  if (s.ok()) {
    /* 0:PageCF 1:LogCF 2:MetaCF */
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.resize(IORequest::IODataType::NUM_IO_DATA_TYPES);
    for (auto i = 0; i < IORequest::IODataType::NUM_IO_DATA_TYPES; i++) {
      column_families[i] =
          rocksdb::ColumnFamilyDescriptor(cf_names_[i], cf_options_[i]);
    }

    //    LOG(INFO) << PAGESTORE_STORAGE_MODULE_NAME << "DiskId " <<
    //    disk_id_to_string(disk_id_)
    //              << " start to open terarkdb.";
    s = rocksdb::DB::Open(db_options_, work_dir_, column_families, &cf_handles_,
                          &db_);
  }
  if (!s.ok()) {
    //    LOG(ERROR) << PAGESTORE_STORAGE_MODULE_NAME << "DiskId " <<
    //    disk_id_to_string(disk_id_)
    //               << " open terark db failed, message is " << s.ToString();
    return Status::IOError();
  }
  return Status::OK();
}

DEFINE_bool(db_open_sync_switch, true, "write with sync or not");

Status TerarkdbStorage::write(IORequest::IODataType type, const Slice &key,
                              const Slice &value) {
  if (db_ == nullptr) {
    return Status::IOError();
  }

  rocksdb::WriteOptions woption = rocksdb::WriteOptions();
  woption.sync = FLAGS_db_open_sync_switch;
  // delta log在进程重启的时候会直接丢掉，不需要保证一致性
  if ((type == IORequest::IODataType::IO_DATE_PAGE_LOG) ||
      (type == IORequest::IODataType::IO_DATE_MVPAGE)) {
    woption.disableWAL = true;
    woption.sync = false;
  }
  rocksdb::Status s = db_->Put(woption, cf_handles_[type], key, value);
  if (s.ok()) {
    return Status::OK();
  } else {
    return Status::IOError();
  }
  //  LOGF_ERROR("TerarkdbStorage::write failed, type(%d), error message(%s)",
  //  type, s.ToString().c_str());
  //
  //  if (s.IsCorruption()) {
  //    return Status::DiskFail();
  //  }
  //  return Status::DBIOErr();
}
void terark_shutdown_instance(
    rocksdb::DB *db, const std::shared_ptr<rocksdb::RateLimiter> &rate_limiter,
    const std::shared_ptr<rocksdb::SstFileManager> &sst_file_manager) {
  std::unique_lock<std::mutex> l(terarkdb_mutex);
  terarkdb_rate_limiter.erase(
      std::remove_if(
          terarkdb_rate_limiter.begin(), terarkdb_rate_limiter.end(),
          [&rate_limiter](
              const std::shared_ptr<rocksdb::RateLimiter> &it_rate_limiter) {
            return it_rate_limiter.get() == rate_limiter.get();
          }),
      terarkdb_rate_limiter.end());
  terarkdb_sst_file_manager.erase(
      std::remove_if(
          terarkdb_sst_file_manager.begin(), terarkdb_sst_file_manager.end(),
          [&sst_file_manager](const std::shared_ptr<rocksdb::SstFileManager>
                                  &it_sst_file_manager) {
            return it_sst_file_manager.get() == sst_file_manager.get();
          }),
      terarkdb_sst_file_manager.end());
}
TerarkdbStorage::TerarkdbStorage(const std::string &work_dir)
    : work_dir_(work_dir) {}
TerarkdbStorage::~TerarkdbStorage() {
  terark_shutdown_instance(db_, rate_limiter_, sst_file_manager_);
  rate_limiter_.reset();
  sst_file_manager_.reset();

  // close db
  for (auto i = 0; i < IORequest::IODataType::NUM_IO_DATA_TYPES; i++) {
    delete cf_handles_[i];
  }

  delete db_;
}

//    IO_DATA_PAGE = 0,      // database page
//    IO_DATA_LOG = 1,       // redo log blocks
//    IO_DATA_META = 2,      // segment meta data
//    IO_DATE_PAGE_LOG = 3,  // page log (delta log)
//    IO_DATE_MVPAGE = 4,    // multi-version page
//    IO_DATA_COM = 5,
//    NUM_IO_DATA_TYPES

// cmake .. -DWITH_EXAMPLES=ON -DWITH_JEMALLOC=OFF -DWITH_TERARK_ZIP=ON
DEFINE_uint64(default_cf_value_length, 16 << 10, "default cf value length");
DEFINE_uint64(log_cf_value_length, 16 << 10, "log_cf value length");
DEFINE_string(dbpath,"./temp","db path");
DEFINE_uint64(ratio_default, 5, "ratio of default_cf");
DEFINE_uint64(ratio_log, 3, "ratio of default_cf");
DEFINE_uint64(ratio_delta_log, 2, "ratio of default_cf");
DEFINE_uint64(fill_threads, 1 ,"the num of fill thread");

// todo

void genValue(uint64_t length, std::string *value) {
  value->clear();
  for (int i = 0; i < length; i++) {
    value->push_back('0');
  }
}
void fill(int tid , TerarkdbStorage* ts){
  int i = 0;
  std::string d_value;
  std::string l_value;
  genValue(FLAGS_default_cf_value_length, &d_value);
  genValue(FLAGS_log_cf_value_length, &l_value);
  std::string key = "key_";
  AppendNumberTo(&key,tid);
  int default_i = 0 ,log_i = 0, delta_i = 0;
  while (true) {
    for(int i = 0; i < FLAGS_ratio_default; i++){
      std::string key_i = key;
      AppendNumberTo(&key_i,default_i + i);
      // write d_value
      ts->write(IORequest::IODataType::IO_DATA_PAGE, key_i , d_value);
    }
    default_i += FLAGS_ratio_default;

    for(int i = 0; i < FLAGS_ratio_log; i++){
      std::string key_i = key;
      AppendNumberTo(&key_i, log_i + i);
      // write l_value
      ts->write(IORequest::IODataType::IO_DATA_PAGE, key_i, l_value);
    }
    log_i += FLAGS_ratio_log;

    for(int i = 0; i < FLAGS_ratio_delta_log; i++){
      std::string key_i = key;
      AppendNumberTo(&key_i,delta_i + i);
      // write l_value
      ts->write(IORequest::IODataType::IO_DATA_PAGE, key , l_value);
    }
    delta_i += FLAGS_ratio_delta_log;
  }
}
void show_config(){
  std::cout << "you can set these param to generate workload such as :" << std::endl;
  std::cout << "--default_cf_length={} --log_cf_value_length={} --dbpath={} --ratio_default={} --ratio_log={} --ratio_delta_log={} --fill_threads={}" << std::endl;
  std::cout << "-------------------------------current config------------------------" << std::endl;
  std::cout << "the db_path" << FLAGS_dbpath << std::endl;
  std::cout << "the num of fill thread:" << FLAGS_fill_threads << std::endl;
  std::cout << "ratio of default_cf :log_cf : meta_cf = " << FLAGS_ratio_default << ":" << FLAGS_ratio_log << ":"<< FLAGS_ratio_delta_log << std::endl;
  std::cout << "the length of default_cf:" << FLAGS_default_cf_value_length << std::endl;
  std::cout << "the length of log_cf:"  <<  FLAGS_log_cf_value_length << std::endl;
}
int main(int argc,char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  show_config();
  TerarkdbStorage ts(FLAGS_dbpath);
  ts.start();
  std::thread threads[FLAGS_fill_threads];
  for(int i = 1; i < FLAGS_fill_threads;i++){
    threads[i] = std::thread(fill,i,&ts);
  }
  fill(0,&ts);

  return 0;
}
