cmake_minimum_required(VERSION 3.4)

set(JNI_NATIVE_SOURCES
        rocksjni/backupablejni.cc
        rocksjni/backupenginejni.cc
        rocksjni/cassandra_compactionfilterjni.cc
        rocksjni/cassandra_value_operator.cc
        rocksjni/checkpoint.cc
        rocksjni/clock_cache.cc
        rocksjni/columnfamilyhandle.cc
        rocksjni/compaction_filter.cc
        rocksjni/compaction_filter_factory.cc
        rocksjni/compaction_filter_factory_jnicallback.cc
        rocksjni/compaction_options_universal.cc
        rocksjni/compact_range_options.cc
        rocksjni/comparator.cc
        rocksjni/comparatorjnicallback.cc
        rocksjni/compression_options.cc
        rocksjni/env.cc
        rocksjni/env_options.cc
        rocksjni/flink_compactionfilterjni.cc
        rocksjni/filter.cc
        rocksjni/ingest_external_file_options.cc
        rocksjni/iterator.cc
        rocksjni/jnicallback.cc
        rocksjni/loggerjnicallback.cc
        rocksjni/lru_cache.cc
        rocksjni/memory_util.cc
        rocksjni/memtablejni.cc
        rocksjni/merge_operator.cc
        rocksjni/native_comparator_wrapper_test.cc
        rocksjni/optimistic_transaction_db.cc
        rocksjni/optimistic_transaction_options.cc
        rocksjni/options.cc
        rocksjni/options_util.cc
        rocksjni/ratelimiterjni.cc
        rocksjni/remove_emptyvalue_compactionfilterjni.cc
        rocksjni/restorejni.cc
        rocksjni/rocks_callback_object.cc
        rocksjni/rocksdb_exception_test.cc
        rocksjni/rocksjni.cc
        rocksjni/slice.cc
        rocksjni/snapshot.cc
        rocksjni/sst_file_manager.cc
        rocksjni/sst_file_writerjni.cc
        rocksjni/statistics.cc
        rocksjni/statisticsjni.cc
        rocksjni/table.cc
        rocksjni/transaction.cc
        rocksjni/transaction_db.cc
        rocksjni/transaction_db_options.cc
        rocksjni/transaction_log.cc
        rocksjni/transaction_notifier.cc
        rocksjni/transaction_notifier_jnicallback.cc
        rocksjni/transaction_options.cc
        rocksjni/ttl.cc
        rocksjni/write_batch.cc
        rocksjni/writebatchhandlerjnicallback.cc
        rocksjni/write_batch_test.cc
        rocksjni/write_batch_with_index.cc
        rocksjni/write_buffer_manager.cc
)

set(NATIVE_JAVA_CLASSES
        org.terarkdb.AbstractCompactionFilter
        org.terarkdb.AbstractCompactionFilterFactory
        org.terarkdb.AbstractComparator
        org.terarkdb.AbstractImmutableNativeReference
        org.terarkdb.AbstractNativeReference
        org.terarkdb.AbstractRocksIterator
        org.terarkdb.AbstractSlice
        org.terarkdb.AbstractTransactionNotifier
        org.terarkdb.BackupableDBOptions
        org.terarkdb.BackupEngine
        org.terarkdb.BlockBasedTableConfig
        org.terarkdb.BloomFilter
        org.terarkdb.CassandraCompactionFilter
        org.terarkdb.CassandraValueMergeOperator
        org.terarkdb.Checkpoint
        org.terarkdb.ClockCache
        org.terarkdb.ColumnFamilyHandle
        org.terarkdb.ColumnFamilyOptions
        org.terarkdb.CompactionOptionsUniversal
        org.terarkdb.CompactRangeOptions
        org.terarkdb.Comparator
        org.terarkdb.ComparatorOptions
        org.terarkdb.CompressionOptions
        org.terarkdb.DBOptions
        org.terarkdb.DirectComparator
        org.terarkdb.DirectSlice
        org.terarkdb.Env
        org.terarkdb.EnvOptions
        org.terarkdb.Filter
        org.terarkdb.FlinkCompactionFilter
        org.terarkdb.FlushOptions
        org.terarkdb.HashLinkedListMemTableConfig
        org.terarkdb.HashSkipListMemTableConfig
        org.terarkdb.IngestExternalFileOptions
        org.terarkdb.Logger
        org.terarkdb.LRUCache
        org.terarkdb.MemoryUtil
        org.terarkdb.MemTableConfig
        org.terarkdb.NativeComparatorWrapper
        org.terarkdb.NativeLibraryLoader
        org.terarkdb.OptimisticTransactionDB
        org.terarkdb.OptimisticTransactionOptions
        org.terarkdb.Options
        org.terarkdb.OptionsUtil
        org.terarkdb.PlainTableConfig
        org.terarkdb.RateLimiter
        org.terarkdb.ReadOptions
        org.terarkdb.RemoveEmptyValueCompactionFilter
        org.terarkdb.RestoreOptions
        org.terarkdb.RocksCallbackObject
        org.terarkdb.RocksDB
        org.terarkdb.RocksEnv
        org.terarkdb.RocksIterator
        org.terarkdb.RocksIteratorInterface
        org.terarkdb.RocksMemEnv
        org.terarkdb.RocksMutableObject
        org.terarkdb.RocksObject
        org.terarkdb.SkipListMemTableConfig
        org.terarkdb.Slice
        org.terarkdb.Snapshot
        org.terarkdb.SstFileManager
        org.terarkdb.SstFileWriter
        org.terarkdb.Statistics
        org.terarkdb.StringAppendOperator
        org.terarkdb.StringAppendOperatorWithVariableDelimitor
        org.terarkdb.TableFormatConfig
        org.terarkdb.Transaction
        org.terarkdb.TransactionDB
        org.terarkdb.TransactionDBOptions
        org.terarkdb.TransactionLogIterator
        org.terarkdb.TransactionOptions
        org.terarkdb.TtlDB
        org.terarkdb.UInt64AddOperator
        org.terarkdb.VectorMemTableConfig
        org.terarkdb.WBWIRocksIterator
        org.terarkdb.WriteBatch
        org.terarkdb.WriteBatch.Handler
        org.terarkdb.WriteBatchInterface
        org.terarkdb.WriteBatchWithIndex
        org.terarkdb.WriteOptions
        org.terarkdb.NativeComparatorWrapperTest
        org.terarkdb.RocksDBExceptionTest
        org.terarkdb.SnapshotTest
        org.terarkdb.WriteBatchTest
        org.terarkdb.WriteBatchTestInternalHelper
        org.terarkdb.WriteBufferManager
)

include(FindJava)
include(UseJava)
include(FindJNI)

include_directories(${JNI_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/java)

set(JAVA_TEST_LIBDIR ${PROJECT_SOURCE_DIR}/java/test-libs)
set(JAVA_TMP_JAR ${JAVA_TEST_LIBDIR}/tmp.jar)
set(JAVA_JUNIT_JAR ${JAVA_TEST_LIBDIR}/junit-4.12.jar)
set(JAVA_HAMCR_JAR ${JAVA_TEST_LIBDIR}/hamcrest-core-1.3.jar)
set(JAVA_MOCKITO_JAR ${JAVA_TEST_LIBDIR}/mockito-all-1.10.19.jar)
set(JAVA_CGLIB_JAR ${JAVA_TEST_LIBDIR}/cglib-2.2.2.jar)
set(JAVA_ASSERTJ_JAR ${JAVA_TEST_LIBDIR}/assertj-core-1.7.1.jar)
set(JAVA_TESTCLASSPATH ${JAVA_JUNIT_JAR} ${JAVA_HAMCR_JAR} ${JAVA_MOCKITO_JAR} ${JAVA_CGLIB_JAR} ${JAVA_ASSERTJ_JAR})

add_jar(
  rocksdbjni_classes
  SOURCES
  src/main/java/org/terarkdb/AbstractCompactionFilterFactory.java
  src/main/java/org/terarkdb/AbstractCompactionFilter.java
  src/main/java/org/terarkdb/AbstractComparator.java
  src/main/java/org/terarkdb/AbstractImmutableNativeReference.java
  src/main/java/org/terarkdb/AbstractNativeReference.java
  src/main/java/org/terarkdb/AbstractRocksIterator.java
  src/main/java/org/terarkdb/AbstractSlice.java
  src/main/java/org/terarkdb/AbstractTransactionNotifier.java
  src/main/java/org/terarkdb/AbstractWriteBatch.java
  src/main/java/org/terarkdb/AccessHint.java
  src/main/java/org/terarkdb/AdvancedColumnFamilyOptionsInterface.java
  src/main/java/org/terarkdb/AdvancedMutableColumnFamilyOptionsInterface.java
  src/main/java/org/terarkdb/BackupableDBOptions.java
  src/main/java/org/terarkdb/BackupEngine.java
  src/main/java/org/terarkdb/BackupInfo.java
  src/main/java/org/terarkdb/BlockBasedTableConfig.java
  src/main/java/org/terarkdb/BloomFilter.java
  src/main/java/org/terarkdb/BuiltinComparator.java
  src/main/java/org/terarkdb/Cache.java
  src/main/java/org/terarkdb/CassandraCompactionFilter.java
  src/main/java/org/terarkdb/CassandraValueMergeOperator.java
  src/main/java/org/terarkdb/Checkpoint.java
  src/main/java/org/terarkdb/ChecksumType.java
  src/main/java/org/terarkdb/ClockCache.java
  src/main/java/org/terarkdb/ColumnFamilyDescriptor.java
  src/main/java/org/terarkdb/ColumnFamilyHandle.java
  src/main/java/org/terarkdb/ColumnFamilyOptionsInterface.java
  src/main/java/org/terarkdb/ColumnFamilyOptions.java
  src/main/java/org/terarkdb/CompactionOptionsUniversal.java
  src/main/java/org/terarkdb/CompactionPriority.java
  src/main/java/org/terarkdb/CompactRangeOptions.java
  src/main/java/org/terarkdb/CompactionStopStyle.java
  src/main/java/org/terarkdb/CompactionStyle.java
  src/main/java/org/terarkdb/Comparator.java
  src/main/java/org/terarkdb/ComparatorOptions.java
  src/main/java/org/terarkdb/ComparatorType.java
  src/main/java/org/terarkdb/CompressionOptions.java
  src/main/java/org/terarkdb/CompressionType.java
  src/main/java/org/terarkdb/DBOptionsInterface.java
  src/main/java/org/terarkdb/DBOptions.java
  src/main/java/org/terarkdb/DbPath.java
  src/main/java/org/terarkdb/DirectComparator.java
  src/main/java/org/terarkdb/DirectSlice.java
  src/main/java/org/terarkdb/EncodingType.java
  src/main/java/org/terarkdb/Env.java
  src/main/java/org/terarkdb/EnvOptions.java
  src/main/java/org/terarkdb/Experimental.java
  src/main/java/org/terarkdb/Filter.java
  src/main/java/org/terarkdb/FlinkCompactionFilter.java
  src/main/java/org/terarkdb/FlushOptions.java
  src/main/java/org/terarkdb/HashLinkedListMemTableConfig.java
  src/main/java/org/terarkdb/HashSkipListMemTableConfig.java
  src/main/java/org/terarkdb/HistogramData.java
  src/main/java/org/terarkdb/HistogramType.java
  src/main/java/org/terarkdb/IndexType.java
  src/main/java/org/terarkdb/InfoLogLevel.java
  src/main/java/org/terarkdb/IngestExternalFileOptions.java
  src/main/java/org/terarkdb/Logger.java
  src/main/java/org/terarkdb/LRUCache.java
  src/main/java/org/terarkdb/MemoryUsageType.java
  src/main/java/org/terarkdb/MemoryUtil.java
  src/main/java/org/terarkdb/MemTableConfig.java
  src/main/java/org/terarkdb/MergeOperator.java
  src/main/java/org/terarkdb/MutableColumnFamilyOptionsInterface.java
  src/main/java/org/terarkdb/MutableColumnFamilyOptions.java
  src/main/java/org/terarkdb/NativeComparatorWrapper.java
  src/main/java/org/terarkdb/NativeLibraryLoader.java
  src/main/java/org/terarkdb/OptimisticTransactionDB.java
  src/main/java/org/terarkdb/OptimisticTransactionOptions.java
  src/main/java/org/terarkdb/Options.java
  src/main/java/org/terarkdb/OptionsUtil.java
  src/main/java/org/terarkdb/PlainTableConfig.java
  src/main/java/org/terarkdb/RateLimiter.java
  src/main/java/org/terarkdb/RateLimiterMode.java
  src/main/java/org/terarkdb/ReadOptions.java
  src/main/java/org/terarkdb/ReadTier.java
  src/main/java/org/terarkdb/RemoveEmptyValueCompactionFilter.java
  src/main/java/org/terarkdb/RestoreOptions.java
  src/main/java/org/terarkdb/RocksCallbackObject.java
  src/main/java/org/terarkdb/RocksDBException.java
  src/main/java/org/terarkdb/RocksDB.java
  src/main/java/org/terarkdb/RocksEnv.java
  src/main/java/org/terarkdb/RocksIteratorInterface.java
  src/main/java/org/terarkdb/RocksIterator.java
  src/main/java/org/terarkdb/RocksMemEnv.java
  src/main/java/org/terarkdb/RocksMutableObject.java
  src/main/java/org/terarkdb/RocksObject.java
  src/main/java/org/terarkdb/SkipListMemTableConfig.java
  src/main/java/org/terarkdb/Slice.java
  src/main/java/org/terarkdb/Snapshot.java
  src/main/java/org/terarkdb/SstFileManager.java
  src/main/java/org/terarkdb/SstFileWriter.java
  src/main/java/org/terarkdb/StatisticsCollectorCallback.java
  src/main/java/org/terarkdb/StatisticsCollector.java
  src/main/java/org/terarkdb/Statistics.java
  src/main/java/org/terarkdb/StatsCollectorInput.java
  src/main/java/org/terarkdb/StatsLevel.java
  src/main/java/org/terarkdb/Status.java
  src/main/java/org/terarkdb/StringAppendOperator.java
  src/main/java/org/terarkdb/StringAppendOperatorWithVariableDelimitor.java
  src/main/java/org/terarkdb/TableFormatConfig.java
  src/main/java/org/terarkdb/TickerType.java
  src/main/java/org/terarkdb/TransactionalDB.java
  src/main/java/org/terarkdb/TransactionalOptions.java
  src/main/java/org/terarkdb/TransactionDB.java
  src/main/java/org/terarkdb/TransactionDBOptions.java
  src/main/java/org/terarkdb/Transaction.java
  src/main/java/org/terarkdb/TransactionLogIterator.java
  src/main/java/org/terarkdb/TransactionOptions.java
  src/main/java/org/terarkdb/TtlDB.java
  src/main/java/org/terarkdb/TxnDBWritePolicy.java
  src/main/java/org/terarkdb/VectorMemTableConfig.java
  src/main/java/org/terarkdb/WALRecoveryMode.java
  src/main/java/org/terarkdb/WBWIRocksIterator.java
  src/main/java/org/terarkdb/WriteBatchInterface.java
  src/main/java/org/terarkdb/WriteBatch.java
  src/main/java/org/terarkdb/WriteBatchWithIndex.java
  src/main/java/org/terarkdb/WriteOptions.java
  src/main/java/org/terarkdb/WriteBufferManager.java
  src/main/java/org/terarkdb/util/BytewiseComparator.java
  src/main/java/org/terarkdb/util/DirectBytewiseComparator.java
  src/main/java/org/terarkdb/util/Environment.java
  src/main/java/org/terarkdb/util/ReverseBytewiseComparator.java
  src/main/java/org/terarkdb/util/SizeUnit.java
  src/test/java/org/terarkdb/BackupEngineTest.java
  src/test/java/org/terarkdb/IngestExternalFileOptionsTest.java
  src/test/java/org/terarkdb/NativeComparatorWrapperTest.java
  src/test/java/org/terarkdb/PlatformRandomHelper.java
  src/test/java/org/terarkdb/RocksDBExceptionTest.java
  src/test/java/org/terarkdb/RocksMemoryResource.java
  src/test/java/org/terarkdb/SnapshotTest.java
  src/main/java/org/terarkdb/UInt64AddOperator.java
  src/test/java/org/terarkdb/WriteBatchTest.java
  src/test/java/org/terarkdb/util/CapturingWriteBatchHandler.java
  src/test/java/org/terarkdb/util/WriteBatchGetter.java
  INCLUDE_JARS ${JAVA_TESTCLASSPATH}
)

if(NOT EXISTS ${PROJECT_SOURCE_DIR}/java/classes)
  file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/java/classes)
endif()

if(NOT EXISTS ${JAVA_TEST_LIBDIR})
  file(MAKE_DIRECTORY mkdir ${JAVA_TEST_LIBDIR})
endif()

if (DEFINED CUSTOM_REPO_URL)
  set(SEARCH_REPO_URL ${CUSTOM_REPO_URL}/)
  set(CENTRAL_REPO_URL ${CUSTOM_REPO_URL}/)
else ()
  set(SEARCH_REPO_URL "http://search.maven.org/remotecontent?filepath=")
  set(CENTRAL_REPO_URL "https://repo1.maven.org/maven2/")
endif()

if(NOT EXISTS ${JAVA_JUNIT_JAR})
  message("Downloading ${JAVA_JUNIT_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}junit/junit/4.12/junit-4.12.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_JUNIT_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_JUNIT_JAR})
endif()
if(NOT EXISTS ${JAVA_HAMCR_JAR})
  message("Downloading ${JAVA_HAMCR_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_HAMCR_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_HAMCR_JAR})
endif()
if(NOT EXISTS ${JAVA_MOCKITO_JAR})
  message("Downloading ${JAVA_MOCKITO_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_MOCKITO_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_MOCKITO_JAR})
endif()
if(NOT EXISTS ${JAVA_CGLIB_JAR})
  message("Downloading ${JAVA_CGLIB_JAR}")
  file(DOWNLOAD ${SEARCH_REPO_URL}cglib/cglib/2.2.2/cglib-2.2.2.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_CGLIB_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_CGLIB_JAR})
endif()
if(NOT EXISTS ${JAVA_ASSERTJ_JAR})
  message("Downloading ${JAVA_ASSERTJ_JAR}")
  file(DOWNLOAD ${CENTRAL_REPO_URL}org/assertj/assertj-core/1.7.1/assertj-core-1.7.1.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_ASSERTJ_JAR}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_ASSERTJ_JAR})
endif()

set(JNI_OUTPUT_DIR ${PROJECT_SOURCE_DIR}/java/include)

file(MAKE_DIRECTORY ${JNI_OUTPUT_DIR})
create_javah(
  TARGET rocksdbjni_headers
  CLASSES ${NATIVE_JAVA_CLASSES}
  CLASSPATH rocksdbjni_classes ${JAVA_TESTCLASSPATH}
  OUTPUT_DIR ${JNI_OUTPUT_DIR}
)

if(NOT MSVC)
  set_property(TARGET ${ROCKSDB_STATIC_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

set(ROCKSDBJNI_STATIC_LIB rocksdbjni${ARTIFACT_SUFFIX})
add_library(${ROCKSDBJNI_STATIC_LIB} ${JNI_NATIVE_SOURCES})
add_dependencies(${ROCKSDBJNI_STATIC_LIB} rocksdbjni_headers)
target_link_libraries(${ROCKSDBJNI_STATIC_LIB} ${ROCKSDB_STATIC_LIB} ${LIBS})

if(NOT MINGW)
  set(ROCKSDBJNI_SHARED_LIB rocksdbjni-shared${ARTIFACT_SUFFIX})
  add_library(${ROCKSDBJNI_SHARED_LIB} SHARED ${JNI_NATIVE_SOURCES})
  add_dependencies(${ROCKSDBJNI_SHARED_LIB} rocksdbjni_headers)
  target_link_libraries(${ROCKSDBJNI_SHARED_LIB} ${ROCKSDB_STATIC_LIB} ${LIBS})

  set_target_properties(
    ${ROCKSDBJNI_SHARED_LIB}
    PROPERTIES
    COMPILE_PDB_OUTPUT_DIRECTORY ${CMAKE_CFG_INTDIR}
    COMPILE_PDB_NAME ${ROCKSDBJNI_STATIC_LIB}.pdb
  )
endif()
