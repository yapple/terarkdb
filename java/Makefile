NATIVE_JAVA_CLASSES = org.terarkdb.AbstractCompactionFilter\
	org.terarkdb.AbstractCompactionFilterFactory\
	org.terarkdb.AbstractSlice\
	org.terarkdb.AbstractTransactionNotifier\
	org.terarkdb.BackupEngine\
	org.terarkdb.BackupableDBOptions\
	org.terarkdb.BlockBasedTableConfig\
	org.terarkdb.BloomFilter\
	org.terarkdb.Checkpoint\
	org.terarkdb.ClockCache\
	org.terarkdb.CassandraCompactionFilter\
	org.terarkdb.CassandraValueMergeOperator\
	org.terarkdb.ColumnFamilyHandle\
	org.terarkdb.ColumnFamilyOptions\
	org.terarkdb.CompactionOptionsUniversal\
	org.terarkdb.CompactRangeOptions\
	org.terarkdb.Comparator\
	org.terarkdb.ComparatorOptions\
	org.terarkdb.CompressionOptions\
	org.terarkdb.DBOptions\
	org.terarkdb.DirectComparator\
	org.terarkdb.DirectSlice\
	org.terarkdb.Env\
	org.terarkdb.EnvOptions\
	org.terarkdb.FlinkCompactionFilter\
	org.terarkdb.FlushOptions\
	org.terarkdb.Filter\
	org.terarkdb.IngestExternalFileOptions\
	org.terarkdb.HashLinkedListMemTableConfig\
	org.terarkdb.HashSkipListMemTableConfig\
	org.terarkdb.Logger\
	org.terarkdb.LRUCache\
	org.terarkdb.MemoryUsageType\
	org.terarkdb.MemoryUtil\
	org.terarkdb.MergeOperator\
	org.terarkdb.NativeComparatorWrapper\
	org.terarkdb.OptimisticTransactionDB\
	org.terarkdb.OptimisticTransactionOptions\
	org.terarkdb.Options\
	org.terarkdb.OptionsUtil\
	org.terarkdb.PlainTableConfig\
	org.terarkdb.RateLimiter\
	org.terarkdb.ReadOptions\
	org.terarkdb.RemoveEmptyValueCompactionFilter\
	org.terarkdb.RestoreOptions\
	org.terarkdb.RocksCallbackObject\
	org.terarkdb.RocksDB\
	org.terarkdb.RocksEnv\
	org.terarkdb.RocksIterator\
	org.terarkdb.RocksMemEnv\
	org.terarkdb.SkipListMemTableConfig\
	org.terarkdb.Slice\
	org.terarkdb.SstFileManager\
	org.terarkdb.SstFileWriter\
	org.terarkdb.Statistics\
	org.terarkdb.Transaction\
	org.terarkdb.TransactionDB\
	org.terarkdb.TransactionDBOptions\
	org.terarkdb.TransactionOptions\
	org.terarkdb.TransactionLogIterator\
	org.terarkdb.TtlDB\
	org.terarkdb.VectorMemTableConfig\
	org.terarkdb.Snapshot\
	org.terarkdb.StringAppendOperator\
	org.terarkdb.StringAppendOperatorWithVariableDelimitor\
	org.terarkdb.UInt64AddOperator\
	org.terarkdb.WriteBatch\
	org.terarkdb.WriteBatch.Handler\
	org.terarkdb.WriteOptions\
	org.terarkdb.WriteBatchWithIndex\
	org.terarkdb.WriteBufferManager\
	org.terarkdb.WBWIRocksIterator

NATIVE_JAVA_TEST_CLASSES = org.terarkdb.RocksDBExceptionTest\
    org.terarkdb.NativeComparatorWrapperTest.NativeStringComparatorWrapper\
    org.terarkdb.WriteBatchTest\
    org.terarkdb.WriteBatchTestInternalHelper

ROCKSDB_MAJOR = $(shell egrep "ROCKSDB_MAJOR.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_MINOR = $(shell egrep "ROCKSDB_MINOR.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_PATCH = $(shell egrep "ROCKSDB_PATCH.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)

NATIVE_INCLUDE = ./include
ARCH := $(shell getconf LONG_BIT)
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-linux$(ARCH).jar
ifeq ($(PLATFORM), OS_MACOSX)
ROCKSDB_JAR = rocksdbjni-$(ROCKSDB_MAJOR).$(ROCKSDB_MINOR).$(ROCKSDB_PATCH)-osx.jar
endif

JAVA_TESTS = org.terarkdb.BackupableDBOptionsTest\
	org.terarkdb.BackupEngineTest\
	org.terarkdb.BlockBasedTableConfigTest\
	org.terarkdb.util.BytewiseComparatorTest\
	org.terarkdb.CheckPointTest\
	org.terarkdb.ClockCacheTest\
	org.terarkdb.ColumnFamilyOptionsTest\
	org.terarkdb.ColumnFamilyTest\
    org.terarkdb.CompactionFilterFactoryTest\
	org.terarkdb.CompactionOptionsUniversalTest\
	org.terarkdb.CompactionPriorityTest\
	org.terarkdb.CompactionStopStyleTest\
	org.terarkdb.ComparatorOptionsTest\
	org.terarkdb.ComparatorTest\
	org.terarkdb.CompressionOptionsTest\
	org.terarkdb.CompressionTypesTest\
	org.terarkdb.DBOptionsTest\
	org.terarkdb.DirectComparatorTest\
	org.terarkdb.DirectSliceTest\
	org.terarkdb.EnvOptionsTest\
	org.terarkdb.IngestExternalFileOptionsTest\
	org.terarkdb.util.EnvironmentTest\
	org.terarkdb.FilterTest\
	org.terarkdb.FlushTest\
	org.terarkdb.InfoLogLevelTest\
	org.terarkdb.KeyMayExistTest\
	org.terarkdb.LoggerTest\
	org.terarkdb.LRUCacheTest\
	org.terarkdb.MemoryUtilTest\
	org.terarkdb.MemTableTest\
	org.terarkdb.MergeTest\
	org.terarkdb.MixedOptionsTest\
	org.terarkdb.MutableColumnFamilyOptionsTest\
	org.terarkdb.NativeComparatorWrapperTest\
	org.terarkdb.NativeLibraryLoaderTest\
	org.terarkdb.OptimisticTransactionTest\
	org.terarkdb.OptimisticTransactionDBTest\
	org.terarkdb.OptimisticTransactionOptionsTest\
	org.terarkdb.OptionsUtilTest\
	org.terarkdb.OptionsTest\
	org.terarkdb.PlainTableConfigTest\
	org.terarkdb.RateLimiterTest\
	org.terarkdb.ReadOnlyTest\
	org.terarkdb.ReadOptionsTest\
	org.terarkdb.RocksDBTest\
	org.terarkdb.RocksDBExceptionTest\
	org.terarkdb.RocksEnvTest\
	org.terarkdb.RocksIteratorTest\
	org.terarkdb.RocksMemEnvTest\
	org.terarkdb.util.SizeUnitTest\
	org.terarkdb.SliceTest\
	org.terarkdb.SnapshotTest\
	org.terarkdb.SstFileManagerTest\
	org.terarkdb.SstFileWriterTest\
	org.terarkdb.TransactionTest\
	org.terarkdb.TransactionDBTest\
	org.terarkdb.TransactionOptionsTest\
	org.terarkdb.TransactionDBOptionsTest\
	org.terarkdb.TransactionLogIteratorTest\
	org.terarkdb.TtlDBTest\
	org.terarkdb.StatisticsTest\
	org.terarkdb.StatisticsCollectorTest\
	org.terarkdb.WALRecoveryModeTest\
	org.terarkdb.WriteBatchHandlerTest\
	org.terarkdb.WriteBatchTest\
	org.terarkdb.WriteBatchThreadedTest\
	org.terarkdb.WriteOptionsTest\
	org.terarkdb.WriteBatchWithIndexTest

MAIN_SRC = src/main/java
TEST_SRC = src/test/java
OUTPUT = target
MAIN_CLASSES = $(OUTPUT)/classes
TEST_CLASSES = $(OUTPUT)/test-classes
JAVADOC = $(OUTPUT)/apidocs

BENCHMARK_MAIN_SRC = benchmark/src/main/java
BENCHMARK_OUTPUT = benchmark/target
BENCHMARK_MAIN_CLASSES = $(BENCHMARK_OUTPUT)/classes

SAMPLES_MAIN_SRC = samples/src/main/java
SAMPLES_OUTPUT = samples/target
SAMPLES_MAIN_CLASSES = $(SAMPLES_OUTPUT)/classes

JAVA_TEST_LIBDIR = test-libs
JAVA_JUNIT_JAR = $(JAVA_TEST_LIBDIR)/junit-4.12.jar
JAVA_HAMCR_JAR = $(JAVA_TEST_LIBDIR)/hamcrest-core-1.3.jar
JAVA_MOCKITO_JAR = $(JAVA_TEST_LIBDIR)/mockito-all-1.10.19.jar
JAVA_CGLIB_JAR = $(JAVA_TEST_LIBDIR)/cglib-2.2.2.jar
JAVA_ASSERTJ_JAR = $(JAVA_TEST_LIBDIR)/assertj-core-1.7.1.jar
JAVA_TESTCLASSPATH = $(JAVA_JUNIT_JAR):$(JAVA_HAMCR_JAR):$(JAVA_MOCKITO_JAR):$(JAVA_CGLIB_JAR):$(JAVA_ASSERTJ_JAR)

MVN_LOCAL = ~/.m2/repository

# Set the default JAVA_ARGS to "" for DEBUG_LEVEL=0
JAVA_ARGS? =

JAVAC_ARGS? =

# When debugging add -Xcheck:jni to the java args
ifneq ($(DEBUG_LEVEL),0)
	JAVA_ARGS = -ea -Xcheck:jni
	JAVAC_ARGS = -Xlint:deprecation -Xlint:unchecked
endif

SEARCH_REPO_URL?=http://search.maven.org/remotecontent?filepath=
CENTRAL_REPO_URL?=https://repo1.maven.org/maven2/
clean: clean-not-downloaded clean-downloaded

clean-not-downloaded:
	$(AM_V_at)rm -rf $(NATIVE_INCLUDE)
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)rm -rf $(BENCHMARK_OUTPUT)
	$(AM_V_at)rm -rf $(SAMPLES_OUTPUT)

clean-downloaded:
	$(AM_V_at)rm -rf $(JAVA_TEST_LIBDIR)

javadocs: java
	$(AM_V_GEN)mkdir -p $(JAVADOC)
	$(AM_V_at)javadoc -d $(JAVADOC) -sourcepath $(MAIN_SRC) -subpackages org

javalib: java java_test javadocs

java:
	$(AM_V_GEN)mkdir -p $(MAIN_CLASSES)
	$(AM_V_at)javac $(JAVAC_ARGS) -d $(MAIN_CLASSES)\
		$(MAIN_SRC)/org/terarkdb/util/*.java\
		$(MAIN_SRC)/org/terarkdb/*.java
	$(AM_V_at)@cp ../HISTORY.md ./HISTORY-CPP.md
	$(AM_V_at)@rm -f ./HISTORY-CPP.md
	$(AM_V_at)javah -cp $(MAIN_CLASSES) -d $(NATIVE_INCLUDE) -jni $(NATIVE_JAVA_CLASSES)

sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/RocksDBSample.java
	$(AM_V_at)@rm -rf /tmp/rocksdbjni
	$(AM_V_at)@rm -rf /tmp/rocksdbjni_not_found
	java $(JAVA_ARGS) -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) RocksDBSample /tmp/rocksdbjni
	$(AM_V_at)@rm -rf /tmp/rocksdbjni
	$(AM_V_at)@rm -rf /tmp/rocksdbjni_not_found

column_family_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/RocksDBColumnFamilySample.java
	$(AM_V_at)@rm -rf /tmp/rocksdbjni
	java $(JAVA_ARGS) -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) RocksDBColumnFamilySample /tmp/rocksdbjni
	$(AM_V_at)@rm -rf /tmp/rocksdbjni

transaction_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)javac -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/TransactionSample.java
	$(AM_V_at)@rm -rf /tmp/rocksdbjni
	java -ea -Xcheck:jni -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) TransactionSample /tmp/rocksdbjni
	$(AM_V_at)@rm -rf /tmp/rocksdbjni

optimistic_transaction_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)javac -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/OptimisticTransactionSample.java
	$(AM_V_at)@rm -rf /tmp/rocksdbjni
	java -ea -Xcheck:jni -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) OptimisticTransactionSample /tmp/rocksdbjni
	$(AM_V_at)@rm -rf /tmp/rocksdbjni

resolve_test_deps:
	test -d "$(JAVA_TEST_LIBDIR)" || mkdir -p "$(JAVA_TEST_LIBDIR)"
	test -s "$(JAVA_JUNIT_JAR)" || cp $(MVN_LOCAL)/junit/junit/4.12/junit-4.12.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o $(JAVA_JUNIT_JAR) $(SEARCH_REPO_URL)junit/junit/4.12/junit-4.12.jar
	test -s "$(JAVA_HAMCR_JAR)" || cp $(MVN_LOCAL)/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o $(JAVA_HAMCR_JAR) $(SEARCH_REPO_URL)/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar
	test -s "$(JAVA_MOCKITO_JAR)" || cp $(MVN_LOCAL)/org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o "$(JAVA_MOCKITO_JAR)" $(SEARCH_REPO_URL)/org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar
	test -s "$(JAVA_CGLIB_JAR)" || cp $(MVN_LOCAL)/cglib/cglib/2.2.2/cglib-2.2.2.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o "$(JAVA_CGLIB_JAR)" $(SEARCH_REPO_URL)cglib/cglib/2.2.2/cglib-2.2.2.jar
	test -s "$(JAVA_ASSERTJ_JAR)" || cp $(MVN_LOCAL)/org/assertj/assertj-core/1.7.1/assertj-core-1.7.1.jar $(JAVA_TEST_LIBDIR) || curl -k -L -o "$(JAVA_ASSERTJ_JAR)" $(CENTRAL_REPO_URL)/org/assertj/assertj-core/1.7.1/assertj-core-1.7.1.jar

java_test: java resolve_test_deps
	$(AM_V_GEN)mkdir -p $(TEST_CLASSES)
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES):$(JAVA_TESTCLASSPATH) -d $(TEST_CLASSES)\
		$(TEST_SRC)/org/terarkdb/test/*.java\
		$(TEST_SRC)/org/terarkdb/util/*.java\
		$(TEST_SRC)/org/terarkdb/*.java
	$(AM_V_at)javah -cp $(MAIN_CLASSES):$(TEST_CLASSES) -d $(NATIVE_INCLUDE) -jni $(NATIVE_JAVA_TEST_CLASSES)

test: java java_test run_test

run_test:
	java $(JAVA_ARGS) -Djava.library.path=target -cp "$(MAIN_CLASSES):$(TEST_CLASSES):$(JAVA_TESTCLASSPATH):target/*" org.terarkdb.test.RocksJunitRunner $(JAVA_TESTS)

db_bench: java
	$(AM_V_GEN)mkdir -p $(BENCHMARK_MAIN_CLASSES)
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(BENCHMARK_MAIN_CLASSES) $(BENCHMARK_MAIN_SRC)/org/terarkdb/benchmark/*.java
