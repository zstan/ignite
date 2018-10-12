/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.cache.IgniteCacheEntryProcessorSequentialCallTest;
import org.apache.ignite.cache.IgniteWarmupClosureSelfTest;
import org.apache.ignite.cache.store.CacheStoreReadFromBackupTest;
import org.apache.ignite.cache.store.CacheTransactionalStoreReadFromBackupTest;
import org.apache.ignite.cache.store.GridCacheBalancingStoreSelfTest;
import org.apache.ignite.cache.store.GridCacheLoadOnlyStoreAdapterSelfTest;
import org.apache.ignite.cache.store.GridStoreLoadCacheTest;
import org.apache.ignite.cache.store.StoreResourceInjectionSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinarySelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerStoreKeepBinaryWithSqlEscapeSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreBinaryMarshallerWithSqlEscapeSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreMultitreadedSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreMultithreadedSelfTest;
import org.apache.ignite.cache.store.jdbc.GridCacheJdbcBlobStoreSelfTest;
import org.apache.ignite.cache.store.jdbc.JdbcTypesDefaultTransformerTest;
import org.apache.ignite.internal.IgniteInternalCacheRemoveTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesMultipleConnectionsTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesTest;
import org.apache.ignite.internal.managers.communication.GridIoManagerSelfTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceMultipleConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalancePairedConnectionsTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationBalanceTest;
import org.apache.ignite.internal.managers.communication.IgniteCommunicationSslBalanceTest;
import org.apache.ignite.internal.managers.communication.IgniteIoTestMessagesTest;
import org.apache.ignite.internal.managers.communication.IgniteVariousConnectionNumberTest;
import org.apache.ignite.internal.processors.cache.BinaryMetadataRegistrationInsideEntryProcessorTest;
import org.apache.ignite.internal.processors.cache.CacheAffinityCallSelfTest;
import org.apache.ignite.internal.processors.cache.CacheAtomicSingleMessageCountSelfTest;
import org.apache.ignite.internal.processors.cache.CacheDeferredDeleteQueueTest;
import org.apache.ignite.internal.processors.cache.CacheDeferredDeleteSanitySelfTest;
import org.apache.ignite.internal.processors.cache.CacheFutureExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.CacheNamesSelfTest;
import org.apache.ignite.internal.processors.cache.CacheNamesWithSpecialCharactersTest;
import org.apache.ignite.internal.processors.cache.CachePutEventListenerErrorSelfTest;
import org.apache.ignite.internal.processors.cache.CacheTxFastFinishTest;
import org.apache.ignite.internal.processors.cache.DataStorageConfigurationValidationTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityApiSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityRoutingSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAsyncOperationsLimitSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicUsersAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheClearAllSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheClearLocallySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheColocatedTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentGetCacheOnClientTest;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConfigurationConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheEntryMemorySizeSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheFullTextQueryMultithreadedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheKeyCheckNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheKeyCheckSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheLeakTest;
import org.apache.ignite.internal.processors.cache.GridCacheLifecycleAwareSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheLocalTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMissingCommitVersionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMixedPartitionExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMultiUpdateLockSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFlagsTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheMvccSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheNearTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheObjectToStringSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapAtomicMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapMultiThreadedUpdateSelfTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedTxStoreExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReplicatedUsersAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheReturnValueTransferSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSlowTxWarnTest;
import org.apache.ignite.internal.processors.cache.GridCacheStopSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStorePutxSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheStoreValueBytesSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSwapPreloadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerLoadTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTxPartitionedLocalStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTxUsersAffinityMapperSelfTest;
import org.apache.ignite.internal.processors.cache.GridDataStorageConfigurationConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicLocalWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicStopBusySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheAtomicWithStoreInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryEntryProcessorSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicLocalTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerEagerTtlDisabledTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxLocalTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerTxTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryProcessorCallTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheManyAsyncOperationsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheNearLockValueSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheObjectPutSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheSerializationSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheStartStopLoadTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTransactionalStopBusySelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxLocalInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxNearEnabledInvokeTest;
import org.apache.ignite.internal.processors.cache.IgniteCachingProviderSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteClientAffinityAssignmentSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteIncompleteCacheObjectSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteOnePhaseCommitNearSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePutAllLargeBatchSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePutAllUpdateNonPreloadedPartitionSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteStaticCacheStartSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteTxConfigCacheSelfTest;
import org.apache.ignite.internal.processors.cache.InterceptorWithKeepBinaryCacheFullApiTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheAtomicExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheContinuousExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheIsolatedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheP2PDisableExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCachePartitionedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCachePrivateExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheReplicatedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheSharedExecutionContextTest;
import org.apache.ignite.internal.processors.cache.context.IgniteCacheTxExecutionContextTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicNearUpdateTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheTxNearUpdateTopologyChangeTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheEntrySetIterationPreloadingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecovery10ConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecoveryPairedConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicMessageRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheConnectionRecovery10ConnectionsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheConnectionRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheMessageRecoveryIdleConnectionTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheMessageWriteTimeoutTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSystemTransactionsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxMessageRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCrossCacheTxStoreSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheAtomicNearCacheSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheGlobalLoadTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionsStateValidationTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionsStateValidatorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheGetStoreErrorSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalTxExceptionSelfTest;
import org.apache.ignite.internal.processors.cache.query.CacheBalanceTxAndScanQueryRestartTest;
import org.apache.ignite.internal.processors.cache.query.CacheBalanceTxAndScanQueryRestartTestPersistence;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheEntryProcessorExternalizableFailedTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheEntryProcessorNonSerializableTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerClientReconnectAfterClusterRestartTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImplSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerMultiThreadedSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerMultinodeCreateCacheTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerTimeoutTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerUpdateAfterLoadTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;

import java.util.Set;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("IgniteCache Test Suite");

        suite.addTestSuite(CacheBalanceTxAndScanQueryRestartTest.class);
        suite.addTestSuite(CacheBalanceTxAndScanQueryRestartTest.class);
        suite.addTestSuite(CacheBalanceTxAndScanQueryRestartTestPersistence.class);
        suite.addTestSuite(CacheBalanceTxAndScanQueryRestartTestPersistence.class);

        return suite;
    }
}
