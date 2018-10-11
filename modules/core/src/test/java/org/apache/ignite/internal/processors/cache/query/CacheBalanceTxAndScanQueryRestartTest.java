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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheBalanceTxAndScanQueryRestartTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHES = 1;

    /** */
    private static final int START_VAL = 100;

    /** */
    private static final int SRV_NODES = 3;

    /** */
    private static final int KEYS = 100;

    /** */
    private static final int TX_THREADS = 1;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (client)
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        cfg.setClientMode(client);
        cfg.setFailoverSpi(new AlwaysFailoverSpi());

        CacheConfiguration[] caches = new CacheConfiguration[CACHES];

        for (int i = 0; i < CACHES; i++) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setName(cacheName(i));

            ccfg.setCacheMode(PARTITIONED);
            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(3);
            ccfg.setRebalanceMode(SYNC);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            caches[i] = ccfg;
        }

        cfg.setCacheConfiguration(caches);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60_000;
    }

    /**
     * @param idx Cache index.
     * @return Cache name.
     */
    static String cacheName(int idx) {
        return "cache-" + idx;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartAndQuery() throws Exception {
        client = true;

        Ignite clientNode = startGrid(0);

        client = false;

        for (int i = 0; i < SRV_NODES; i++)
            startGrid(i + 1);

        for (int i = 0; i < CACHES; i++) {
            String cacheName = cacheName(i);

            try (IgniteDataStreamer<Long, TestValue> s = clientNode.dataStreamer(cacheName)) {
                for (long k = 0; k < KEYS; k++)
                    s.addData(k, new TestValue(START_VAL));
            }
        }

        checkTotal(null, clientNode);

        AtomicBoolean stop = new AtomicBoolean();
        ReadWriteLock lock = new ReentrantReadWriteLock();

        List<IgniteInternalFuture> txFuts = new ArrayList<>();

        try {
            for (int i = 0 ; i < TX_THREADS; i++)
                txFuts.add(startTxThread(clientNode, stop, lock));

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            long topVer = SRV_NODES + 1;

            for (int i = 0; i < 10; i++) {
                info("Check: " + i);

                for (int c = 0; c < 20; c++) {
                    checkTotal(lock, clientNode);

                    U.sleep(100);
                }

                int nodeIdx = rnd.nextInt(SRV_NODES) + 1;

                info("Restart node: " + nodeIdx);

                U.sleep(1000);

                IgniteProcessProxy.kill(getTestIgniteInstanceName(nodeIdx));

                topVer++;

                for (int c = 0; c < 20; c++) {
                    checkTotal(lock, clientNode);

                    U.sleep(100);
                }

                U.sleep(1000);

                startGrid(nodeIdx);

                topVer++;

                for (int c = 0; c < 20; c++) {
                    checkTotal(lock, clientNode);

                    U.sleep(100);
                }

                AffinityTopologyVersion waitVer = new AffinityTopologyVersion(topVer, 1);

                info("Wait for topology: " + waitVer);

                IgniteInternalFuture fut =
                    ((IgniteKernal)clientNode).context().cache().context().exchange().affinityReadyFuture(waitVer);

                if (fut != null) {
                    while (!fut.isDone()) {
                        checkTotal(lock, clientNode);

                        U.sleep(50);
                    }

                    fut.get();
                }
            }
        }
        finally {
            stop.set(true);
        }

        for (IgniteInternalFuture fut : txFuts)
            fut.get();
    }

    /**
     * @param node Node.
     * @param stop Stop flag.
     * @param lock Update lock.
     * @return Thread stop future.
     */
    private IgniteInternalFuture startTxThread(final Ignite node, final AtomicBoolean stop, final ReadWriteLock lock) {
        return GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    lock.readLock().lock();

                    try {
                        IgniteCache<Long, TestValue> cache1 = node.cache(cacheName(rnd.nextInt(CACHES)));
                        IgniteCache<Long, TestValue> cache2 = node.cache(cacheName(rnd.nextInt(CACHES)));

                        long key1 = rnd.nextLong(KEYS);
                        long key2;
                        do {
                            key2 = rnd.nextLong(KEYS);
                        }
                        while (key1 == key2);

                        if (key1 > key2) {
                            long tmp = key1;
                            key1 = key2;
                            key2 = tmp;
                        }

                        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            TestValue val1 = cache1.get(key1);
                            TestValue val2 = cache1.get(key2);

                            long delta = 1;

                            val1 = new TestValue(val1.val + delta);
                            val2 = new TestValue(val2.val - delta);

                            cache1.put(key1, val1);
                            cache2.put(key2, val2);

                            tx.commit();
                        }
                    }
                    catch (Exception e) {
                        info("Cache update error: " + e);
                    }
                    finally {
                        lock.readLock().unlock();
                    }
                }
            }
        });
    }

    /**
     * @param clientNode Client node.
     */
    private void checkTotal(@Nullable  ReadWriteLock lock, Ignite clientNode) {
        if (lock != null)
            lock.writeLock().lock();

        try {
            for (int retry = 0; retry < 10; retry++) {
                List<IgniteFuture<Long>> futs = new ArrayList<>();

                for (int i = 0; i < CACHES; i++) {
                    String cacheName = cacheName(i);

                    int parts = clientNode.affinity(cacheName).partitions();

                    for (int p = 0; p < parts; p++) {
                        assertEquals(p, clientNode.affinity(cacheName).partition(p));

                        futs.add(clientNode.compute().affinityCallAsync(cacheName, p, new CacheSumTask(cacheName, p)));
                    }
                }

                long total = 0;

                boolean err = false;

                for (IgniteFuture<Long> fut : futs) {
                    try {
                        total += fut.get();
                    }
                    catch (Exception e) {
                        if (!err) {
                            err = true;

                            info("AffinityCall error: " +  e);
                        }
                    }
                }

                if (!err) {
                    assertEquals(CACHES * KEYS * START_VAL, total);

                    break;
                }
            }
        }
        finally {
            if (lock != null)
                lock.writeLock().unlock();
        }
    }

    /**
     *
     */
    static class CacheSumTask implements IgniteCallable<Long> {
        /** */
        private final String cacheName;

        /** */
        private final int part;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param cacheName Cache name.
         * @param part Partition.
         */
        public CacheSumTask(String cacheName, int part) {
            this.cacheName = cacheName;
            this.part = part;
        }

        /** {@inheritDoc} */
        @Override public Long call() throws Exception {
            IgniteCache cache = ignite.cache(cacheName);

            final Iterator<Cache.Entry> entries = cache.query(new ScanQuery<>(part)).iterator();

            long sum = 0;

            while (entries.hasNext())
                sum += ((TestValue)entries.next().getValue()).val;

            return sum;
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        public long val;

        /**
         * @param val Value.
         */
        public TestValue(long val) {
            this.val = val;
        }
    }
}
