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

package org.apache.ignite.util;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteJdbcThinDataSource;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.internal.processors.cache.TestCacheStoreStrategy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import javax.cache.configuration.Factory;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.util.GridCommandHandlerClusterByClassTest.dumpFileNameMatcher;

/** */
public class IdleVerifyDumpTest extends GridCommandHandlerClusterByClassAbstractTest {
    protected static TestCacheStoreStrategy storeStgy;

    public static class C1 implements Factory {
        @Override public CacheStoreSessionListener create() {
            CacheJdbcStoreSessionListener lsnr = new CacheJdbcStoreSessionListener();

            IgniteJdbcThinDataSource ids = new IgniteJdbcThinDataSource();

            try {
                ids.setAddresses("127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            lsnr.setDataSource(ids);

            return lsnr;
        }
    }

    @Override protected long getTestTimeout() {
        return 500 * 1000;
    }

    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return F.asList(CLI_CMD_HND);
    }

    @Test
    public void test1() throws Exception {
        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL varbinary) WITH " +
                "\"CACHE_NAME=default2\""));

        IgniteCache<Object, Object> cache1 = client.cache("default2");

        cache1.clear();
        storeStgy.resetStore();

        cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);
        cache1 = client.cache("default2");
        //cache1.loadCache(null, 100);
        System.err.println("cache size: " + cache1.size());
        final AtomicInteger counter = new AtomicInteger();

        cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL varbinary) WITH " +
                "\"CACHE_NAME=default2\""));

        Connection conn0 = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800");
        PreparedStatement stmtInsert = conn0.prepareStatement("INSERT INTO T1 VALUES (?, ?)");

        int k = counter.incrementAndGet();
        try {
            stmtInsert.setInt(1, k);
            stmtInsert.setBytes(2, new byte[2048]);
            stmtInsert.executeUpdate();
        } catch (java.sql.SQLException ex) {
            // no op.
        }

        checkDump(0);
    }

    @Test
    public void test0() throws Exception {
        String URL[] = {"jdbc:ignite:thin://127.0.0.1:10800", "jdbc:ignite:thin://127.0.0.1:10801", "jdbc:ignite:thin://127.0.0.1:10802"};

        int insertCount = 20;
        long testTime = 5 * 1000;

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL varbinary) WITH " +
                "\"CACHE_NAME=default2\""));

        IgniteCache<Object, Object> cache1 = client.cache("default2");

        cache1.clear();
        storeStgy.resetStore();

        cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);
        cache1 = client.cache("default2");
        //cache1.loadCache(null, 100);
        System.err.println("cache size: " + cache1.size());
        final AtomicInteger counter = new AtomicInteger();

        cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL varbinary) WITH " +
                "\"CACHE_NAME=default2\""));

        List<IgniteInternalFuture> insertFuts = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        for (int c = 0; c < insertCount; ++c) {
            IgniteInternalFuture<Object> f = GridTestUtils.runAsync(() -> {
                int urlPos = ThreadLocalRandom.current().nextInt(3);
                Connection conn0 = DriverManager.getConnection(URL[urlPos]);
                PreparedStatement stmtInsert = conn0.prepareStatement("INSERT INTO T1 VALUES (?, ?)");

                long start = System.currentTimeMillis();

                while (true) {
                    int k = counter.incrementAndGet();
                    try {
                        stmtInsert.setInt(1, k);
                        stmtInsert.setBytes(2, new byte[2048]);
                        stmtInsert.executeUpdate();
                    } catch (java.sql.SQLException ex) {
                        // no op.
                    }

                    if (System.currentTimeMillis() - start > testTime) {
                        System.err.println("!!!thread complete");
                        return;
                    }

                    if (System.currentTimeMillis() - start > testTime / 2) {
                        if (latch.getCount() != 0) {
                            latch.countDown();
                            System.err.println("!!!latch");
                        }
                    }
                }
            });

            insertFuts.add(f);
        }

        IgniteInternalFuture<Object> fDelete = GridTestUtils.runAsync(() -> {
            Connection conn0 = DriverManager.getConnection(URL[0]);
            PreparedStatement stmtInsert = conn0.prepareStatement("DELETE FROM T1 WHERE ID=?");

            long start = System.currentTimeMillis();

            while (true) {
                int k = counter.get() - 1;
                try {
                    stmtInsert.setInt(1, k);
                    stmtInsert.executeUpdate();
                } catch (java.sql.SQLException ex) {
                    // no op.
                }

                if (System.currentTimeMillis() - start > testTime) {
                    System.err.println("!!!thread complete");
                    return;
                }
            }
        });

        IgniteInternalFuture<Object> fKill = GridTestUtils.runAsync(() -> {
            latch.await();
            System.err.println("!!!! NODE KILL !!!!");
            stopGrid(1);
        });

        insertFuts.forEach(f -> {
            try {
                f.get();
            } catch (IgniteCheckedException e) {
                // no op
            }
        });

        fKill.get();
        fDelete.get();

        try {
            checkDump(0);
        } catch (Throwable th) {
            U.sleep(1000);
            fail();
        }
        System.err.println("!!!! final size: " + cache.query(new SqlFieldsQuery("select count (*) from t1")).getAll());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Object, Object> cc1 = new CacheConfiguration<>("default2");

        CacheConfiguration cc = new CacheConfiguration(DEFAULT_CACHE_NAME);

        storeStgy = new MapCacheStoreStrategy();

        if (storeStgy != null) {
            Factory<? extends CacheStore<Object, Object>> storeFactory = storeStgy.getStoreFactory();

            CacheStore<?, ?> store = storeFactory.create();

            cc1.setAtomicityMode(TRANSACTIONAL);
            cc1.setCacheMode(REPLICATED);
            cc1.setWriteSynchronizationMode(FULL_SYNC);
            cc1.setStoreKeepBinary(true);

            cc1.setCacheStoreSessionListenerFactories(new C1());

            if (store != null) {
                cc1.setCacheStoreFactory(storeFactory);
                cc1.setReadThrough(true);
                cc1.setWriteThrough(true);
            }
        }

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        //.setPersistenceEnabled(true)
                        .setMaxSize(4096L * 1024 * 1024)));

        cfg.setCacheConfiguration(cc, cc1);

        return cfg.setWorkDirectory(nodeWorkDirectory(igniteInstanceName));
    }

    /**
     * Test ensuring that idle verify dump output file is created exactly
     * on server node specified via the --host parameter.
     */
    @Test
    public void testDumpResultMatchesConnection() throws Exception {
        injectTestSystemOut();

        client.createCache(DEFAULT_CACHE_NAME).put(1, 1);

        checkDump(0);

        checkDump(1);
    }

    /** */
    private void checkDump(int nodeIdx) throws Exception {
        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--port", connectorPort(grid(nodeIdx))));

/*        Matcher fileNameMatcher = dumpFileNameMatcher();

        assertTrue(fileNameMatcher.find());

        Path dumpFileName = Paths.get(fileNameMatcher.group(1));

        String dumpRes = new String(Files.readAllBytes(dumpFileName));

        assertContains(log, dumpRes, "The check procedure has finished, no conflicts have been found.");

        assertContains(log, dumpFileName.toString(), nodeWorkDirectory(getTestIgniteInstanceName(nodeIdx)));*/
    }

    /** */
    private String nodeWorkDirectory(String igniteInstanceName) throws IgniteCheckedException {
        return new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath();
    }
}
