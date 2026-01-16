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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runners.Parameterized;

import javax.cache.configuration.Factory;
import javax.cache.expiry.EternalExpiryPolicy;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.util.GridCommandHandlerClusterByClassTest.dumpFileNameMatcher;

/** */
public class IdleVerifyDumpTest extends GridCommandHandlerClusterByClassAbstractTest {
    @Override protected boolean persistenceEnable() {
        return false;
    }

    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return F.asList(CLI_CMD_HND);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {

        CacheConfiguration cc = cacheConfiguration(igniteInstanceName);
        ///
        //CacheConfiguration cc1 = cacheConfiguration("default2");
        CacheConfiguration<Object, Object> cc1 = new CacheConfiguration<>("default2");
        cc1.setName("default2");

        if (storeStgy != null) {
            Factory<? extends CacheStore<Object, Object>> storeFactory = storeStgy.getStoreFactory();

            CacheStore<?, ?> store = storeFactory.create();

            cc1.setAtomicityMode(TRANSACTIONAL);
            cc1.setCacheMode(REPLICATED);
            //cc1.setWriteSynchronizationMode(FULL_SYNC);
            cc1.setStoreKeepBinary(true);

            if (store != null) {
                cc1.setCacheStoreFactory(storeFactory);
                cc1.setReadThrough(true);
                cc1.setWriteThrough(true);
                cc1.setReadFromBackup(true);
            }
        }
        ///

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setMaxSize(4096L * 1024 * 1024)));

        cfg.setCacheConfiguration(cc, cc1);

        return cfg.setWorkDirectory(nodeWorkDirectory(igniteInstanceName));
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return REPLICATED;
    }

    @Override protected long getTestTimeout() {
        return 500 * 1000;
    }

    @Test
    public void test0() throws Exception {
        //client = startGrid(CLIENT_NODE_NAME_PREFIX);

        String URL[] = {"jdbc:ignite:thin://127.0.0.1:10800", "jdbc:ignite:thin://127.0.0.1:10801", "jdbc:ignite:thin://127.0.0.1:10802"};

        int insertCount = 20;
        long testTime = 6 * 1000;

        //for (int i = 0; i < 20; ++ i) {

            IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

/*            cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL varbinary) WITH " +
                    "\"atomicity=TRANSACTIONAL,CACHE_NAME=default2\""));*/
        cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL varbinary) WITH " +
                "\"CACHE_NAME=default2\""));

            //cache.query(new SqlFieldsQuery("delete from t1"));

            IgniteCache<Object, Object> cache1 = client.cache("default2");

            cache1.clear();

/*            try {
                PreparedStatement stmt = conn.prepareStatement("INSERT INTO T1 VALUES (?, ?)");
                for (int k = 500; k < 2000; k++) {
                    stmt.setInt(1, k);
                    stmt.setBytes(2, new byte[2048]);
                    stmt.executeUpdate();
                }
            } catch (Throwable th) {
                // no op.
            }*/

            stopAllGrids();
            startGrids(3);
            client = startClientGrid(CLIENT_NODE_NAME_PREFIX);

            cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);
            cache1 = client.cache("default2");
            //cache1.loadCache(null, 100);
            System.err.println("cache size: " + cache1.size());
            final AtomicInteger counter = new AtomicInteger();

/*            cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL varbinary) WITH " +
                    "\"atomicity=TRANSACTIONAL,CACHE_NAME=default2\""));*/
        cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL varbinary) WITH " +
                "\"CACHE_NAME=default2\""));

            List<IgniteInternalFuture> insertFuts = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);

            for (int c = 0; c < insertCount; ++c) {
                IgniteInternalFuture<Object> f = GridTestUtils.runAsync(() -> {
                    int urlPos = ThreadLocalRandom.current().nextInt(2);
                    Connection conn0 = DriverManager.getConnection(URL[urlPos]);
                    PreparedStatement stmtInsert = conn0.prepareStatement("INSERT INTO T1 VALUES (?, ?)");

                    long start = System.currentTimeMillis();

                    while (true) {
                        int k = counter.incrementAndGet();//ThreadLocalRandom.current().nextInt(2000, 60000);
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

            IgniteInternalFuture<Object> fKill = GridTestUtils.runAsync(() -> {
                latch.await();
                System.err.println("!!!! NODE KILL !!!!");
                stopGrid(1);
            });

            insertFuts.forEach(f -> {
                try {
                    f.get();
                } catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            });
            fKill.get();

            checkDump(0);
            System.err.println("!!!! final size: " + cache.query(new SqlFieldsQuery("select count (*) from t1")).getAll());
        //}
    }

    @Test
    public void test1() throws Exception {
        //client = startGrid(CLIENT_NODE_NAME_PREFIX);

        for (int i = 0; i < 20; ++ i) {

            IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

            cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL INT) WITH " +
                    "\"template=replicated,atomicity=TRANSACTIONAL,CACHE_NAME=default2\""));

            cache.query(new SqlFieldsQuery("delete from t1"));

            IgniteCache<Object, Object> cache1 = client.cache("default2");

            //cache1.clear();

            try {
                for (int k = 10000; k < 20000; k++) {
                    cache1.query(new SqlFieldsQuery("INSERT INTO T1 VALUES (" + k + ", " + k + ")")).getAll();
                }
            } catch (Throwable th) {
                System.err.println("!!!!: " + th);
            }

            stopAllGrids();
            startGrids(3);
            client = startClientGrid(CLIENT_NODE_NAME_PREFIX);
            IgniteEx client1 = startClientGrid(CLIENT_NODE_NAME_PREFIX + "1");

            cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);
            cache1 = client.cache("default2");
            cache1.loadCache(null, 1000);
            System.err.println("cache size: " + cache1.size());

            checkDump(2);

            cache.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, VAL INT) WITH " +
                    "\"template=replicated,atomicity=TRANSACTIONAL,CACHE_NAME=default2\""));

            //checkDump(2);

            CountDownLatch latch = new CountDownLatch(1);

            IgniteCache<Object, Object> finalCache = cache;
            IgniteInternalFuture<Object> f1 = GridTestUtils.runAsync(() -> {
                for (int k = 1000; k < 5000; k++) {
                    finalCache.query(new SqlFieldsQuery("INSERT INTO T1 VALUES (" + k + ", " + k + ")")).getAll();

                    if (k == 1500) {
                        latch.countDown();
                    }
                }
            });

            IgniteCache<Object, Object> finalCache1 = cache;
            IgniteInternalFuture<Object> f3 = GridTestUtils.runAsync(() -> {
                for (int k = 5000; k < 8000; k++) {
                    finalCache1.query(new SqlFieldsQuery("INSERT INTO T1 VALUES (" + k + ", " + k + ")")).getAll();
                }
            });

            IgniteCache<Object, Object> finalCache2 = client1.cache("default2");
            IgniteInternalFuture<Object> f4 = GridTestUtils.runAsync(() -> {
                for (int k = 8000; k > 5000; k--) {
                    finalCache2.query(new SqlFieldsQuery("DELETE FROM T1 WHERE ID=" + k)).getAll();
                    //System.err.println("!!!!!delete");
                }
            });

            IgniteInternalFuture<Object> f2 = GridTestUtils.runAsync(() -> {
                latch.await();
                System.err.println("!!!! KILL !!!!");
                stopGrid(0);
            });

            //f2.listen(() -> checkDump(2));
            //f2.listen(() -> checkDump(2));

            f1.get();
            f2.get();
            f3.get();
            f4.get();

            //U.sleep(2000);
            checkDump(1);
            checkDump(2);
            System.err.println("!!!!: " + cache.query(new SqlFieldsQuery("select count (*) from t1")).getAll());
        }
    }

    /**
     * Test ensuring that idle verify dump output file is created exactly
     * on server node specified via the --host parameter.
     */
    @Test
    public void testDumpResultMatchesConnection() throws Exception {
        String URL = "jdbc:ignite:thin://127.0.0.1";

        Statement stmt = DriverManager.getConnection(URL).createStatement();

        stmt.execute("CREATE TABLE T1 (ID INT PRIMARY KEY, VAL INT)");

        //client.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE T1 (ID INT PRIMARY KEY, VAL INT)"));

        injectTestSystemOut();

        //client.createCache(DEFAULT_CACHE_NAME).put(1, 1);

        IgniteCache<String, Integer> cache = grid(1).cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++)
            cache.put("key" + k, k);

        stopAllGrids();

        startGrids(3);
        client = startGrid(CLIENT_NODE_NAME_PREFIX);

        awaitPartitionMapExchange();

        Ignite ignite = grid(1);

        cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.loadCache(null, 1000);

        for (int k = 0; k < 1000; k++) {
            String key = "key" + k;

            assertNotNull("Null value for key: " + key, cache.get(key));
            assertNotNull("Null value for key: " + key, cache.get(key));
        }

        //checkDump(2);

        CountDownLatch latch = new CountDownLatch(1);

        IgniteCache<String, Integer> finalCache = cache;
        IgniteInternalFuture<Object> f1 = GridTestUtils.runAsync(() -> {
            for (int k = 1000; k < 5000; k++) {
                Transaction tx = ignite.transactions().txStart(OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
                finalCache.put("key" + k, k);
                try {
                    tx.commit();
                } catch (IgniteException ex) {
                    try {
                        tx.rollback();
                    } catch (Throwable th) {
                        System.err.println("!!!! roll back failed");
                    }
                }
                if (k == 1500) {
                    latch.countDown();
                }
            }
        });

        IgniteInternalFuture<Object> f2 = GridTestUtils.runAsync(() -> {
            latch.await();
            stopGrid(0);
        });

        f2.get();
        f1.get();

        checkDump(2);


/*        f2.listen(() -> {
            try {
                U.sleep(1000);
            } catch (IgniteInterruptedCheckedException e) {
                //
            }
            checkDump(2);
        });
        f2.listen(() -> {
            try {
                U.sleep(500);
            } catch (IgniteInterruptedCheckedException e) {
                //
            }
            checkDump(2);
        });

        f1.get();
        f2.get();

        U.sleep(5000);

        IgniteInternalFuture<Object> fFinal = GridTestUtils.runAsync(() -> checkDump(2));

        fFinal.get();*/
    }

    /** */
    private void checkDump(int nodeIdx) {
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
