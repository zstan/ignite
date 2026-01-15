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
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.util.GridCommandHandlerClusterByClassTest.dumpFileNameMatcher;

/** */
public class IdleVerifyDumpTest extends GridCommandHandlerClusterByClassAbstractTest {
    @Override protected boolean persistenceEnable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {

        CacheConfiguration cc = cacheConfiguration(igniteInstanceName);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cc);

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

    /**
     * Test ensuring that idle verify dump output file is created exactly
     * on server node specified via the --host parameter.
     */
    @Test
    public void testDumpResultMatchesConnection() throws Exception {
        String URL = "jdbc:ignite:thin://127.0.0.1";

        //Statement stmt = DriverManager.getConnection(URL).createStatement();

        //stmt.executeQuery("CREATE TABLE T1 (ID INT PRIMARY KEY, VAL INT)");

        //client.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE T1 (ID INT PRIMARY KEY, VAL INT)"));

        injectTestSystemOut();

        //client.createCache(DEFAULT_CACHE_NAME).put(1, 1);

        IgniteCache<String, Integer> cache = grid(1).cache(DEFAULT_CACHE_NAME);
        grid(1).getOrCreateCache("default1");

        client.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE T1 (ID VARCHAR PRIMARY KEY, VAL INT) WITH " +
                "\"template=replicated,atomicity=TRANSACTIONAL,CACHE_NAME=default1\""));

        for (int k = 0; k < 1000; k++) {
            client.cache("default1").withKeepBinary().query(new SqlFieldsQuery("INSERT INTO T1 VALUES ('" + k + "', " + k + ")")).getAll();
            //cache.put("key" + k, k);
        }

        stopAllGrids();

        startGrids(3);
        client = startGrid(CLIENT_NODE_NAME_PREFIX);

        awaitPartitionMapExchange();

        Ignite ignite = grid(1);

        cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.loadCache(null, 1000);

        System.err.println("!!!: " + cache.size());

        for (int k = 0; k < 1000; k++) {
            String key = "key" + k;

            assertNotNull("Null value for key: " + key, cache.get(key));
            assertNotNull("Null value for key: " + key, cache.get(key));
        }

        //checkDump(2);

        CountDownLatch latch = new CountDownLatch(1);

/*        client.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE T1 (ID VARCHAR PRIMARY KEY, VAL INT) WITH " +
                "\"template=replicated,atomicity=TRANSACTIONAL,CACHE_NAME=default\""));*/

        IgniteCache<String, Integer> finalCache = cache;
        IgniteInternalFuture<Object> f1 = GridTestUtils.runAsync(() -> {
            for (int k = 1000; k < 5000; k++) {
                //Transaction tx = ignite.transactions().txStart(OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
                //finalCache.put("key" + k, k);
                //stmt.executeQuery("INSERT INTO T1 VALUES (" + k + ", " + k + ")");
                client.cache(DEFAULT_CACHE_NAME).withKeepBinary().query(new SqlFieldsQuery("INSERT INTO T1 VALUES ('" + k + "', " + k + ")")).getAll();
                System.err.println("1!!insert");
/*                try {
                    tx.commit();
                } catch (IgniteException ex) {
                    try {
                        tx.rollback();
                    } catch (Throwable th) {
                        System.err.println("!!!! roll back failed");
                    }
                }*/
                if (k == 1500) {
                    latch.countDown();
                }
            }
        });

        IgniteInternalFuture<Object> f2 = GridTestUtils.runAsync(() -> {
            latch.await();
            stopGrid(0);
        });

        f1.get();
        f2.get();

        checkDump(2);

        FieldsQueryCursor<List<?>> res = client.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("SELECT COUNT(*) from T1"));

        System.err.println("!!!!: " + res.getAll().get(0));

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
