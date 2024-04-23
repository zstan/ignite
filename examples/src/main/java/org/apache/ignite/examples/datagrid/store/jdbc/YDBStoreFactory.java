package org.apache.ignite.examples.datagrid.store.jdbc;

import java.util.Objects;
import javax.cache.configuration.Factory;

public class YDBStoreFactory <K, V> implements Factory<YDBCacheStore<K, V>> {
    private String connUrl = "grpc://localhost:2136/local";

    private String cacheName;

    public YDBStoreFactory<K, V> setConnectionUrl(String connUrl) {
        this.connUrl = connUrl;

        return this;
    }

    public YDBStoreFactory<K, V> setCacheName(String cacheName) {
        this.cacheName = cacheName;

        return this;
    }

    @Override
    public YDBCacheStore<K, V> create() {
        Objects.requireNonNull(cacheName);

        return new YDBCacheStore<>(cacheName, connUrl);
    }
}
