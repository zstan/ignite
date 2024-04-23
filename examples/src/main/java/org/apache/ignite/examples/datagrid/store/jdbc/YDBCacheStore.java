package org.apache.ignite.examples.datagrid.store.jdbc;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.resources.IgniteInstanceResource;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableDescription.Builder;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;

public class YDBCacheStore <K, V> extends CacheStoreAdapter<K, V> {
    private final String cacheName;
    private SessionRetryContext retryCtx;
    AtomicBoolean initialized = new AtomicBoolean();
    boolean dropBeforeCreate = true;

    @IgniteInstanceResource
    private Ignite ignite;

    private String database;

    public YDBCacheStore(String cacheName, String connUrl) {
        this.cacheName = cacheName;

        GrpcTransport transport = GrpcTransport.forConnectionString(connUrl)
                //.withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build();

        database = transport.getDatabase();

        TableClient tableClient = TableClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    private void init() {
        if (initialized.compareAndSet(false, true)) {
            Builder ydbTableBuilder = TableDescription.newBuilder();

            Collection<GridQueryTypeDescriptor> types = ((IgniteEx) ignite).context().query().types(cacheName);

            Set<String> pkeys = null;
            Map<String, Class<?>> fields = null;
            Map<String, GridQueryProperty> fieldProperties = null;

            assert types.size() == 1;

            for (GridQueryTypeDescriptor type : types) {
                pkeys = type.primaryKeyFields();

                if (pkeys.isEmpty()) {
                    pkeys = Collections.singleton(type.keyFieldName());
                }

                fields = type.fields();
                fieldProperties = type.properties();
            }

            for (Map.Entry<String, Class<?>> fldInfo : fields.entrySet()) {
                PrimitiveType type0;
                switch (fldInfo.getValue().getSimpleName()) {
                    case "Integer":
                        type0 = PrimitiveType.Int32;
                        break;
                    case "Long":
                        type0 = PrimitiveType.Int64;
                        break;
                    case "String":
                        type0 = PrimitiveType.Text;
                        break;
                    default:
                        throw new UnsupportedOperationException("Undefined mapping for [" + fldInfo.getValue().getSimpleName() + "] type");
                }

                GridQueryProperty fldProp = Objects.requireNonNull(fieldProperties.get(fldInfo.getKey()));

/*                if (fldProp.notNull()) { // need to discuss with ya what the problem here
                    ydbTableBuilder.addNonnullColumn(fldInfo.getKey(), type0);
                } else {
                    ydbTableBuilder.addNullableColumn(fldInfo.getKey(), type0);
                }*/

                ydbTableBuilder.addNullableColumn(fldInfo.getKey(), type0);
            }
            ydbTableBuilder.setPrimaryKeys(pkeys.toArray(new String[pkeys.size()]));

            TableDescription table = ydbTableBuilder.build();

            if (dropBeforeCreate) {
                retryCtx.supplyStatus(session -> session.dropTable(database + "/" + cacheName)).join();
            }

            retryCtx.supplyStatus(session -> session.createTable(database + "/" + cacheName, table))
                    .join().expectSuccess();
        }
    }

    @Override
    public V load(K k) throws CacheLoaderException {
        init();

        String query
                = "SELECT ID, VAL, SVAL "
                + "FROM %s WHERE ID = " + k + ";";

        String query0 = String.format(query, cacheName);

        TxControl txControl = TxControl.serializableRw().setCommitTx(true);

        Result<DataQueryResult> result = retryCtx.supplyResult(session -> session.executeDataQuery(query0, txControl)).join();

        DataQueryResult res = result.getValue();

        ResultSetReader rs = res.getResultSet(0);

        while (rs.next()) {
            return (V) new FlexHolder(rs.getColumn("VAL").getInt32(), rs.getColumn("SVAL").getText());
        }

        return null;
    }

    @Override
    public void write(Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        FlexHolder holder = (FlexHolder) entry.getValue();
        String query
                = "INSERT INTO %s (ID, VAL, SVAL) "
                + "VALUES (" + entry.getKey() + ", " + holder.intHolder + ", \"" + holder.strHolder + "\");";

        String query0 = String.format(query, cacheName);

        TxControl txControl = TxControl.serializableRw().setCommitTx(true);

        retryCtx.supplyResult(session -> session.executeDataQuery(query0, txControl)).join();
    }

    @Override
    public void delete(Object key) throws CacheWriterException {
        // key can be complex !!!
        String query = "delete from %s where ID=" + key;

        TxControl txControl = TxControl.serializableRw().setCommitTx(true);

        String query0 = String.format(query, cacheName);

        retryCtx.supplyResult(session -> session.executeDataQuery(query0, txControl)).join();
    }
}
