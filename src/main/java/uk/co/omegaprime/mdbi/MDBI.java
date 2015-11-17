package uk.co.omegaprime.mdbi;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class MDBI {
    private interface ConnectionObtainer {
        <T> T with(ConnectionUser<T> user) throws SQLException;

        static ConnectionObtainer fromDataSource(DataSource dataSource) {
            return new ConnectionObtainer() {
                @Override
                public <T> T with(ConnectionUser<T> user) throws SQLException {
                    try (final Connection c = dataSource.getConnection()) {
                        return user.consume(c);
                    }
                }
            };
        }

        static ConnectionObtainer fromConnection(Connection connection) {
            return new ConnectionObtainer() {
                @Override
                public <T> T with(ConnectionUser<T> user) throws SQLException {
                    return user.consume(connection);
                }
            };
        }
    }

    private interface ConnectionUser<T> {
        T consume(Connection c) throws SQLException;
    }

    private final Context context;
    private final ConnectionObtainer connectionObtainer;
    private final boolean prepared;
    private final Supplier<Retry> retryPolicy;

    // TODO: support generated keys? Bit awkward because we need to know we need the feature when we prepare the stmt.
    public static MDBI of(Connection connection) {
        return MDBI.of(Context.DEFAULT, connection);
    }
    public static MDBI of(DataSource dataSource) {
        return MDBI.of(Context.DEFAULT, dataSource);
    }
    public static MDBI of(Context context, Connection connection) {
        return new MDBI(context, ConnectionObtainer.fromConnection(connection));
    }
    public static MDBI of(Context context, DataSource dataSource) {
        return new MDBI(context, ConnectionObtainer.fromDataSource(dataSource));
    }

    private MDBI(Context context, ConnectionObtainer connectionObtainer) {
        this(context, connectionObtainer, true, Retries::deadlocks);
    }

    private MDBI(Context context, ConnectionObtainer connectionObtainer,
                 boolean prepared, Supplier<Retry> retryPolicy) {
        this.context = context;
        this.connectionObtainer = connectionObtainer;
        this.prepared = prepared;
        this.retryPolicy = retryPolicy;
    }

    public boolean isPrepared() { return prepared; }
    public MDBI withPrepared(boolean prepared) {
        return new MDBI(context, connectionObtainer, prepared, retryPolicy);
    }

    // Note that the retry policy will only be used when executing a query against a connection with no open transaction
    public Supplier<Retry> getRetryPolicy() { return retryPolicy; }
    public MDBI withRetryPolicy(Supplier<Retry> retryPolicy) {
        return new MDBI(context, connectionObtainer, prepared, retryPolicy);
    }

    public Context getContext() { return context; }
    public MDBI withContext(Context context) {
        return new MDBI(context, connectionObtainer, prepared, retryPolicy);
    }

    public static SQL sql(String x) {
        return new SQL(SnocList.singleton(x), null);
    }

    public void execute(SQL sql) throws SQLException {
        query(sql, (ctxt, s) -> {
            s.execute();
            return null;
        });
    }

    public long[] updateBatch(SQL sql) throws SQLException {
        if (prepared) {
            return connectionObtainer.with(c -> {
                try (final PreparedStatement ps = BatchPreparedSQLBuilder.build(sql, context.writeContext(), c)) {
                    return retry(c, () -> {
                        try {
                            return ps.executeLargeBatch();
                        } catch (UnsupportedOperationException _unsupported) {
                            final int[] ints = ps.executeBatch();
                            final long[] longs = new long[ints.length];
                            for (int i = 0; i < ints.length; i++) {
                                longs[i] = ints[i];
                            }
                            return longs;
                        }
                    });
                }
            });
        } else {
            return connectionObtainer.with(c -> {
                try (final Statement s = c.createStatement()) {
                    final Map.Entry<Integer, Iterator<String>> e = BatchUnpreparedSQLBuilder.build(sql, context.writeContext());
                    final Iterator<String> it = e.getValue();

                    return Transactionally.run(c, () -> retry(c, () -> {
                        final long[] result = new long[e.getKey()];
                        boolean supportsLargeUpdate = true;
                        int i = 0;
                        while (it.hasNext()) {
                            final String x = it.next();
                            if (!supportsLargeUpdate) {
                                result[i] = s.executeUpdate(x);
                            } else {
                                try {
                                    result[i] = s.executeLargeUpdate(x);
                                } catch (UnsupportedOperationException _unsupported) {
                                    supportsLargeUpdate = false;
                                    result[i] = s.executeUpdate(x);
                                }
                            }
                            i++;
                        }

                        return result;
                    }));
                }
            });
        }
    }

    public long update(SQL sql) throws SQLException {
        return query(sql, (ctxt, s) -> {
            try {
                return s.executeLargeUpdate();
            } catch (UnsupportedOperationException _unsupported) {
                return (long)s.executeUpdate();
            }
        });
    }

    public <T> List<T> queryList(SQL sql, Class<T> klass) throws SQLException {
        return queryList(sql, new ContextRead<>(klass));
    }

    public <T> List<T> queryList(SQL sql, Read<T> read) throws SQLException {
        return query(sql, BatchReads.asList(read));
    }

    public <K, V> Map<K, V> queryMap(SQL sql, Class<K> keyKlass, Class<V> valueKlass) throws SQLException {
        return queryMap(sql, new ContextRead<>(keyKlass), new ContextRead<>(valueKlass));
    }

    public <K, V> Map<K, V> queryMap(SQL sql, Read<K> keyRead, Read<V> valueRead) throws SQLException {
        return query(sql, BatchReads.asMap(keyRead, valueRead));
    }

    public <T> T queryFirst(SQL sql, Class<T> klass) throws SQLException {
        return queryFirst(sql, new ContextRead<>(klass));
    }

    public <T> T queryFirst(SQL sql, Read<T> read) throws SQLException {
        return query(sql, BatchReads.first(read));
    }

    public <T> T query(SQL sql, BatchRead<T> batchRead) throws SQLException {
        if (prepared) {
            return connectionObtainer.with(c -> {
                try (final PreparedStatement ps = BespokePreparedSQLBuilder.build(sql, context.writeContext(), c)) {
                    return retry(c, () -> batchRead.get(context.readContext(), new PreparedStatementlike(ps)));
                }
            });
        } else {
            return connectionObtainer.with(c -> {
                try (final Statement s = c.createStatement()) {
                    return retry(c, () -> batchRead.get(context.readContext(), new UnpreparedStatementlike(s, BespokeUnpreparedSQLBuilder.build(sql, context.writeContext()))));
                }
            });
        }
    }

    private <T> T retry(Connection c, SQLAction<T> act) throws SQLException {
        if (!c.getAutoCommit()) {
            // Already in transaction, we can't safely retry because failure of the SQL action we
            // are trying to do might cause rollback. Example: what if we execute these 3 one after another:
            //   update tab set x = 1
            //   begin tran; update tab set x = 2
            //   select x from tab  // <-- if this deadlocks then the rollback causes the update to be lost. If we just retry this statement we'll return 1 (unexpected).
            return act.run();
        } else {
            final Retry retry = retryPolicy.get();
            while (true) {
                try {
                    return Transactionally.run(c, act);
                } catch (RuntimeException e) {
                    retry.consider(e);
                } catch (SQLException e) {
                    retry.consider(e);
                }
            }
        }
    }
}
