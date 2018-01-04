package uk.co.omegaprime.mdbi;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The most important class in the library: a wrapper around a {@link Connection} or {@link DataSource} that gives it superpowers.
 * <p>
 * To get hold of one of these, you'll probably want to use either {@link #of(Connection)} or {@link #of(DataSource)}.
 */
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
    private final boolean prepared, transactional;
    private final Supplier<Retry> retryPolicy;

    // TODO: support generated keys? Bit awkward because we need to know we need the feature when we prepare the stmt.

    /** Creates a {@code MDBI} where all queries are executed against this connection. */
    public static MDBI of(Connection connection) {
        return MDBI.of(Context.DEFAULT, connection);
    }
    /** Creates a {@code MDBI} where all queries are executed against fresh connections retrieved from the {@code DataSource}. */
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
        this(context, connectionObtainer, true, true, Retries::deadlocks);
    }

    private MDBI(Context context, ConnectionObtainer connectionObtainer,
                 boolean prepared, boolean transactional, Supplier<Retry> retryPolicy) {
        this.context = context;
        this.connectionObtainer = connectionObtainer;
        this.prepared = prepared;
        this.transactional = transactional;
        this.retryPolicy = retryPolicy;
    }

    /** Should we use {@link PreparedStatement}s to execute SQL (the default)? Or should we instead construct SQL strings for use with {@link Statement}? */
    public boolean isPrepared() { return prepared; }
    public MDBI withPrepared(boolean prepared) {
        return new MDBI(context, connectionObtainer, prepared, transactional, retryPolicy);
    }

    /**
     * Should we use execute the SQL statement in a transaction?
     * <p>
     * Note that if transactions are disabled then the retry policy ({@link #getRetryPolicy()}) will never be used.
     */
    public boolean isTransactional() { return transactional; }
    public MDBI withTransactional(boolean transactional) {
        return new MDBI(context, connectionObtainer, prepared, transactional, retryPolicy);
    }

    /**
     * Specifies how exceptions that occur during querying should be handled. By default, we'll retry
     * deadlocks using the {@link Retries#deadlocks()} strategy.
     * <p>
     * For more about what how to construct a policy, see the documentation of {@link Retry}.
     * <p>
     * <b>Important note:</b> the retry policy will only be used if both:
     * <ol>
     *     <li>Transactions are enabled in MDBI (the default)</li>
     *     <li>You executing a query against a
     *         connection with no open transaction. i.e. it will always come into play if you constructed this
     *         {@code MDBI} using a {@code DataSource}, but won't be used if you constructed it from a {@code Connection}
     *         with autocommit set to false. You might think this is a strange restriction, but if we didn't have it
     *         then users of transactions might get just <i>part</i> of their transaction retried, which would be really surprising.
     *     </li>
     * </ol>
     */
    public Supplier<Retry> getRetryPolicy() { return retryPolicy; }
    public MDBI withRetryPolicy(Supplier<Retry> retryPolicy) {
        return new MDBI(context, connectionObtainer, prepared, transactional, retryPolicy);
    }

    public Context getContext() { return context; }
    public MDBI withContext(Context context) {
        return new MDBI(context, connectionObtainer, prepared, transactional, retryPolicy);
    }

    /** Executes a query and throws away the result, if any. */
    public void execute(SQL sql) throws SQLException {
        query(sql, (StatementlikeBatchRead<Void>) (ctxt, s) -> {
            s.execute();
            return null;
        });
    }

    /** Executes a batch query, and returns the number of rows affected by each statement in the batch. */
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

    /** Executes a query and returns the number of rows affected */
    public long update(SQL sql) throws SQLException {
        return query(sql, (StatementlikeBatchRead<Long>) (ctxt, s) -> {
            try {
                return s.executeLargeUpdate();
            } catch (UnsupportedOperationException _unsupported) {
                return (long)s.executeUpdate();
            }
        });
    }

    /** Executes a query and interprets each row of the result as an instance of the supplied class. */
    public <T> List<T> queryList(SQL sql, Class<T> klass) throws SQLException {
        return queryList(sql, new ContextRead<>(klass));
    }

    /** Executes a query and interprets each row using the supplied {@code Read} instance. */
    public <T> List<T> queryList(SQL sql, Read<T> read) throws SQLException {
        return query(sql, BatchReads.asList(read));
    }

    /** Executes a query and interprets each row of the result as an entry in a map using the supplied classes. */
    public <K, V> Map<K, V> queryMap(SQL sql, Class<K> keyKlass, Class<V> valueKlass) throws SQLException {
        return queryMap(sql, new ContextRead<>(keyKlass), new ContextRead<>(valueKlass));
    }

    /** Executes a query and interprets each row of the result as an entry in a map using the supplied {@code Read} instances. */
    public <K, V> Map<K, V> queryMap(SQL sql, Read<K> keyRead, Read<V> valueRead) throws SQLException {
        return query(sql, BatchReads.asMap(keyRead, valueRead));
    }

    /**
     * Executes a query and returns the first row of the result interpreted as the supplied class.
     *
     * @throws java.util.NoSuchElementException if the result is empty.
     */
    public <T> T queryFirst(SQL sql, Class<T> klass) throws SQLException {
        return queryFirst(sql, new ContextRead<>(klass));
    }

    /**
     * Executes a query and returns the first row of the result interpreted using the supplied {@code Read} instance.
     *
     * @throws java.util.NoSuchElementException if the result is empty.
     */
    public <T> T queryFirst(SQL sql, Read<T> read) throws SQLException {
        return query(sql, BatchReads.first(read));
    }

    /**
     * Executes a query and returns the first row of the result interpreted as the supplied class, or null if there is no such row.
     */
    public <T> T queryFirstOrNull(SQL sql, Class<T> klass) throws SQLException {
        return queryFirstOrNull(sql, new ContextRead<>(klass));
    }

    /**
     * Executes a query and returns the first row of the result interpreted using the supplied {@code Read} instance, or null if there is no such row.
     */
    public <T> T queryFirstOrNull(SQL sql, Read<T> read) throws SQLException {
        return query(sql, BatchReads.firstOrNull(read));
    }

    /** Executes a query and interprets the result in a fully customizable way using the {@code BatchRead} instance. */
    public <T> T query(SQL sql, BatchRead<T> batchRead) throws SQLException {
        return query(sql, StatementlikeBatchReads.fromBatchRead(batchRead));
    }

    /** Executes a query and interprets the result in a fully customizable way using the {@code StatementlikeBatchRead} instance. */
    public <T> T query(SQL sql, StatementlikeBatchRead<T> batchRead) throws SQLException {
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
        if (!transactional || !c.getAutoCommit()) {
            // Already in transaction, we can't safely retry because failure of the SQL action we
            // are trying to do might cause rollback. Example: what if we execute these 3 one after another:
            //   update tab set x = 1
            //   begin tran; update tab set x = 2
            //   select x from tab  // <-- if this deadlocks then the rollback causes the update to be lost. If we just retry this statement we'll return 1 (unexpected).
            return act.run();
        } else {
            return Transactionally.runWithRetry(c, retryPolicy.get(), act);
        }
    }

    // Static versions of SQL.* methods below this line:

    /** See {@link SQL#$(Object)}. */
    public static SQL $(@Nullable Object x) {
        return sql("").$(x);
    }

    /** See {@link SQL#$(Class, Object)}. */
    public static <T> SQL $(Class<T> klass, @Nullable T x) {
        return sql("").$(klass, x);
    }

    /** See {@link SQL#$(Write, Object)}. */
    public static <T> SQL $(Write<T> write, @Nullable T x) {
        return sql("").$(write, x);
    }

    /** See {@link SQL#$s(Collection)}. */
    public static <T> SQL $s(Collection<T> arg) {
        return sql("").$s(arg);
    }

    /** See {@link SQL#$s(Class, Collection)}. */
    public static <T> SQL $s(Class<T> klass, Collection<T> x) {
        return sql("").$s(klass, x);
    }

    /** See {@link SQL#$s(Write, Collection)}. */
    public static <T> SQL $s(Write<T> write, Collection<T> x) {
        return sql("").$s(write, x);
    }

    /**
     * Constructs a simple {@link SQL} object representing just the supplied SQL fragment. See {@link SQL#sql(String)}.
     * <p>
     * For the best ergonomics, we suggest that you import this method using a static import.
     */
    public static SQL sql(String x) {
        return new SQL(SnocList.singleton(x), null);
    }

    /** See {@link SQL#sql(Object...)}. */
    public static SQL sql(Object... bits) {
        return sql("").sql(bits);
    }

    /** See {@link SQL#in(Object...)}. */
    @SafeVarargs
    public static <T> SQL in(Object... xs) {
        return sql("").in(xs);
    }

    /** See {@link SQL#in(Iterable)}. */
    public static <T> SQL in(Iterable<T> xs) {
        return sql("").in(xs);
    }

    /** See {@link SQL#in(Class, Iterable)}. */
    public static <T> SQL in(Class<T> klass, Iterable<T> xs) {
        return sql("").in(klass, xs);
    }

    /** See {@link SQL#in(Write, Iterable)}. */
    public static <T> SQL in(Write<T> write, Iterable<T> xs) {
        return sql("").in(write, xs);
    }
}
