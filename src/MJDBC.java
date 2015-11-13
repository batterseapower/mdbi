
import javax.sql.DataSource;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MJDBC {
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

    // FIXME: transactions
    // FIXME: retry deadlocks
    public MJDBC(Context context, Connection connection) {
        this(context, ConnectionObtainer.fromConnection(connection), true);
    }
    public MJDBC(Context context, DataSource dataSource) {
        this(context, ConnectionObtainer.fromDataSource(dataSource), true);
    }

    private MJDBC(Context context, ConnectionObtainer connectionObtainer,
                  boolean prepared) {
        this.context = context;
        this.connectionObtainer = connectionObtainer;
        this.prepared = prepared;
    }

    public MJDBC withPrepared(boolean prepared) {
        return new MJDBC(context, connectionObtainer, prepared);
    }

    public void execute(SQL sql) throws SQLException {
        query(sql, (ctxt, s) -> {
            s.execute();
            return null;
        });
    }

    public long[] updateBatch(SQL sql) throws SQLException {
        if (prepared) {
            final BatchPreparedSQLBuilder builder = new BatchPreparedSQLBuilder(context.writers);
            builder.visitSQL(sql);
            return connectionObtainer.with(c -> {
                try (final PreparedStatement ps = builder.build(c)) {
                    try {
                        return ps.executeLargeBatch();
                    } catch (UnsupportedOperationException _) {
                        final int[] ints = ps.executeBatch();
                        final long[] longs = new long[ints.length];
                        for (int i = 0; i < ints.length; i++) {
                            longs[i] = ints[i];
                        }
                        return longs;
                    }
                }
            });
        } else {
            final BatchUnpreparedSQLBuilder builder = new BatchUnpreparedSQLBuilder(context.writers);
            builder.visitSQL(sql);
            return connectionObtainer.with(c -> {
                try (final Statement s = c.createStatement()) {
                    final Map.Entry<Integer, Iterator<String>> e = builder.buildIterator();
                    final Iterator<String> it = e.getValue();
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
                            } catch (UnsupportedOperationException _) {
                                supportsLargeUpdate = false;
                                result[i] = s.executeUpdate(x);
                            }
                        }
                        i++;
                    }

                    return result;
                }
            });
        }
    }

    public long update(SQL sql) throws SQLException {
        return query(sql, (ctxt, s) -> {
            try {
                return s.executeLargeUpdate();
            } catch (UnsupportedOperationException _) {
                return (long)s.executeUpdate();
            }
        });
    }

    public <T> List<T> queryList(SQL sql, Class<T> klass) throws SQLException {
        return queryList(sql, new ContextRead<T>(klass));
    }

    public <T> List<T> queryList(SQL sql, Read<T> read) throws SQLException {
        return query(sql, new ListBatchRead<T>(read));
    }

    public <T> T queryExactlyOne(SQL sql, Class<T> klass) throws SQLException {
        return queryExactlyOne(sql, new ContextRead<>(klass));
    }

    public <T> T queryExactlyOne(SQL sql, Read<T> read) throws SQLException {
        return query(sql, new ExactlyOneBatchRead<T>(read));
    }

    public <T> T query(SQL sql, BatchRead<T> batchRead) throws SQLException {
        if (prepared) {
            final PreparedSQLBuilder builder = new PreparedSQLBuilder(context.writers);
            builder.visitSQL(sql);
            return connectionObtainer.with(c -> {
                try (final PreparedStatement ps = builder.build(c)) {
                    return batchRead.get(context.readers, new PreparedStatementlike(ps));
                }
            });
        } else {
            final UnpreparedSQLBuilder builder = new UnpreparedSQLBuilder(context.writers);
            builder.visitSQL(sql);
            return connectionObtainer.with(c -> {
                try (final Statement s = c.createStatement()) {
                    return batchRead.get(context.readers, new UnpreparedStamentlike(s, builder.build()));
                }
            });
        }
    }
}
