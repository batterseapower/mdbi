import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MJDBC {
    private final Context context;
    private final Connection connection;
    private final boolean prepared;

    // FIXME: support DataSource
    // FIXME: named params? Maybe.
    // FIXME: transactions
    // FIXME: retry deadlocks
    public MJDBC(Context context, Connection connection) {
        this(context, connection, true);
    }

    private MJDBC(Context context, Connection connection,
                  boolean prepared) {
        this.context = context;
        this.connection = connection;
        this.prepared = prepared;
    }

    public MJDBC withPrepared(boolean prepared) {
        return new MJDBC(context, connection, prepared);
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
            try (final PreparedStatement ps = builder.build(connection)) {
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
        } else {
            final BatchUnpreparedSQLBuilder builder = new BatchUnpreparedSQLBuilder(context.writers);
            builder.visitSQL(sql);
            try (final Statement s = connection.createStatement()) {
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
            try (final PreparedStatement ps = builder.build(connection)) {
                return batchRead.get(context.readers, new PreparedStatementlike(ps));
            }
        } else {
            final UnpreparedSQLBuilder builder = new UnpreparedSQLBuilder(context.writers);
            builder.visitSQL(sql);
            try (final Statement s = connection.createStatement()) {
                return batchRead.get(context.readers, new UnpreparedStamentlike(s, builder.build()));
            }
        }
    }
}
