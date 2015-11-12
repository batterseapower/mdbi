import java.sql.*;
import java.util.HashMap;
import java.util.List;

public class MJDBC {
    private final Context context;
    private final Connection connection;

    // FIXME: support DataSource
    public MJDBC(Context context, Connection connection) {
        this.context = context;
        this.connection = connection;
    }

    public void execute(SQL sql) throws SQLException {
        query(sql, (ctxt, s) -> {
            s.execute();
            return null;
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
        // FIXME: support no preparedstatement
        // FIXME: transactions
        // FIXME: retry deadlocks
        final SQLBuilder builder = new SQLBuilder(context.writers);
        builder.visitSQL(sql);
        try (final PreparedStatement ps = builder.build(connection)) {
            return batchRead.get(context.readers, ps);
        }
    }
}
