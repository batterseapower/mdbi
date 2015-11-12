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
        query(sql, new BatchRead<Void>() {
            public Void get(PreparedStatement s) throws SQLException {
                s.execute();
                return null;
            }
        });
    }

    public <T> List<T> queryList(SQL sql, Class<T> klass) throws SQLException {
        return query(sql, new ListBatchRead<T>(context.readers.get(klass)));
    }

    public <T> T query(SQL sql, BatchRead<T> batchRead) throws SQLException {
        // FIXME: support no preparedstatement
        // FIXME: transactions
        final SQLBuilder builder = new SQLBuilder(context.writers);
        builder.visitSQL(sql);
        try (final PreparedStatement ps = builder.build(connection)) {
            return batchRead.get(ps);
        }
    }
}
