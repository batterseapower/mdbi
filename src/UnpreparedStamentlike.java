import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class UnpreparedStamentlike implements Statementlike {
    private final Statement s;
    private final String sql;

    public UnpreparedStamentlike(Statement s, String sql) {
        this.s = s;
        this.sql = sql;
    }

    @Override
    public void execute() throws SQLException {
        s.execute(sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return s.executeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return s.executeLargeUpdate(sql);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return s.executeQuery(sql);
    }
}
