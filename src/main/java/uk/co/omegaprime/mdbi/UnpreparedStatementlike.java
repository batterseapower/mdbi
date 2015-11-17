package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class UnpreparedStatementlike implements Statementlike {
    private final Statement s;
    private final String sql;

    public UnpreparedStatementlike(Statement s, String sql) {
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
