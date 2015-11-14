package uk.co.omegaprime.mdbi;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class PreparedStatementlike implements Statementlike {
    private final PreparedStatement ps;

    public PreparedStatementlike(PreparedStatement ps) {
        this.ps = ps;
    }

    @Override
    public void execute() throws SQLException {
        ps.execute();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return ps.executeUpdate();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return ps.executeLargeUpdate();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return ps.executeQuery();
    }
}
