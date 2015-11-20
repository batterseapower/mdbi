package uk.co.omegaprime.mdbi;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** The whole point of this interface is to give {@link Statement} and {@link PreparedStatement} a common API for use in {@link BatchRead}. */
public interface Statementlike {
    void execute() throws SQLException;
    int executeUpdate() throws SQLException;
    long executeLargeUpdate() throws SQLException;
    ResultSet executeQuery() throws SQLException;
}
