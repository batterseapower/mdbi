import java.sql.ResultSet;
import java.sql.SQLException;

// The whole point of this interface is to give Statement and PreparedStatement a common API
public interface Statementlike {
    void execute() throws SQLException;
    int executeUpdate() throws SQLException;
    long executeLargeUpdate() throws SQLException;
    ResultSet executeQuery() throws SQLException;
}
