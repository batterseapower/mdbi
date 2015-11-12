import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface BatchRead<T> {
    T get(PreparedStatement ps) throws SQLException;
}
