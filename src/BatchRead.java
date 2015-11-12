import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface BatchRead<T> {
    T get(Read.Map ctxt, PreparedStatement ps) throws SQLException;
}
