import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface BoundWrite<T> {
    void set(PreparedStatement s, IndexRef ix, T x) throws SQLException;
}
