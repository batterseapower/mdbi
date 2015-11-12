import java.sql.ResultSet;
import java.sql.SQLException;

public interface BoundRead<T> {
    T get(ResultSet rs, IndexRef ix) throws SQLException;
}
