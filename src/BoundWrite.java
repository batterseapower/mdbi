import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public interface BoundWrite<T> {
    int arity();

    void set(PreparedStatement s, IndexRef ix, T x) throws SQLException;

    List<String> asSQL(T x);
}
