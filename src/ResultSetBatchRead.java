import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetBatchRead<T> {
    public static <T> BatchRead<T> adapt(ResultSetBatchRead<T> rsbr) {
        return new BatchRead<T>() {
            @Override
            public T get(PreparedStatement ps) throws SQLException {
                return rsbr.get(ps.executeQuery());
            }
        };
    }

    T get(ResultSet rs) throws SQLException;
}
