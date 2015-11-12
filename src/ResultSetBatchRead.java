import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetBatchRead<T> {
    static <T> BatchRead<T> adapt(ResultSetBatchRead<T> rsbr) {
        return (ctxt, ps) -> rsbr.get(ctxt, ps.executeQuery());
    }

    T get(Read.Map ctxt, ResultSet rs) throws SQLException;
}
