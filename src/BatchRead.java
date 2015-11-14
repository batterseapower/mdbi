import java.sql.SQLException;

public interface BatchRead<T> {
    T get(Reads.Map ctxt, Statementlike ps) throws SQLException;
}
