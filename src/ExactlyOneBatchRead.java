import java.sql.SQLException;
import java.util.NoSuchElementException;

public class ExactlyOneBatchRead<T> implements BatchRead<T> {
    private final Read<T> read;

    public ExactlyOneBatchRead(Read<T> read) {
        this.read = read;
    }

    @Override
    public T get(Reads.Map ctxt, Statementlike ps) throws SQLException {
        return ResultSetBatchRead.adapt((ctxt1, rs) -> {
            if (rs.next()) {
                return read.bind(ctxt1).get(rs, new IndexRef());
            } else {
                throw new NoSuchElementException();
            }
        }).get(ctxt, ps);
    }
}
