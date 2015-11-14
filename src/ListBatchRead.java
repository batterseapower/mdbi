import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ListBatchRead<T> implements BatchRead<List<T>> {
    private final Read<T> read;

    public ListBatchRead(Read<T> read) {
        this.read = read;
    }

    @Override
    public List<T> get(Reads.Map ctxt, Statementlike ps) throws SQLException {
        return ResultSetBatchRead.adapt((ctxt2, rs) -> {
            final BoundRead<T> boundRead = read.bind(ctxt2);

            final List<T> result = new ArrayList<>();
            while (rs.next()) {
                result.add(boundRead.get(rs, new IndexRef()));
            }
            return result;
        }).get(ctxt, ps);
    }
}
