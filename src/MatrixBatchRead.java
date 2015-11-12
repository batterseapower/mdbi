import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MatrixBatchRead implements BatchRead<Object[]> {
    private final List<Read<?>> reads;

    public MatrixBatchRead(Class<?>... klasses) {
        this(Arrays.asList(klasses).stream().map(ContextRead::new).collect(Collectors.toList()));
    }

    public MatrixBatchRead(List<Read<?>> reads) {
        this.reads = reads;
    }

    @Override
    public Object[] get(Read.Map ctxt, PreparedStatement ps) throws SQLException {
        return ResultSetBatchRead.adapt((ctxt1, rs) -> {
            final List<BoundRead<?>> boundReads = reads.stream().map(read -> read.bind(ctxt1)).collect(Collectors.toList());

            final List[] columnLists = new List[reads.size()];
            for (int i = 0; i < columnLists.length; i++) {
                columnLists[i] = new ArrayList<>();
            }

            while (rs.next()) {
                final IndexRef ix = new IndexRef();
                for (int i = 0; i < columnLists.length; i++) {
                    columnLists[i].add(boundReads.get(i).get(rs, ix));
                }
            }

            final Object[] columns = new Object[columnLists.length];
            for (int i = 0; i < columnLists.length; i++) {
                final List list = columnLists[i];
                columns[i] = listToArray(reads.get(i).getElementClass(), list);
            }

            return columns;
        }).get(ctxt, ps);
    }

    // FIXME: support other primitives
    private static Object listToArray(Class<?> klass, List list) {
        if (klass == int.class) {
            final int[] result = new int[list.size()];
            for (int i = 0; i < list.size(); i++) result[i] = (int)list.get(i);
            return result;
        } else if (Object.class.isAssignableFrom(klass)) {
            final Object[] result = (Object[])Array.newInstance(klass, list.size());
            for (int i = 0; i < list.size(); i++) result[i] = list.get(i);
            return result;
        } else {
            throw new IllegalStateException("Please add support for primitive type " + klass);
        }
    }
}
