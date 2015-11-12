import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MatrixBatchRead implements BatchRead<Object[]> {
    private final List<Read<?>> reads;

    public MatrixBatchRead(Context ctxt, Class<?>... klasses) {
        this(Arrays.asList(klasses).stream().map((Function<Class<?>, Read<?>>) ctxt.readers::get).collect(Collectors.toList()));
    }

    public MatrixBatchRead(List<Read<?>> reads) {
        this.reads = reads;
    }

    @Override
    public Object[] get(PreparedStatement ps) throws SQLException {
        return ResultSetBatchRead.adapt(new ResultSetBatchRead<Object[]>() {
            @Override
            @SuppressWarnings("unchecked")
            public Object[] get(ResultSet rs) throws SQLException {
                final List[] columnLists = new List[reads.size()];
                for (int i = 0; i < columnLists.length; i++) {
                    columnLists[i] = new ArrayList<>();
                }

                while (rs.next()) {
                    final IndexRef ix = new IndexRef();
                    for (int i = 0; i < columnLists.length; i++) {
                        columnLists[i].add(reads.get(i).get(rs, ix));
                    }
                }

                final Object[] columns = new Object[columnLists.length];
                for (int i = 0; i < columnLists.length; i++) {
                    final List list = columnLists[i];
                    columns[i] = listToArray(reads.get(i).getElementClass(), list);
                }

                return columns;
            }
        }).get(ps);
    }

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
