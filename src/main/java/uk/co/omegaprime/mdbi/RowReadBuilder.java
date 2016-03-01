package uk.co.omegaprime.mdbi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.*;

/**
 * A convenience wrapper around the {@link Reads#list(Collection)} functionality that avoids you having to track column indexes.
 * <p>
 * <pre>
 * RowReadBuilder rrb = RowReadBuilder.create();
 * Supplier&lt;String&gt; names = rrb.add(sql("name"), String.class);
 * IntSupplier ages = rrb.addInt(sql("age"));
 *
 * for (List&lt;Object&gt; row : mdbi.queryList(sql("select ", columns, " from people"), rrb.build())) {
 *     rrb.bindSuppliers(row);
 *     System.out.println("Hello " + names.get() + " of age " + ages.get());
 * }
 * </pre>
 */
public class RowReadBuilder {
    private final List<SQL> columns = new ArrayList<>();
    private final List<Read<?>> reads = new ArrayList<>();
    private final List<CompletableSupplier<?>> suppliers = new ArrayList<>();

    private static class CompletableSupplier<T> implements Supplier<T> {
        public T value;

        @Override
        public T get() {
            if (value == null) {
                throw new IllegalStateException("You must bindSuppliers on the corresponding RowReadBuilder before invoking a Supplier that it returns");
            }

            return value;
        }
    }

    private RowReadBuilder() {}

    public static RowReadBuilder create() {
        return new RowReadBuilder();
    }

    /** Use the supplied row to bind all the {@code Supplier} objects that we have returned. */
    @SuppressWarnings("unchecked")
    public void bindSuppliers(List<?> row) {
        for (int i = 0; i < row.size(); i++) {
            ((CompletableSupplier<Object>)suppliers.get(i)).value = row.get(i);
        }
    }

    /** Returns comma delimited column list */
    public SQL buildColumns() {
        return SQL.commaSeparate(columns.iterator());
    }

    /** Returns how to interpret a {@code ResultSet} as a row */
    public Read<List<Object>> build() {
        return Reads.list(reads);
    }

    public <T> Supplier<T> add(SQL column, Class<T> klass) {
        return add(column, Reads.useContext(klass));
    }

    private <T> CompletableSupplier<T> add(SQL column, Read<T> read) {
        columns.add(column);
        reads.add(read);

        final CompletableSupplier<T> supplier = new CompletableSupplier<>();
        suppliers.add(supplier);
        return supplier;
    }

    // Very boring repetitive code below this line to deal with each prim type

    public BooleanSupplier addBoolean(SQL column) {
        return addBoolean(column, Reads.useContext(boolean.class));
    }

    private BooleanSupplier addBoolean(SQL column, Read<Boolean> read) {
        return add(column, read)::get;
    }

    public IntSupplier addInt(SQL column) {
        return addInt(column, Reads.useContext(int.class));
    }

    private IntSupplier addInt(SQL column, Read<Integer> read) {
        return add(column, read)::get;
    }

    public LongSupplier addLong(SQL column) {
        return addLong(column, Reads.useContext(long.class));
    }

    private LongSupplier addLong(SQL column, Read<Long> read) {
        return add(column, read)::get;
    }

    public DoubleSupplier addDouble(SQL column) {
        return addDouble(column, Reads.useContext(double.class));
    }

    private DoubleSupplier addDouble(SQL column, Read<Double> read) {
        return add(column, read)::get;
    }
}
