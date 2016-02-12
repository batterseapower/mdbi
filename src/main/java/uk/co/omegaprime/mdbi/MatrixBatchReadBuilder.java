package uk.co.omegaprime.mdbi;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A convenience wrapper around the {@link BatchReads#matrix(Class[])} functionality that avoids you having to track column indexes.
 * <p>
 * <pre>
 * MatrixBatchReadBuilder mrb = MatrixBatchReadBuilder.create();
 * Supplier&lt;String[]&gt; names = mrb.add(sql("name"), String.class);
 * Supplier&lt;int[]&gt; ages = mrb.addInt(sql("age"));
 *
 * int n = mrb.buildAndExecute(mdbi, columns -> sql("select ", columns, " from people"));
 *
 * for (int i = 0; i < n; i++) {
 *     System.out.println("Hello " + names[i] + " of age " + ages[i]);
 * }
 * </pre>
 */
public class MatrixBatchReadBuilder {
    private final List<SQL> columns = new ArrayList<>();
    private final List<Read<?>> reads = new ArrayList<>();
    private final List<CompletableSupplier<?>> suppliers = new ArrayList<>();

    private static class CompletableSupplier<T> implements Supplier<T> {
        public T value;

        @Override
        public T get() {
            if (value == null) {
                throw new IllegalStateException("You must complete the corresponding MatrixBatchReadBuilder before invoking a Supplier that it returns");
            }

            return value;
        }
    }

    private MatrixBatchReadBuilder() {}

    public static MatrixBatchReadBuilder create() { return new MatrixBatchReadBuilder(); }

    /** A convenience that builds the SQL query and executes it with an appropriate reader all in one go. Returns the row count. */
    public int buildAndExecute(MDBI mdbi, Function<SQL, SQL> mkSelect) throws SQLException {
        return complete(mdbi.query(mkSelect.apply(buildColumns()), build()));
    }

    /** Use the supplied matrix to complete all the {@code Supplier} objects that we have returned. Returns the row count. */
    @SuppressWarnings("unchecked")
    public int complete(Object[] matrix) {
        if (suppliers.get(0).value != null) {
            throw new IllegalStateException("You may only complete each MatrixBatchReadBuilder once");
        }

        for (int i = 0; i < matrix.length; i++) {
            ((CompletableSupplier<Object>)suppliers.get(i)).value = matrix[i];
        }

        return Array.getLength(matrix[0]);
    }

    /** Returns comma delimited column list */
    public SQL buildColumns() {
        if (columns.isEmpty()) throw new IllegalStateException("You must add at least one column");

        SQL result = columns.get(0);
        for (int i = 1; i < columns.size(); i++) {
            result = result.sql(", ").sql(columns.get(i));
        }

        return result;
    }

    /** Returns how to interpret a {@code ResultSet} as a matrix */
    public BatchRead<Object[]> build() {
        return BatchReads.matrix(reads);
    }

    private <Ts> Supplier<Ts> addInternal(SQL column, Read<?> read) {
        columns.add(column);
        reads.add(read);

        final CompletableSupplier<Ts> supplier = new CompletableSupplier<>();
        suppliers.add(supplier);
        return supplier;
    }

    public <T> Supplier<T[]> add(SQL column, Class<T> klass) {
        return add(column, new ContextRead<>(klass));
    }

    public <T> Supplier<T[]> add(SQL column, Read<T> read) {
        return addInternal(column, read);
    }

    // Very boring repetitive code below this line to deal with each prim type

    public Supplier<boolean[]> addBoolean(SQL column) {
        return addBoolean(column, Reads.useContext(boolean.class));
    }

    public Supplier<boolean[]> addBoolean(SQL column, Read<Boolean> read) {
        return addInternal(column, read);
    }

    public Supplier<byte[]> addByte(SQL column) {
        return addByte(column, Reads.useContext(byte.class));
    }

    public Supplier<byte[]> addByte(SQL column, Read<Byte> read) {
        return addInternal(column, read);
    }

    public Supplier<char[]> addChar(SQL column) {
        return addChar(column, Reads.useContext(char.class));
    }

    public Supplier<char[]> addChar(SQL column, Read<Character> read) {
        return addInternal(column, read);
    }

    public Supplier<short[]> addShort(SQL column) {
        return addShort(column, Reads.useContext(short.class));
    }

    public Supplier<short[]> addShort(SQL column, Read<Short> read) {
        return addInternal(column, read);
    }

    public Supplier<int[]> addInt(SQL column) {
        return addInt(column, Reads.useContext(int.class));
    }

    public Supplier<int[]> addInt(SQL column, Read<Integer> read) {
        return addInternal(column, read);
    }

    public Supplier<long[]> addLong(SQL column) {
        return addLong(column, Reads.useContext(long.class));
    }

    public Supplier<long[]> addLong(SQL column, Read<Long> read) {
        return addInternal(column, read);
    }

    public Supplier<float[]> addFloat(SQL column) {
        return addFloat(column, Reads.useContext(float.class));
    }

    public Supplier<float[]> addFloat(SQL column, Read<Float> read) {
        return addInternal(column, read);
    }

    public Supplier<double[]> addDouble(SQL column) {
        return addDouble(column, Reads.useContext(double.class));
    }

    public Supplier<double[]> addDouble(SQL column, Read<Double> read) {
        return addInternal(column, read);
    }
}
