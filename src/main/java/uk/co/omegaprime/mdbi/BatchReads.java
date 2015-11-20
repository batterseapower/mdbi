package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;

/** Functions for creating useful instances of {@link BatchRead}. */
@ParametersAreNonnullByDefault
public final class BatchReads {
    private BatchReads() {}

    /** Returns the first row of the {@code ResultSet}, and throws {@code NoSuchElementException} if no such row exists. */
    public static <T> BatchRead<T> first(Read<T> read) {
        return fromResultSetBatchRead((ctxt, rs) -> {
            if (rs.next()) {
                return read.bind(ctxt).get(rs, new IndexRef());
            } else {
                throw new NoSuchElementException();
            }
        });
    }

    /** Returns the first row of the {@code ResultSet}, or null if no such row exists. */
    public static <T> BatchRead<T> firstOrNull(Read<T> read) {
        return fromResultSetBatchRead((ctxt, rs) -> rs.next() ? read.bind(ctxt).get(rs, new IndexRef()) : null);
    }

    public static <T> BatchRead<List<T>> asList(Read<T> read) {
        return new CollectionBatchRead<>(ArrayList::new, read);
    }

    public static <T> BatchRead<Set<T>> asSet(Read<T> read) {
        return new CollectionBatchRead<>(LinkedHashSet::new, read);
    }

    public static <K, V> BatchRead<Map<K, V>> asMap(Read<K> readKey, Read<V> readValue) {
        return new MapBatchRead<>(LinkedHashMap::new, readKey, readValue);
    }

    /**
     * Returns the {@code ResultSet} interpreted as an array of column vectors.
     * <p>
     * The classes specify the element types of the column vectors. So if you call {@code matrix(String.class, int.class)}
     * then your {@code ResultSet} will be turned into an {@code Object[]} with two elements: a {@code String[]} and a {@code int[]}.
     */
    public static BatchRead<Object[]> matrix(Class<?>... klasses) {
        return new MatrixBatchRead(klasses);
    }

    /** As {@link #matrix(Class[])}, but for the case where you want to be explicit about how the columns are constructed. */
    public static BatchRead<Object[]> matrix(Collection<Read<?>> reads) {
        return new MatrixBatchRead(reads);
    }

    /** Creates a {@code BatchRead} that just processes a {@code ResultSet} via the {@link ResultSetBatchRead} interface. */
    public static <T> BatchRead<T> fromResultSetBatchRead(ResultSetBatchRead<T> rsbr) {
        return (ctxt, ps) -> rsbr.get(ctxt, ps.executeQuery());
    }
}
