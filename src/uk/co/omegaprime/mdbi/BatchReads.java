package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;

@ParametersAreNonnullByDefault
public final class BatchReads {
    private BatchReads() {}

    public static <T> BatchRead<T> first(Read<T> read) {
        return fromResultSetBatchRead((ctxt, rs) -> {
            if (rs.next()) {
                return read.bind(ctxt).get(rs, new IndexRef());
            } else {
                throw new NoSuchElementException();
            }
        });
    }

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

    public static BatchRead<Object[]> matrix(Class<?>... klasses) {
        return new MatrixBatchRead(klasses);
    }

    public static BatchRead<Object[]> matrix(Collection<Read<?>> reads) {
        return new MatrixBatchRead(reads);
    }

    public static <T> BatchRead<T> fromResultSetBatchRead(ResultSetBatchRead<T> rsbr) {
        return (ctxt, ps) -> rsbr.get(ctxt, ps.executeQuery());
    }
}
