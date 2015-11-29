package uk.co.omegaprime.mdbi;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** Functions for creating useful instances of {@link BatchRead}. */
public class BatchReads {
    private BatchReads() {}

    /** Returns the first row of the {@code ResultSet}, and throws {@code NoSuchElementException} if no such row exists. */
    public static <T> BatchRead<T> first(Class<T> klass) {
        return first(new ContextRead<>(klass));
    }

    /** Returns the first row of the {@code ResultSet}, and throws {@code NoSuchElementException} if no such row exists. */
    public static <T> BatchRead<T> first(Read<T> read) {
        return (ctxt, rs) -> {
            if (rs.next()) {
                return read.bind(ctxt).get(rs, IndexRef.create());
            } else {
                throw new NoSuchElementException();
            }
        };
    }

    /** Returns the first row of the {@code ResultSet}, or null if no such row exists. */
    public static <T> BatchRead<T> firstOrNull(Class<T> klass) {
        return firstOrNull(new ContextRead<>(klass));
    }

    /** Returns the first row of the {@code ResultSet}, or null if no such row exists. */
    public static <T> BatchRead<T> firstOrNull(Read<T> read) {
        return (ctxt, rs) -> rs.next() ? read.bind(ctxt).get(rs, IndexRef.create()) : null;
    }

    public static <T> BatchRead<List<T>> asList(Class<T> klass) {
        return asList(new ContextRead<>(klass));
    }

    public static <T> BatchRead<List<T>> asList(Read<T> read) {
        return new CollectionBatchRead<>(ArrayList::new, read);
    }

    public static <T> BatchRead<Set<T>> asSet(Class<T> klass) {
        return asSet(new ContextRead<>(klass));
    }

    public static <T> BatchRead<Set<T>> asSet(Read<T> read) {
        return new CollectionBatchRead<>(LinkedHashSet::new, read);
    }

    /** Return the {@code ResultSet} as a map, failing if any key occurs more than once */
    public static <K, V> BatchRead<Map<K, V>> asMap(Class<K> keyClass, Class<V> valueClass) {
        return asMap(new ContextRead<>(keyClass), new ContextRead<>(valueClass));
    }

    /** Return the {@code ResultSet} as a map, failing if any key occurs more than once */
    public static <K, V> BatchRead<Map<K, V>> asMap(Read<K> readKey, Read<V> readValue) {
        return new MapBatchRead<>(LinkedHashMap::new, BatchReads::appendFail, readKey, readValue);
    }

    private static <K, V> V appendFail(K key, V oldValue, V newValue) {
        throw new IllegalArgumentException("Key " + key + " occurs more than once in result, associated with both " + oldValue + " and " + newValue);
    }

    /** Return the {@code ResultSet} as a map, using the first value encountered for any given key */
    public static <K, V> BatchRead<Map<K, V>> asMapFirst(Class<K> keyClass, Class<V> valueClass) {
        return asMapFirst(new ContextRead<>(keyClass), new ContextRead<>(valueClass));
    }

    /** Return the {@code ResultSet} as a map, using the first value encountered for any given key */
    public static <K, V> BatchRead<Map<K, V>> asMapFirst(Read<K> readKey, Read<V> readValue) {
        return new MapBatchRead<>(LinkedHashMap::new, (_key, od, _nw) -> od, readKey, readValue);
    }

    /** Return the {@code ResultSet} as a map, using the last value encountered for any given key */
    public static <K, V> BatchRead<Map<K, V>> asMapLast(Class<K> keyClass, Class<V> valueClass) {
        return asMapLast(new ContextRead<>(keyClass), new ContextRead<>(valueClass));
    }

    /** Return the {@code ResultSet} as a map, using the last value encountered for any given key */
    public static <K, V> BatchRead<Map<K, V>> asMapLast(Read<K> readKey, Read<V> readValue) {
        return new MapBatchRead<>(LinkedHashMap::new, (_key, _od, nw) -> nw, readKey, readValue);
    }

    /** Return the {@code ResultSet} as a map, allowing multiple values for any given key */
    public static <K, V> BatchRead<Map<K, List<V>>> asMultiMap(Class<K> keyClass, Class<V> valueClass) {
        return asMultiMap(new ContextRead<>(keyClass), new ContextRead<>(valueClass));
    }

    /** Return the {@code ResultSet} as a map, allowing multiple values for any given key */
    @SuppressWarnings("unchecked")
    public static <K, V> BatchRead<Map<K, List<V>>> asMultiMap(Read<K> readKey, Read<V> readValue) {
        return new MapBatchRead<>(LinkedHashMap::new, (_key, od, nw) -> {
            if (nw.size() != 1) throw new IllegalStateException("This really shouldn't happen..");
            od.add(nw.get(0));
            return od;
        }, readKey, (Read<List<V>>)(Read)Reads.map(List.class, readValue, (V v) -> new ArrayList<V>(Collections.singletonList(v))));
    }

    /**
     * Returns the {@code ResultSet} interpreted as an array of column vectors.
     * <p>
     * The classes specify the element types of the column vectors. So if you call {@code matrix(String.class, int.class)}
     * then your {@code ResultSet} will be turned into an {@code Object[]} with two elements: a {@code String[]} and a {@code int[]}.
     */
    public static BatchRead<Object[]> matrix(Class<?>... klasses) {
        return matrix(Arrays.asList(klasses).stream().map(ContextRead::new).collect(Collectors.toList()));
    }

    /** As {@link #matrix(Class[])}, but for the case where you want to be explicit about how the columns are constructed. */
    public static BatchRead<Object[]> matrix(Collection<Read<?>> reads) {
        return new MatrixBatchRead(reads);
    }

    /**
     * Returns the {@code ResultSet} interpreted as a map of column names to column vectors.
     * <p>
     * The classes specify the element types of the column vectors. So if you call {@code matrix(String.class, int.class)}
     * then your {@code ResultSet} will be turned into a {@code Map} with two elements: a {@code String[]} and a {@code int[]}.
     */
    public static BatchRead<Map<String, Object>> labelledMatrix(Class<?>... klasses) {
        return labelledMatrix(Arrays.asList(klasses).stream().map(ContextRead::new).collect(Collectors.toList()));
    }

    /** As {@link #labelledMatrix(Class[])}, but for the case where you want to be explicit about how the columns are constructed. */
    public static BatchRead<Map<String, Object>> labelledMatrix(Collection<Read<?>> reads) {
        return new LabelledMatrixBatchRead(reads);
    }
}
