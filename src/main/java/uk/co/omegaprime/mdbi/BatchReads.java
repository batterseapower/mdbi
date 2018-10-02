package uk.co.omegaprime.mdbi;

import java.util.*;
import java.util.function.Function;
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
        return new MapBatchRead<>(LinkedHashMap::new, BatchReads::appendListHack, readKey, (Read<List<V>>)(Read)Reads.map(List.class, readValue, (V v) -> new ArrayList<V>(Collections.singletonList(v))));
    }

    /** Return the {@code ResultSet} as a {@code NavigableMap}, allowing multiple values for any given key */
    @SuppressWarnings("unchecked")
    public static <K, V> BatchRead<NavigableMap<K, List<V>>> asNavigableMultiMap(Class<K> keyClass, Class<V> valueClass) {
        return asNavigableMultiMap(new ContextRead<>(keyClass), new ContextRead<>(valueClass));
    }

    /** Return the {@code ResultSet} as a {@code NavigableMap}, allowing multiple values for any given key */
    @SuppressWarnings("unchecked")
    public static <K, V> BatchRead<NavigableMap<K, List<V>>> asNavigableMultiMap(Read<K> readKey, Read<V> readValue) {
        return new MapBatchRead<>(TreeMap::new, BatchReads::appendListHack, readKey, (Read<List<V>>)(Read)Reads.map(List.class, readValue, (V v) -> new ArrayList<V>(Collections.singletonList(v))));
    }

    // Bit dodgy because correctness depends crucially on how we are called
    private static <K, V> List<V> appendListHack(K key, List<V> od, List<V> nw) {
        if (nw.size() != 1) throw new IllegalStateException("This really shouldn't happen..");
        od.add(nw.get(0));
        return od;
    }

    /** As {@link #asMap(Read, BatchRead)} but simply reads the key using the {@code Context}-default read instance for the class */
    public static <K, V> BatchRead<Map<K, V>> asMap(Class<K> readKey, BatchRead<V> readValue) {
        return asMap(Reads.useContext(readKey), readValue);
    }

    /**
     * Splits the {@code ResultSet} into contiguous runs based on equality of the supplied key type. Reads the remaining
     * columns in each segment using the supplied {@code BatchRead}.
     * <p>
     * So for example, {@code asMap(key, value)} is equivalent to {@code segmented(key, first(value)}, and {@code asMultiMap(key, value)}
     * is equivalent to {@code segmented(key, asList(value)} in the case where the {@code ResultSet} is sorted by the key columns.
     * <p>
     * If a key occurs non-contiguously then {@code IllegalArgumentException} will be thrown.
     */
    public static <K, V> BatchRead<Map<K, V>> asMap(Read<K> readKey, BatchRead<V> readValue) {
        return new SegmentedMapBatchRead<>(LinkedHashMap::new, BatchReads::appendFail, readKey, readValue);
    }

    /** As {@link #asMapFirst(Read, BatchRead)} but simply reads the key using the {@code Context}-default read instance for the class */
    public static <K, V> BatchRead<Map<K, V>> asMapFirst(Class<K> readKey, BatchRead<V> readValue) {
        return asMapFirst(Reads.useContext(readKey), readValue);
    }

    /** As {@link #asMap(Read, BatchRead)} but returns the value associated with the first occurrence of a given key instead of failing. */
    public static <K, V> BatchRead<Map<K, V>> asMapFirst(Read<K> readKey, BatchRead<V> readValue) {
        return new SegmentedMapBatchRead<>(LinkedHashMap::new, (_key, od, _nw) -> od, readKey, readValue);
    }

    /** As {@link #asMapLast(Read, BatchRead)} but simply reads the key using the {@code Context}-default read instance for the class */
    public static <K, V> BatchRead<Map<K, V>> asMapLast(Class<K> readKey, BatchRead<V> readValue) {
        return asMapLast(Reads.useContext(readKey), readValue);
    }

    /** As {@link #asMap(Read, BatchRead)} but returns the value associated with the first occurrence of a given key instead of failing. */
    public static <K, V> BatchRead<Map<K, V>> asMapLast(Read<K> readKey, BatchRead<V> readValue) {
        return new SegmentedMapBatchRead<>(LinkedHashMap::new, (_key, _od, nw) -> nw, readKey, readValue);
    }

    /** As {@link #asMultiMap(Read, BatchRead)} but simply reads the key using the {@code Context}-default read instance for the class */
    public static <K, V> BatchRead<Map<K, List<V>>> asMultiMap(Class<K> readKey, BatchRead<V> readValue) {
        return asMultiMap(Reads.useContext(readKey), readValue);
    }

    /** As {@link #asMap(Read, BatchRead)} but returns all values associated with the a given key instead of failing. */
    @SuppressWarnings("unchecked")
    public static <K, V> BatchRead<Map<K, List<V>>> asMultiMap(Read<K> readKey, BatchRead<V> readValue) {
        return new SegmentedMapBatchRead<>(LinkedHashMap::new, BatchReads::appendListHack, readKey, (BatchRead<List<V>>)(BatchRead)BatchReads.map(readValue, (V v) -> new ArrayList<V>(Collections.singletonList(v))));
    }

    public static <U, V> BatchRead<V> map(BatchRead<U> read, Function<U, V> f) {
        return (ctxt, rs) -> f.apply(read.get(ctxt, rs));
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
     * <p>
     * If a {@code Read} instance spans more than one column, the name chosen will be that of the first column.
     */
    public static BatchRead<Map<String, Object>> labelledMatrix(Class<?>... klasses) {
        return labelledMatrix(Arrays.asList(klasses).stream().map(ContextRead::new).collect(Collectors.toList()));
    }

    /** As {@link #labelledMatrix(Class[])}, but for the case where you want to be explicit about how the columns are constructed. */
    public static BatchRead<Map<String, Object>> labelledMatrix(Collection<Read<?>> reads) {
        return new LabelledMatrixBatchRead(reads);
    }
}
