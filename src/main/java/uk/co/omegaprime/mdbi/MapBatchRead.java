package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@ParametersAreNonnullByDefault
class MapBatchRead<K, V, MapT extends Map<K, V>> implements BatchRead<Map<K, V>> {
    private final Supplier<? extends MapT> factory;
    private final Appender<K, V> append;
    private final Read<K> readKey;
    private final Read<V> readValue;

    public interface Appender<K, V> {
        V append(K key, V oldValue, V newValue);
    }

    public MapBatchRead(Supplier<? extends MapT> factory, Appender<K, V> append, Read<K> readKey, Read<V> readValue) {
        this.factory = factory;
        this.append = append;
        this.readKey = readKey;
        this.readValue = readValue;
    }

    @Override
    public MapT get(Read.Context ctxt, ResultSet rs) throws SQLException {
        final BoundRead<? extends K> boundReadKey = readKey.bind(ctxt);
        final BoundRead<? extends V> boundReadValue = readValue.bind(ctxt);
        final MapT result = factory.get();
        while (rs.next()) {
            final IndexRef ix = IndexRef.create();
            final K key = boundReadKey.get(rs, ix);
            final V value = boundReadValue.get(rs, ix);

            final V consolidatedValue;
            if (!result.containsKey(key)) {
                consolidatedValue = value;
            } else {
                consolidatedValue = append.append(key, result.get(key), value);
            }

            result.put(key, consolidatedValue);
        }

        return result;
    }
}
