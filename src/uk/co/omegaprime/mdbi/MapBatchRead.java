package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Supplier;

@ParametersAreNonnullByDefault
class MapBatchRead<K, V, MapT extends Map<K, V>> implements BatchRead<Map<K, V>> {
    private final Supplier<? extends MapT> factory;
    private final Read<K> readKey;
    private final Read<V> readValue;

    public MapBatchRead(Supplier<? extends MapT> factory, Read<K> readKey, Read<V> readValue) {
        this.factory = factory;
        this.readKey = readKey;
        this.readValue = readValue;
    }

    @Override
    public MapT get(Read.Context ctxt, Statementlike ps) throws SQLException {
        final BoundRead<? extends K> boundReadKey = readKey.bind(ctxt);
        final BoundRead<? extends V> boundReadValue = readValue.bind(ctxt);
        try (final ResultSet rs = ps.executeQuery()) {
            final MapT result = factory.get();
            while (rs.next()) {
                final IndexRef ix = new IndexRef();
                final K key = boundReadKey.get(rs, ix);
                final V value = boundReadValue.get(rs, ix);
                if (result.put(key, value) != null) {
                    throw new IllegalArgumentException("Key " + key + " occurs more than once in result");
                }
            }

            return result;
        }
    }
}
