package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

@ParametersAreNonnullByDefault
class SegmentedMapBatchRead<K, V, MapT extends Map<K, V>> implements BatchRead<MapT> {
    private final Supplier<MapT> factory;
    private final Read<K> readKey;
    private final BatchRead<V> readValue;
    private final MapEntryAppender<K, V> append;

    public SegmentedMapBatchRead(Supplier<MapT> factory, MapEntryAppender<K, V> append, Read<K> readKey, BatchRead<V> readValue) {
        this.factory = factory;
        this.append = append;
        this.readKey = readKey;
        this.readValue = readValue;
    }

    @Override
    public MapT get(Read.Context ctxt, ResultSet rs) throws SQLException {
        final MapT result = factory.get();
        final BoundRead<? extends K> boundReadKey = readKey.bind(ctxt);
        if (rs.next()) {
            while (true) {
                final IndexRef ix = IndexRef.create();
                final K key = boundReadKey.get(rs, ix);

                final PeekedResultSet prs = new PeekedResultSet(rs);
                final ContiguouslyFilteredResultSet srs = new ContiguouslyFilteredResultSet(prs, () -> Objects.equals(key, boundReadKey.get(prs, IndexRef.create())));
                final V newValue = readValue.get(ctxt, new DropColumnsResultSet(ix.peek(), srs));

                final V consolidatedValue;
                if (result.containsKey(key)) {
                    consolidatedValue = append.append(key, result.get(key), newValue);
                } else {
                    consolidatedValue = newValue;
                }

                result.put(key, consolidatedValue);

                if (prs.isUsedPeekedRow()) {
                    // 1. If readValue never called next(), it left "srs" on the row before where "rs" is right now, and so
                    //    we still need to treat the current row of "rs" as unconsumed, so don't next() over it.
                    //
                    // 2. Equally, if readValue() called next() but did so leaving the ResultSet after the last row in the filtered
                    //    set, we should *not* treat that as meaning that the current row of "rs" is unconsumed, since the user
                    //    of "srs" never actually got a chance to see it!
                    //
                    // Note that if readValue called next() and then previous() then "srs" will be on row 0, exactly as in case 1.
                    // However, *unlike* in that case "rs" will have been moved back to the row prior to what it was on before the
                    // readValue call, so doing next() here just takes us back to where we were before the readValue call, which is fine.
                    if (srs.isAfterLast()) {
                        if (rs.isAfterLast()) break;
                    } else {
                        if (!rs.next()) break;
                    }
                }
            }
        }

        return result;
    }
}
