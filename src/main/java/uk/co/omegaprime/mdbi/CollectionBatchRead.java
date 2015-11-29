package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.function.Supplier;

class CollectionBatchRead<T, CollectionT extends Collection<T>> implements BatchRead<CollectionT> {
    private final Supplier<? extends CollectionT> factory;
    private final Read<T> read;

    public CollectionBatchRead(Supplier<? extends CollectionT> factory, Read<T> read) {
        this.factory = factory;
        this.read = read;
    }

    @Override
    public CollectionT get(@Nonnull Read.Context ctxt, @Nonnull ResultSet rs) throws SQLException {
        final BoundRead<? extends T> boundRead = read.bind(ctxt);

        final CollectionT result = factory.get();
        while (rs.next()) {
            result.add(boundRead.get(rs, IndexRef.create()));
        }
        return result;
    }
}
