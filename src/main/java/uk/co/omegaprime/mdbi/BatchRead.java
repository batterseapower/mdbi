package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

/** A slightly simplified version of {@link StatementlikeBatchRead} that consumes a {@code ResultSet}. */
public interface BatchRead<T> {
    /**
     * Consume one or more row from the {@code ResultSet}, returning a corresponding Java object.
     *
     * If you cease consuming the {@code ResultSet} early, you should leave the {@code ResultSet}
     * on the first row that you have decided not to consume. An exception to this is if you only want
     * to consume a single row from the input, in which case you are permitted to leave the {@code ResultSet}
     * on the row in question. This is a performance optimization intended to avoid the overhead of {@link ResultSet#next()} in
     * the common case where you only want the first row from a {@code ResultSet}.
     *
     * As a result of this optimization, there is no way to signal that you have consumed zero rows
     * from the {@code ResultSet}.
     */
    T get(Read.Context ctxt, ResultSet rs) throws SQLException;
}
