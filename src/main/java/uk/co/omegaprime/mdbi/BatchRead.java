package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A slightly simplified version of {@link StatementlikeBatchRead} that consumes a {@code ResultSet}.
 * <p>
 * Useful instances of this can be obtained from {@link BatchReads}.
 */
public interface BatchRead<T> {
    /**
     * Consume one or more row from the {@code ResultSet}, returning a corresponding Java object.
     * <p>
     * When it arrives, the {@code ResultSet} will be on the row before the first one that you can consume -- i.e.
     * it will not be on a valid row. Consequently you need to call {@link ResultSet#next()} before you access any column.
     * <p>
     * If you cease consuming the {@code ResultSet} early, you should leave the {@code ResultSet}
     * on the last row that you have decided to consume. This means that there is no difference between
     * leaving the {@code ResultSet} on the final row and leaving it after the end of the whole {@code ResultSet}.
     */
    T get(Read.Context ctxt, ResultSet rs) throws SQLException;
}
