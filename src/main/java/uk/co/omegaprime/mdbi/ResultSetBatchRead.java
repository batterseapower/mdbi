package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

/** A slightly simplified version of {@link BatchRead} that consumes a {@code ResultSet}. */
public interface ResultSetBatchRead<T> {
    T get(Read.Context ctxt, ResultSet rs) throws SQLException;
}
