package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

/** A slightly simplified version of {@link StatementlikeBatchRead} that consumes a {@code ResultSet}. */
public interface BatchRead<T> {
    T get(Read.Context ctxt, ResultSet rs) throws SQLException;
}
