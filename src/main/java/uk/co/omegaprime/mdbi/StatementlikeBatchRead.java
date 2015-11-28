package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.SQLException;

/**
 * A function that turns the entire result of executing some SQL into a single Java object.
 * <p>
 * Useful instances of this interface can be created using the methods in {@link BatchReads}.
 */
@ParametersAreNonnullByDefault
public interface StatementlikeBatchRead<T> {
    T get(Read.Context ctxt, Statementlike ps) throws SQLException;
}
