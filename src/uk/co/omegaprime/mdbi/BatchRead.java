package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.SQLException;

@ParametersAreNonnullByDefault
public interface BatchRead<T> {
    T get(Reads.Map ctxt, Statementlike ps) throws SQLException;
}
