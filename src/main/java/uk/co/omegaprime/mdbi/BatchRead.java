package uk.co.omegaprime.mdbi;

import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.SQLException;

@ParametersAreNonnullByDefault
public interface BatchRead<T> {
    T get(Read.Context ctxt, Statementlike ps) throws SQLException;
}
