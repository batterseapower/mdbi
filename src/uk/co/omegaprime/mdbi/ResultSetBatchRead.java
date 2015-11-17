package uk.co.omegaprime.mdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultSetBatchRead<T> {
    T get(Read.Context ctxt, ResultSet rs) throws SQLException;
}
