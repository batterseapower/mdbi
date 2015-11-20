package uk.co.omegaprime.mdbi;

import java.sql.SQLException;

/** Basically a {@link java.util.concurrent.Callable} that throws a more discriminating exception type */
public interface SQLAction<T> {
    T run() throws SQLException;
}
