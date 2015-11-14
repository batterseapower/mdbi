package uk.co.omegaprime.mdbi;

import java.sql.SQLException;

public interface SQLAction<T> {
    T run() throws SQLException;
}
