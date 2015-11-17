package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface BoundRead<T> {
    T get(@Nonnull ResultSet rs, @Nonnull IndexRef ix) throws SQLException;
}
