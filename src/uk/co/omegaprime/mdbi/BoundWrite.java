package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public interface BoundWrite<T> {
    int arity();

    void set(@Nonnull PreparedStatement s, @Nonnull IndexRef ix, @Nullable T x) throws SQLException;

    @Nonnull List<String> asSQL(@Nullable T x);
}
