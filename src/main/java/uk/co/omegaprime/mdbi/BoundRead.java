package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A function that turns a consecutive subset of columns in one row of a {@code ResultSet} into a Java object.
 *
 * You should only have to worry about this interface if you are creating your own implementations
 * of the {@link Read} interface. Users who are just using the built-in type mappers can ignore it.
 */
public interface BoundRead<T> {
    /**
     * Consumes data in zero or more columns of the current {@code ResultSet} row, beginning with
     * that indicated by {@code ix}.
     *
     * Please do not advance the {@code ResultSet} from within your implementation of this method!
     * However, feel free to advance the {@code IndexRef} as many times as you need.
     */
    T get(@Nonnull ResultSet rs, @Nonnull IndexRef ix) throws SQLException;
}
