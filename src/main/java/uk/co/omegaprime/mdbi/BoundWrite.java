package uk.co.omegaprime.mdbi;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * A function that turns a Java object into data in a consecutive subset of parameters of a SQL query.
 * <p>
 * You should only have to worry about this interface if you are creating your own implementations
 * of the {@link Read} interface. Users who are just using the built-in type mappers can ignore it.
 */
public interface BoundWrite<T> {
    /** Returns the number of parameters that we will require to represent the Java object. */
    int arity();

    /**
     * Inserts data into zero or more columns of the {@code PreparedStatement}, beginning with that
     * indicated by {@code ix}. It is expected that this data will be based on the value of {@code x}.
     * <p>
     * Please do not e.g. execute the {@code PreparedStatement} from within your implementation of this method!
     * You should only be setting data into the statement. You are also expected to advance the {@code IndexRef}
     * exactly {@link #arity()} times.
     */
    void set(@Nonnull PreparedStatement s, @Nonnull IndexRef ix, @Nullable T x) throws SQLException;

    /**
     * Returns a sequence of SQL expressions presenting the data that would have been set on the {@code PreparedStatement}
     * by the equivalent {@link #set(PreparedStatement, IndexRef, Object)} call.
     * <p>
     * The returned list should be exactly {@link #arity()} elements in length.
     * <p>
     * This method is used when the user of MDBI has elected not to use {@code PreparedStatement}s.
     */
    @Nonnull List<String> asSQL(@Nullable T x);
}
