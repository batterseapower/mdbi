package uk.co.omegaprime.mdbi;

/**
 * Given a context describing how to obtain arbitrary Java types from SQL ones, describes how a <i>particular</i>
 * Java type should be derived from a SQL type.
 * <p>
 * Useful instances of this can be obtained from {@link Reads}.
 */
public interface Read<T> {
    /** Runtime representation of the {@code Read} type parameter: useful for constructing arrays */
    Class<? extends T> getElementClass();

    BoundRead<? extends T> bind(Context ctxt);

    /** An immutable description of how to retrieve specific Java types from SQL. */
    interface Context {
        <T> Read<? extends T> get(Class<T> klass);
    }
}
