package uk.co.omegaprime.mdbi;

/**
 * Given a context describing how to obtain arbitrary SQL types from Java ones, describes how a <i>particular</i>
 * SQL type should be derived from a Java type.
 * <p>
 * Useful instances of this can be obtained from {@link Writes}.
 */
public interface Write<T> {
    BoundWrite<? super T> bind(Context ctxt);

    /** An immutable description of how to obtain specific SQL types from Java. */
    interface Context {
        <T> Write<? super T> get(Class<T> klass);
    }
}
