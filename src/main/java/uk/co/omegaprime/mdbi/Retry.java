package uk.co.omegaprime.mdbi;

/**
 * A function that decides how to handle an exception that is encountered in the course of executing a query.
 * <p>
 * When MDBI performs a query, it constructs a {@code Retry} instance. If an error is encountered during
 * execution of the query, the exception is supplied to the instance. The {@code Retry} may choose to rethrow
 * the exception, in which case execution of the query terminates, or it may choose to discard the exception,
 * in which case the query will be retried.
 * <p>
 * By keeping mutable state in the {@code Retry} instance it's possible to implement elaborate retry strategies
 * that e.g. retry a fixed number of times before giving up, or have arbitrarily interesting backoff schemes.
 */
public interface Retry {
    /** Rethrows the supplied exception if necessary */
    <T extends Throwable> void consider(T e) throws T;
}
