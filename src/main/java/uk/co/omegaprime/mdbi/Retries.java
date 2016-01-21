package uk.co.omegaprime.mdbi;

import java.util.concurrent.TimeUnit;

/** Useful instances of the {@link Retry} interface.  */
public class Retries {
    private Retries() {}

    /** Always rethrow exceptions &mdash; i.e. don't retry anything */
    public static Retry nothing() {
        return new RetryNothing();
    }

    /** Retry exceptions that seem to be caused by deadlocks using a reasonable default backoff strategy */
    public static Retry deadlocks() {
        return new RetryDeadlocks();
    }

    /** Retry exceptions that seem to be caused by deadlocks using up to the given number of times, using the supplied initial backoff period */
    public static Retry deadlocks(int maxRetries, int backoffInterval, TimeUnit backoffIntervalUnit) {
        return new RetryDeadlocks(maxRetries, backoffInterval, backoffIntervalUnit);
    }
}
