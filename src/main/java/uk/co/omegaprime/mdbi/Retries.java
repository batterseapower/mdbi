package uk.co.omegaprime.mdbi;

import java.util.concurrent.TimeUnit;

/** Useful instances of the {@link Retry} interface.  */
public class Retries {
    private Retries() {}

    /** Always rethrow exceptions &mdash; i.e. don't retry anything */
    public static Retry nothing() {
        return new RetryNothing();
    }

    /** Retry exceptions that seem to be caused by deadlocks up to 5 times, backing off for 100ms extra each time */
    public static Retry deadlocks() {
        return new RetryDeadlocks();
    }

    /** Retry exceptions that seem to be caused by deadlocks using the supplied retry count and linear backoff period */
    public static Retry deadlocks(int maxRetries, int backoffInterval, TimeUnit backoffIntervalUnit) {
        return new RetryDeadlocks(maxRetries, backoffInterval, backoffIntervalUnit);
    }
}
