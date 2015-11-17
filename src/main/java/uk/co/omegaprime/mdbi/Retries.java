package uk.co.omegaprime.mdbi;

import java.util.concurrent.TimeUnit;

public class Retries {
    private Retries() {}

    public static Retry nothing() {
        return new RetryNothing();
    }

    public static Retry deadlocks() {
        return new RetryDeadlocks();
    }

    public static Retry deadlocks(int maxRetries, int backoffInterval, TimeUnit backoffIntervalUnit) {
        return new RetryDeadlocks(maxRetries, backoffInterval, backoffIntervalUnit);
    }
}
