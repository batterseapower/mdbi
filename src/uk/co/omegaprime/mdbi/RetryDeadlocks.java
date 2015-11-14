package uk.co.omegaprime.mdbi;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

class RetryDeadlocks implements Retry {
    private final int maxRetries;
    private final int backoffMillis;

    private int retry = 0;

    public RetryDeadlocks() {
        this(5, 100, TimeUnit.MILLISECONDS);
    }

    public RetryDeadlocks(int maxRetries, int backoffInterval, TimeUnit backoffIntervalUnit) {
        this.maxRetries = maxRetries;
        this.backoffMillis = (int)backoffIntervalUnit.toMillis(backoffInterval);
    }

    @Override
    public <T extends Throwable> void consider(T e) throws T {
        if (e instanceof SQLException && e.getMessage() != null && e.getMessage().toLowerCase().contains("deadlock") && retry++ < maxRetries) {
            try {
                Thread.sleep(backoffMillis * retry);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
        } else {
            throw e;
        }
    }
}
