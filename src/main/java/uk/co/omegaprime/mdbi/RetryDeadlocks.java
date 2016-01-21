package uk.co.omegaprime.mdbi;

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

class RetryDeadlocks implements Retry {
    private final int maxRetries;
    private final int backoffMillis;

    private Random jitterSource;
    private int retry = 0;

    public RetryDeadlocks() {
        this(5, 1000, TimeUnit.MILLISECONDS);
    }

    public RetryDeadlocks(int maxRetries, int backoffInterval, TimeUnit backoffIntervalUnit) {
        this.maxRetries = maxRetries;
        this.backoffMillis = (int)backoffIntervalUnit.toMillis(backoffInterval);
    }

    private long pow(int a, int b) {
        if (b < 0) {
            throw new IllegalArgumentException();
        }

        long result = 1;
        while (b != 0) {
            if (b == 1) {
                result *= a;
                break;
            } else if (b % 2 == 1) {
                result *= a;
            }

            a = a * a;
            b = b / 2;
        }

        return result;
    }

    @Override
    public <T extends Throwable> void consider(T e) throws T {
        if (e instanceof SQLException && e.getMessage() != null && e.getMessage().toLowerCase().contains("deadlock") && retry++ < maxRetries) {
            try {
                // Lazy initialization here is just a small perf hack
                if (jitterSource == null) {
                    jitterSource = new Random();
                }

                final int maxJitterMillis = backoffMillis / 4;
                final int jitterMillis = maxJitterMillis == 0 ? 0 : jitterSource.nextInt(maxJitterMillis);

                // 1 <= retry <= maxRetries
                Thread.sleep(jitterMillis + (backoffMillis * pow(2, retry - 1)));
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
        } else {
            throw e;
        }
    }
}
