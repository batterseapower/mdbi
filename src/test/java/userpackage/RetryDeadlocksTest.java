package userpackage;

import org.junit.Test;
import uk.co.omegaprime.mdbi.Retries;
import uk.co.omegaprime.mdbi.Retry;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class RetryDeadlocksTest {
    private final SQLException DEADLOCK_EXCEPTION = new SQLException("deadlocked with another process");

    @Test
    public void retryTwiceIfRetryCountIsTwo() throws SQLException {
        final Retry r = Retries.deadlocks(2, 0, TimeUnit.MILLISECONDS);
        r.consider(DEADLOCK_EXCEPTION);
        // First retry happens here..
        r.consider(DEADLOCK_EXCEPTION);
        // Second retry happens here..
    }

    @Test(expected = SQLException.class)
    public void dontRetryThrifeIfRetryCountIsTwo() throws SQLException {
        final Retry r = Retries.deadlocks(2, 0, TimeUnit.MILLISECONDS);
        r.consider(DEADLOCK_EXCEPTION);
        // First retry happens here..
        r.consider(DEADLOCK_EXCEPTION);
        // Second retry happens here..
        r.consider(DEADLOCK_EXCEPTION);
    }

    @Test(expected = SQLException.class)
    public void rethrowNonDeadlockyExceptions() throws SQLException {
        final Retry r = Retries.deadlocks(2, 0, TimeUnit.MILLISECONDS);
        r.consider(new SQLException("you got a column name wrong"));
    }
}
