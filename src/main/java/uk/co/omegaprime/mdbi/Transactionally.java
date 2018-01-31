package uk.co.omegaprime.mdbi;

import com.sun.media.jfxmedia.locator.ConnectionHolder;

import java.sql.Connection;
import java.sql.SQLException;

/** Utilities for working with transactions */
public class Transactionally {
    private Transactionally() {}

    private static class TransactionHelper implements AutoCloseable {
        private final Connection conn;
        private boolean success;

        public TransactionHelper(Connection conn) throws SQLException {
            this.conn = conn;
            conn.setAutoCommit(false);
        }

        @Override
        public void close() throws SQLException {
            if (conn.getAutoCommit()) {
                // rollback() docs say it should be used only when auto-commit mode has been disabled
                // If the transaction has failed for some reason, auto-commit may have been disabled
                // behind our back, so check for that. Doing this also helps make close() idempotent.
                return;
            } else if (!success) {
                conn.rollback();
            }
            conn.setAutoCommit(true);
        }
    }

    /**
     * Runs the action (which presumably performs some SQL queries) in the context of a transaction.
     * <p>
     * If the action throws an exception, the transaction will be rolled back. If the action completes
     * without throwing, the transaction will be committed.
     * <p>
     * If a transaction is already in progress then the action will just be executed with no special
     * handling, essentially joining the transaction that is already in progress.
     */
    public static <T> T run(Connection c, SQLAction<T> action) throws SQLException {
        if (!c.getAutoCommit()) {
            // Already in transaction, join that one
            return action.run();
        } else {
            try (TransactionHelper th = new TransactionHelper(c)) {
                final T result = action.run();
                th.success = true;
                return result;
            }
        }
    }

    /**
     * Runs the action (which presumably performs some SQL queries) in the context of a transaction.
     * <p>
     * If the action throws an exception, the transaction will be retried assuming that the supplied
     * {@link Retry} instance does not rethrow the exception.
     *
     * @throws IllegalArgumentException if a transaction is already in progress on the supplied connection.
     */
    public static <T> T runWithRetry(Connection c, Retry retry, SQLAction<T> action) throws SQLException {
        if (!c.getAutoCommit()) {
            throw new IllegalArgumentException("The supplied connection must be in auto-commit mode (retrying an action " +
                                               "on a connection with an open connection probably won't do what you expect!)");
        }

        while (true) {
            try {
                return Transactionally.run(c, action);
            } catch (RuntimeException e) {
                retry.consider(e);
            } catch (SQLException e) {
                retry.consider(e);
            }
        }
    }
}
